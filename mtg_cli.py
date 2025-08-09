#!/usr/bin/env python3
"""
Unified CLI interface for MTG data processing.

This script provides a single entry point for all MTG processing operations
with subcommands for different tasks and comprehensive progress reporting.

Requires Python 3.10+
"""

import argparse
import logging
import sys
from pathlib import Path

import tqdm

from mtg_utils import (
    BatchProcessor,
    ConnectionPool,
    DownloadError,
    batch_insert_cards,
    create_database,
    create_price_table,
    download_collection,
    download_prices,
    download_sets,
    ensure_column_exists,
    get_available_collections,
    get_existing_card_uuids,
    get_project_paths,
    optimize_sqlite_connection,
    print_processing_summary,
    process_set_cards,
    read_json_file,
    setup_environment,
    unzip_files,
    unzip_single_file,
    validate_set_codes,
    verify_database,
    verify_price_data,
)
from mtg_utils.constants import CSV_HEADERS, DEFAULT_EXPORT_LIMIT
from mtg_utils.database import create_temp_table_from_list
from mtg_utils.exceptions import MTGProcessingError
from mtg_utils.io_operations import read_card_list
from mtg_utils.reporting import (
    export_csv_preview,
    export_to_csv,
    print_collection_summary,
)
from mtg_utils.sql import (
    GET_CARDS_FROM_LIST,
    GET_TOP_CARDS_WITH_PRICES,
    INSERT_PRICE_QUERY,
)

logger = logging.getLogger(__name__)


class TqdmLoggingHandler(logging.Handler):
    """Custom logging handler that works with tqdm progress bars."""

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.tqdm.write(msg)
        except Exception:
            self.handleError(record)


def setup_tqdm_logging():
    """Set up logging to work properly with tqdm progress bars."""
    # Remove default handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # Add tqdm-compatible handler
    handler = TqdmLoggingHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    logging.root.addHandler(handler)


def process_cards_command(args: argparse.Namespace) -> int:
    """Process MTG card data from individual set files."""
    try:
        paths = get_project_paths("sets")

        if not paths["gzipped"].exists():
            logger.error(f"Source directory not found: {paths['gzipped']}")
            return 1

        # Unzip files with progress bar
        with tqdm.tqdm(desc="Unzipping files", unit="file") as pbar:

            def update_progress(current, total):
                pbar.total = total
                pbar.n = current
                pbar.refresh()

            json_files = unzip_files(paths["gzipped"], paths["json"])
            pbar.total = len(json_files)
            pbar.n = len(json_files)

        if not json_files:
            logger.warning("No JSON files to process!")
            return 0

        # Create database
        conn = create_database(paths["db"], fresh_start=args.fresh)
        optimize_sqlite_connection(conn)

        try:
            # Set up connection pool for better performance
            connection_pool = ConnectionPool(paths["db"], max_connections=4)
            batch_processor = BatchProcessor(connection_pool, batch_size=2000)

            total_new = 0
            total_updated = 0
            total_skipped = 0

            # Process files with progress bar
            with tqdm.tqdm(
                desc="Processing sets", unit="set", total=len(json_files)
            ) as pbar:
                for json_file in json_files:
                    pbar.set_description(f"Processing {json_file.name}")

                    # Read and parse JSON file
                    data = read_json_file(json_file)
                    set_data = data.get("data", {})

                    # Process cards from this set
                    cards_data = process_set_cards(set_data)

                    if cards_data:
                        # Batch insert with connection pool
                        def process_batch(conn, batch):
                            return batch_insert_cards(conn, batch)

                        stats = batch_processor.process_batches(
                            cards_data,
                            process_batch,
                            progress_callback=lambda c, t: None,  # Handled by outer progress bar
                        )

                        total_new += stats.processed_items
                        total_skipped += stats.failed_items

                    pbar.update(1)

            print_processing_summary(total_new, total_updated, total_skipped)
            verify_database(conn)

        finally:
            conn.close()
            if "connection_pool" in locals():
                connection_pool.close_all()

        logger.info("✓ Card processing complete!")
        return 0

    except MTGProcessingError as e:
        logger.error(f"Processing failed: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1


def process_prices_command(args: argparse.Namespace) -> int:
    """Process MTG price data from AllPrices.json.gz."""
    try:
        paths = get_project_paths("prices")
        db_path = get_project_paths("sets")["db"]

        source_file = paths["gzipped"] / "AllPrices.json.gz"
        if not source_file.exists():
            logger.error(f"Price file not found: {source_file}")
            return 1

        if not db_path.exists():
            logger.error(f"Database not found: {db_path}")
            logger.error("Please run 'mtg process-cards' first to create the database.")
            return 1

        # Unzip price file
        with tqdm.tqdm(desc="Unzipping prices", unit="file") as pbar:
            json_path = unzip_single_file(source_file, paths["json"])
            pbar.update(1)

        # Connect to database
        conn = create_database(db_path)
        optimize_sqlite_connection(conn)

        try:
            create_price_table(conn)

            # Load price data
            with tqdm.tqdm(desc="Loading price data") as pbar:
                price_data = read_json_file(json_path)
                pbar.update(1)

            # Get existing card UUIDs
            existing_uuids = get_existing_card_uuids(conn)

            # Process price data with progress
            all_price_entries = price_data.get("data", {})

            with tqdm.tqdm(
                desc="Processing prices", unit="card", total=len(all_price_entries)
            ) as pbar:
                price_batch = []
                batch_size = 1000

                for uuid, card_price_data in all_price_entries.items():
                    if uuid in existing_uuids:
                        # Process this card's price data
                        from mtg_utils.card_processing import extract_tcgplayer_price

                        avg_price = extract_tcgplayer_price(card_price_data)

                        if avg_price is not None:
                            from datetime import date

                            today = date.today().isoformat()
                            price_batch.append((uuid, avg_price, today))

                    pbar.update(1)

                    # Insert batch when it reaches size limit
                    if len(price_batch) >= batch_size:
                        cursor = conn.cursor()
                        cursor.executemany(
                            INSERT_PRICE_QUERY,
                            price_batch,
                        )
                        conn.commit()
                        price_batch = []

                # Insert remaining data
                if price_batch:
                    cursor = conn.cursor()
                    cursor.executemany(
                        INSERT_PRICE_QUERY,
                        price_batch,
                    )
                    conn.commit()

            verify_price_data(conn)

        finally:
            conn.close()

        logger.info("✓ Price processing complete!")
        return 0

    except MTGProcessingError as e:
        logger.error(f"Price processing failed: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1


def process_collections_command(args: argparse.Namespace) -> int:
    """Process MTG collection files containing multiple sets."""
    try:
        paths = get_project_paths("collections")

        if not paths["gzipped"].exists():
            logger.error(f"Source directory {paths['gzipped']} does not exist!")
            return 1

        # Unzip files
        json_files = unzip_files(paths["gzipped"], paths["json"])

        if not json_files:
            logger.warning("No files to process!")
            return 0

        # Create database
        conn = create_database(paths["db"], fresh_start=args.fresh)
        optimize_sqlite_connection(conn)

        try:
            ensure_column_exists(conn, "cards", "collection_name", "TEXT")

            total_new = 0
            total_updated = 0
            total_skipped = 0

            # Process collections with progress
            with tqdm.tqdm(
                desc="Processing collections", unit="collection", total=len(json_files)
            ) as pbar:
                for json_file in json_files:
                    collection_name = json_file.stem
                    pbar.set_description(f"Processing {collection_name}")

                    # Read JSON data
                    data = read_json_file(json_file)
                    sets_data = data.get("data", {})

                    for set_code, set_data in sets_data.items():
                        if "cards" not in set_data or not set_data.get("cards"):
                            continue

                        # Process cards from this set
                        cards_data = process_set_cards(set_data, collection_name)

                        if cards_data:
                            new, updated, skipped = batch_insert_cards(conn, cards_data)
                            total_new += new
                            total_updated += updated
                            total_skipped += skipped

                    pbar.update(1)

            print_processing_summary(total_new, total_updated, total_skipped)
            verify_database(conn)

        finally:
            conn.close()

        logger.info("✓ Collection processing complete!")
        return 0

    except MTGProcessingError as e:
        logger.error(f"Collection processing failed: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1


def export_top_command(args: argparse.Namespace) -> int:
    """Export top N most expensive cards to CSV."""
    try:
        db_path = get_project_paths("sets")["db"]

        if not db_path.exists():
            logger.error(f"Database not found: {db_path}")
            return 1

        # Query top cards with progress
        import sqlite3

        conn = sqlite3.connect(db_path)

        try:
            query = GET_TOP_CARDS_WITH_PRICES

            with tqdm.tqdm(desc="Querying database") as pbar:
                cursor = conn.cursor()
                cursor.execute(query, (args.limit,))
                results = cursor.fetchall()
                pbar.update(1)

            if not results:
                logger.error("No card price data found!")
                return 1

            # Export to CSV
            output_filename = f"top_{args.limit}_cards.csv"
            output_path = Path(output_filename)

            with tqdm.tqdm(desc="Exporting to CSV", total=len(results)) as pbar:
                export_to_csv(results, output_path, CSV_HEADERS, round_prices=True)
                pbar.update(len(results))

            export_csv_preview(results, limit=10)
            logger.info(f"✓ Exported {len(results)} cards to {output_path}")

        finally:
            conn.close()

        return 0

    except MTGProcessingError as e:
        logger.error(f"Export failed: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1


def export_list_command(args: argparse.Namespace) -> int:
    """Export cards from a text list to CSV."""
    try:
        db_path = get_project_paths("sets")["db"]

        # Validate and resolve input file path
        try:
            input_path = Path(args.input_file).resolve()
            # Basic validation - ensure it's a reasonable file path
            if input_path.suffix.lower() not in ['.txt', '.csv', '.list', '.dec', '.mwdeck', '.mtgsdeck', '']:
                logger.warning(f"Unusual file extension for card list: {input_path.suffix}")
        except (OSError, ValueError) as e:
            logger.error(f"Invalid input file path: {args.input_file} - {e}")
            return 1

        if not db_path.exists():
            logger.error(f"Database not found: {db_path}")
            return 1

        if not input_path.exists():
            logger.error(f"Input file not found: {input_path}")
            return 1

        # Generate output filename if not provided
        if args.output_file:
            try:
                output_path = Path(args.output_file).resolve()
                # Ensure output has .csv extension
                if not output_path.suffix.lower() == '.csv':
                    output_path = output_path.with_suffix('.csv')
            except (OSError, ValueError) as e:
                logger.error(f"Invalid output file path: {args.output_file} - {e}")
                return 1
        else:
            output_path = input_path.with_suffix("").with_suffix(".csv")
            output_path = output_path.parent / f"{output_path.stem}_prices.csv"

        # Read card list
        card_names = read_card_list(input_path)

        if not card_names:
            logger.error("No card names found in input file!")
            return 1

        # Query database with progress
        import sqlite3

        conn = sqlite3.connect(db_path)

        try:
            with tqdm.tqdm(desc="Processing card list", total=3) as pbar:
                # Create temporary table
                create_temp_table_from_list(conn, "temp_card_list", card_names)
                pbar.update(1)

                # Query for cheapest versions
                query = GET_CARDS_FROM_LIST

                cursor = conn.cursor()
                cursor.execute(query)
                results = cursor.fetchall()
                pbar.update(1)

                # Export to CSV
                export_to_csv(results, output_path, CSV_HEADERS, round_prices=True)
                pbar.update(1)

            export_csv_preview(results, limit=10)
            print_collection_summary(results, len(card_names))

            logger.info(f"✓ Exported {len(results)} cards to {output_path}")

        finally:
            conn.close()

        return 0

    except MTGProcessingError as e:
        logger.error(f"Export failed: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1


def download_sets_command(args: argparse.Namespace) -> int:
    """Download individual MTG sets from MTGJSON."""
    try:
        # Validate set codes
        set_codes = validate_set_codes(args.sets)

        # Get destination directory
        paths = get_project_paths("sets")
        dest_dir = paths["gzipped"]

        logger.info(f"Downloading {len(set_codes)} sets: {', '.join(set_codes)}")

        # Download sets
        downloaded_files = download_sets(set_codes, dest_dir, clear_existing=args.fresh)

        if downloaded_files:
            logger.info(
                f"✓ Successfully downloaded {len(downloaded_files)} sets to {dest_dir}"
            )
            for file_path in downloaded_files:
                logger.info(f"  - {file_path.name}")

        return 0

    except (DownloadError, ValueError) as e:
        logger.error(f"Download failed: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1


def download_collection_command(args: argparse.Namespace) -> int:
    """Download a MTG collection from MTGJSON."""
    try:
        collection_name = args.collection

        # Get destination directory
        paths = get_project_paths("collections")
        dest_dir = paths["gzipped"]

        logger.info(f"Downloading {collection_name} collection")

        # Download collection
        downloaded_file = download_collection(
            collection_name, dest_dir, clear_existing=args.fresh
        )

        logger.info(
            f"✓ Successfully downloaded {collection_name} collection to {downloaded_file}"
        )

        return 0

    except (DownloadError, ValueError) as e:
        logger.error(f"Download failed: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1


def download_prices_command(args: argparse.Namespace) -> int:
    """Download price data from MTGJSON."""
    try:
        # Get destination directory
        paths = get_project_paths("prices")
        dest_dir = paths["gzipped"]

        logger.info("Downloading price data")

        # Download prices
        downloaded_file = download_prices(dest_dir, clear_existing=args.fresh)

        logger.info(f"✓ Successfully downloaded price data to {downloaded_file}")

        return 0

    except DownloadError as e:
        logger.error(f"Download failed: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="MTG Data Processing Toolkit",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Download data
    mtg download-sets ZEN WWK ROE --fresh
    mtg download-collection Legacy --fresh
    mtg download-prices --fresh

    # Process card data
    mtg process-cards --fresh --log-level DEBUG

    # Process prices
    mtg process-prices

    # Export top expensive cards
    mtg export-top 500

    # Export from card list (supports multiple formats)
    mtg export-list cube.txt --output cube_prices.csv
    mtg export-list deck.dec
    mtg export-list deck.mwDeck
    mtg export-list deck.mtgsDeck
        """,
    )

    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Set logging level (DEBUG, INFO, WARNING, ERROR)",
    )

    # Create subparsers
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Process cards command
    cards_parser = subparsers.add_parser(
        "process-cards", help="Process MTG card data from individual set files"
    )
    cards_parser.add_argument(
        "--fresh",
        action="store_true",
        help="Drop existing tables and start fresh (WARNING: deletes data)",
    )

    # Process prices command
    subparsers.add_parser(
        "process-prices", help="Process MTG price data from AllPrices.json.gz"
    )

    # Process collections command
    collections_parser = subparsers.add_parser(
        "process-collections", help="Process collection files containing multiple sets"
    )
    collections_parser.add_argument(
        "--fresh",
        action="store_true",
        help="Drop existing tables and start fresh (WARNING: deletes data)",
    )

    # Export top cards command
    top_parser = subparsers.add_parser(
        "export-top", help="Export top N most expensive cards to CSV"
    )

    def validate_positive_int(value):
        """Validate that a value is a positive integer."""
        try:
            ivalue = int(value)
            if ivalue <= 0:
                raise argparse.ArgumentTypeError(f"'{value}' must be a positive integer")
            if ivalue > 100000:
                raise argparse.ArgumentTypeError(f"'{value}' is too large (max: 100000)")
            return ivalue
        except ValueError:
            raise argparse.ArgumentTypeError(f"'{value}' is not a valid integer")

    top_parser.add_argument(
        "limit",
        type=validate_positive_int,
        nargs="?",
        default=DEFAULT_EXPORT_LIMIT,
        help=f"Number of cards to export (default: {DEFAULT_EXPORT_LIMIT})",
    )

    # Export from list command
    list_parser = subparsers.add_parser(
        "export-list", help="Export cards from deck lists to CSV (supports .txt, .dec, .mwDeck, .mtgsDeck formats)"
    )
    list_parser.add_argument(
        "input_file", help="Deck list file (supports plain text, MTGO, .dec, .mwDeck, .mtgsDeck formats with quantities and set codes)"
    )
    list_parser.add_argument(
        "--output",
        dest="output_file",
        help="Output CSV filename (default: <input_name>_prices.csv)",
    )

    # Download sets command
    sets_download_parser = subparsers.add_parser(
        "download-sets", help="Download individual MTG sets from MTGJSON"
    )
    sets_download_parser.add_argument(
        "sets", nargs="+", help="Set codes to download (e.g. ZEN WWK ROE)"
    )
    sets_download_parser.add_argument(
        "--fresh",
        action="store_true",
        help="Clear existing set data before downloading",
    )

    # Download collection command
    collection_download_parser = subparsers.add_parser(
        "download-collection", help="Download MTG collection from MTGJSON"
    )
    collection_download_parser.add_argument(
        "collection",
        choices=get_available_collections(),
        help="Collection name to download",
    )
    collection_download_parser.add_argument(
        "--fresh",
        action="store_true",
        help="Clear existing collection data before downloading",
    )

    # Download prices command
    prices_download_parser = subparsers.add_parser(
        "download-prices", help="Download AllPrices.json.gz from MTGJSON"
    )
    prices_download_parser.add_argument(
        "--fresh",
        action="store_true",
        help="Clear existing price data before downloading",
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    # Set up environment
    setup_environment(log_level=args.log_level)
    setup_tqdm_logging()

    # Route to appropriate command
    commands = {
        "process-cards": process_cards_command,
        "process-prices": process_prices_command,
        "process-collections": process_collections_command,
        "export-top": export_top_command,
        "export-list": export_list_command,
        "download-sets": download_sets_command,
        "download-collection": download_collection_command,
        "download-prices": download_prices_command,
    }

    command_func = commands.get(args.command)
    if command_func:
        return command_func(args)
    else:
        logger.error(f"Unknown command: {args.command}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
