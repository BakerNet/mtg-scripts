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
    batch_insert_cards,
    create_database,
    create_price_table,
    download_all_data,
    get_existing_card_uuids,
    get_project_paths,
    optimize_sqlite_connection,
    process_all_printings_cards,
    read_json_file,
    setup_environment,
    unzip_single_file,
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
    GET_CARDS_FROM_LIST_WITH_FORMATS_FILTER,
    GET_CARDS_FROM_LIST_WITH_SETS_AND_FORMATS_FILTER,
    GET_CARDS_FROM_LIST_WITH_SETS_FILTER,
    GET_TOP_CARDS_WITH_FORMATS_FILTER,
    GET_TOP_CARDS_WITH_PRICES,
    GET_TOP_CARDS_WITH_SETS_AND_FORMATS_FILTER,
    GET_TOP_CARDS_WITH_SETS_FILTER,
    INSERT_PRICE_QUERY,
)

logger = logging.getLogger(__name__)


def parse_filter_list(filter_string: str | None) -> list[str]:
    """Parse comma-separated filter string into list.

    Args:
        filter_string: Comma-separated string like "ZEN,WWK,ROE"

    Returns:
        List of cleaned filter values
    """
    if not filter_string:
        return []

    return [item.strip().upper() for item in filter_string.split(",") if item.strip()]


def build_filtered_query(
    sets_filter: list[str] = None, formats_filter: list[str] = None
) -> tuple[str, list[str]]:
    """Build appropriate SQL query based on filters for top cards.

    Args:
        sets_filter: List of set codes to filter by
        formats_filter: List of formats to filter by

    Returns:
        Tuple of (sql_query, parameters_list)
    """
    if sets_filter and formats_filter:
        # Both filters
        set_placeholders = ",".join(["?"] * len(sets_filter))
        format_placeholders = ",".join(["?"] * len(formats_filter))
        query = GET_TOP_CARDS_WITH_SETS_AND_FORMATS_FILTER.format(
            set_placeholders=set_placeholders, format_placeholders=format_placeholders
        )
        params = sets_filter + formats_filter
    elif sets_filter:
        # Only sets filter
        set_placeholders = ",".join(["?"] * len(sets_filter))
        query = GET_TOP_CARDS_WITH_SETS_FILTER.format(set_placeholders=set_placeholders)
        params = sets_filter
    elif formats_filter:
        # Only formats filter
        format_placeholders = ",".join(["?"] * len(formats_filter))
        query = GET_TOP_CARDS_WITH_FORMATS_FILTER.format(
            format_placeholders=format_placeholders
        )
        params = formats_filter
    else:
        # No filters
        query = GET_TOP_CARDS_WITH_PRICES
        params = []

    return query, params


def build_list_filtered_query(
    sets_filter: list[str] = None, formats_filter: list[str] = None
) -> tuple[str, list[str]]:
    """Build appropriate SQL query based on filters for card lists.

    Args:
        sets_filter: List of set codes to filter by
        formats_filter: List of formats to filter by

    Returns:
        Tuple of (sql_query, parameters_list)
    """
    if sets_filter and formats_filter:
        # Both filters
        set_placeholders = ",".join(["?"] * len(sets_filter))
        format_placeholders = ",".join(["?"] * len(formats_filter))
        query = GET_CARDS_FROM_LIST_WITH_SETS_AND_FORMATS_FILTER.format(
            set_placeholders=set_placeholders, format_placeholders=format_placeholders
        )
        params = sets_filter + formats_filter
    elif sets_filter:
        # Only sets filter
        set_placeholders = ",".join(["?"] * len(sets_filter))
        query = GET_CARDS_FROM_LIST_WITH_SETS_FILTER.format(
            set_placeholders=set_placeholders
        )
        params = sets_filter
    elif formats_filter:
        # Only formats filter
        format_placeholders = ",".join(["?"] * len(formats_filter))
        query = GET_CARDS_FROM_LIST_WITH_FORMATS_FILTER.format(
            format_placeholders=format_placeholders
        )
        params = formats_filter
    else:
        # No filters
        query = GET_CARDS_FROM_LIST
        params = []

    return query, params


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


def setup_command(args: argparse.Namespace) -> int:
    """Complete setup: download all data, process cards, and add prices."""
    try:
        logger.info("Starting complete MTG database setup...")

        # Step 1: Download all data
        logger.info("Step 1/4: Downloading data from MTGJSON...")
        cards_file, prices_file = download_all_data(clear_existing=args.fresh)
        logger.info("✓ Downloaded card database and prices")

        # Step 2: Process all cards
        logger.info("Step 2/4: Processing card data...")
        paths = get_project_paths("sets")

        # Unzip AllPrintings
        json_path = unzip_single_file(cards_file, paths["json"])

        # Create database with fresh flag
        conn = create_database(paths["db"], fresh_start=args.fresh)
        optimize_sqlite_connection(conn)

        try:
            # Set up connection pool
            connection_pool = ConnectionPool(paths["db"], max_connections=4)
            batch_processor = BatchProcessor(connection_pool, batch_size=5000)

            # Load and process AllPrintings data
            all_printings_data = read_json_file(json_path)
            cards_data = process_all_printings_cards(all_printings_data)

            if not cards_data:
                logger.error("No card data found!")
                return 1

            logger.info(f"Processing {len(cards_data)} cards...")

            # Batch insert cards
            def process_batch(conn, batch):
                return batch_insert_cards(conn, batch)

            with tqdm.tqdm(
                desc="Inserting cards", unit="card", total=len(cards_data)
            ) as pbar:
                stats = batch_processor.process_batches(
                    cards_data,
                    process_batch,
                    progress_callback=lambda c, t: pbar.update(c - pbar.n),
                )

            logger.info(f"✓ Processed {stats.processed_items} cards")

            # Step 3: Process prices
            logger.info("Step 3/4: Processing price data...")

            # Unzip prices
            prices_paths = get_project_paths("prices")
            price_json_path = unzip_single_file(prices_file, prices_paths["json"])

            # Create price table
            create_price_table(conn)

            # Load price data
            price_data = read_json_file(price_json_path)
            existing_uuids = get_existing_card_uuids(conn)

            # Process prices
            all_price_entries = price_data.get("data", {})
            price_batch = []
            batch_size = 1000
            prices_added = 0

            with tqdm.tqdm(
                desc="Processing prices", unit="card", total=len(all_price_entries)
            ) as pbar:
                for uuid, card_price_data in all_price_entries.items():
                    if uuid in existing_uuids:
                        from mtg_utils.card_processing import extract_tcgplayer_price

                        avg_price = extract_tcgplayer_price(card_price_data)

                        if avg_price is not None:
                            from datetime import date

                            today = date.today().isoformat()
                            price_batch.append((uuid, avg_price, today))
                            prices_added += 1

                    pbar.update(1)

                    # Insert batch when it reaches size limit
                    if len(price_batch) >= batch_size:
                        cursor = conn.cursor()
                        cursor.executemany(INSERT_PRICE_QUERY, price_batch)
                        conn.commit()
                        price_batch = []

                # Insert remaining data
                if price_batch:
                    cursor = conn.cursor()
                    cursor.executemany(INSERT_PRICE_QUERY, price_batch)
                    conn.commit()

            logger.info(f"✓ Added prices for {prices_added} cards")

            # Step 4: Verify
            logger.info("Step 4/4: Verifying database...")
            verify_database(conn)
            verify_price_data(conn)

        finally:
            conn.close()
            if "connection_pool" in locals():
                connection_pool.close_all()

        logger.info("✅ Setup complete! You can now use export commands:")
        logger.info("  • mtg export-top 100")
        logger.info("  • mtg export-list deck.txt --sets ZEN,WWK")
        return 0

    except Exception as e:
        logger.error(f"Setup failed: {e}")
        return 1


def update_command(args: argparse.Namespace) -> int:
    """Update database with latest cards and prices."""
    try:
        logger.info("Updating MTG database...")

        # Check if database exists
        db_path = get_project_paths("sets")["db"]
        if not db_path.exists():
            logger.error("Database not found! Please run 'mtg setup' first.")
            return 1

        # Step 1: Download latest data
        logger.info("Step 1/3: Downloading latest data from MTGJSON...")
        cards_file, prices_file = download_all_data(clear_existing=True)
        logger.info("✓ Downloaded latest data")

        # Step 2: Update cards (new cards only)
        logger.info("Step 2/3: Updating card database...")
        paths = get_project_paths("sets")

        # Unzip AllPrintings
        json_path = unzip_single_file(cards_file, paths["json"])

        # Connect to existing database
        conn = create_database(paths["db"], fresh_start=False)
        optimize_sqlite_connection(conn)

        try:
            # Set up connection pool
            connection_pool = ConnectionPool(paths["db"], max_connections=4)
            batch_processor = BatchProcessor(connection_pool, batch_size=5000)

            # Load and process AllPrintings data
            all_printings_data = read_json_file(json_path)
            cards_data = process_all_printings_cards(all_printings_data)

            if not cards_data:
                logger.warning("No new card data found")
            else:
                logger.info(
                    f"Processing {len(cards_data)} cards (new cards will be added)..."
                )

                # Batch insert cards (INSERT OR REPLACE will update existing, add new)
                def process_batch(conn, batch):
                    return batch_insert_cards(conn, batch)

                with tqdm.tqdm(
                    desc="Updating cards", unit="card", total=len(cards_data)
                ) as pbar:
                    stats = batch_processor.process_batches(
                        cards_data,
                        process_batch,
                        progress_callback=lambda c, t: pbar.update(c - pbar.n),
                    )

                logger.info(
                    f"✓ Updated card database ({stats.processed_items} cards processed)"
                )

            # Step 3: Update ALL prices (replace old prices)
            logger.info("Step 3/3: Updating all prices...")

            # Unzip prices
            prices_paths = get_project_paths("prices")
            price_json_path = unzip_single_file(prices_file, prices_paths["json"])

            # Ensure price table exists
            create_price_table(conn)

            # Clear old prices first (we're replacing all)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM card_prices")
            conn.commit()
            logger.info("Cleared old prices")

            # Load price data
            price_data = read_json_file(price_json_path)
            existing_uuids = get_existing_card_uuids(conn)

            # Process all prices fresh
            all_price_entries = price_data.get("data", {})
            price_batch = []
            batch_size = 1000
            prices_added = 0

            with tqdm.tqdm(
                desc="Updating prices", unit="card", total=len(all_price_entries)
            ) as pbar:
                for uuid, card_price_data in all_price_entries.items():
                    if uuid in existing_uuids:
                        from mtg_utils.card_processing import extract_tcgplayer_price

                        avg_price = extract_tcgplayer_price(card_price_data)

                        if avg_price is not None:
                            from datetime import date

                            today = date.today().isoformat()
                            price_batch.append((uuid, avg_price, today))
                            prices_added += 1

                    pbar.update(1)

                    # Insert batch when it reaches size limit
                    if len(price_batch) >= batch_size:
                        cursor = conn.cursor()
                        cursor.executemany(INSERT_PRICE_QUERY, price_batch)
                        conn.commit()
                        price_batch = []

                # Insert remaining data
                if price_batch:
                    cursor = conn.cursor()
                    cursor.executemany(INSERT_PRICE_QUERY, price_batch)
                    conn.commit()

            logger.info(f"✓ Updated prices for {prices_added} cards")

            # Verify
            verify_database(conn)
            verify_price_data(conn)

        finally:
            conn.close()
            if "connection_pool" in locals():
                connection_pool.close_all()

        logger.info("✅ Update complete! Database is current.")
        return 0

    except Exception as e:
        logger.error(f"Update failed: {e}")
        return 1


def export_top_command(args: argparse.Namespace) -> int:
    """Export top N most expensive cards to CSV."""
    try:
        db_path = get_project_paths("sets")["db"]

        if not db_path.exists():
            logger.error(f"Database not found: {db_path}")
            return 1

        # Parse filters
        sets_filter = parse_filter_list(args.sets)
        formats_filter = parse_filter_list(args.formats)

        # Build query based on filters
        query, params = build_filtered_query(sets_filter, formats_filter)
        params.append(args.limit)  # Add limit parameter at the end

        # Build output filename
        filter_parts = []
        if sets_filter:
            filter_parts.append(f"sets_{'_'.join(sets_filter)}")
        if formats_filter:
            filter_parts.append(f"formats_{'_'.join(formats_filter)}")

        if filter_parts:
            output_filename = f"top_{args.limit}_cards_{'_'.join(filter_parts)}.csv"
        else:
            output_filename = f"top_{args.limit}_cards.csv"

        # Query database with progress
        import sqlite3

        conn = sqlite3.connect(db_path)

        try:
            with tqdm.tqdm(desc="Querying database") as pbar:
                cursor = conn.cursor()
                cursor.execute(query, params)
                results = cursor.fetchall()
                pbar.update(1)

            if not results:
                if sets_filter or formats_filter:
                    logger.error(
                        f"No cards found matching filters (sets: {sets_filter}, formats: {formats_filter})"
                    )
                else:
                    logger.error("No card price data found!")
                return 1

            # Log applied filters
            if sets_filter or formats_filter:
                filter_msg = []
                if sets_filter:
                    filter_msg.append(f"sets: {', '.join(sets_filter)}")
                if formats_filter:
                    filter_msg.append(f"formats: {', '.join(formats_filter)}")
                logger.info(f"Applied filters - {', '.join(filter_msg)}")

            # Export to CSV
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
            if input_path.suffix.lower() not in [
                ".txt",
                ".csv",
                ".list",
                ".dec",
                ".mwdeck",
                ".mtgsdeck",
                "",
            ]:
                logger.warning(
                    f"Unusual file extension for card list: {input_path.suffix}"
                )
        except (OSError, ValueError) as e:
            logger.error(f"Invalid input file path: {args.input_file} - {e}")
            return 1

        if not db_path.exists():
            logger.error(f"Database not found: {db_path}")
            return 1

        if not input_path.exists():
            logger.error(f"Input file not found: {input_path}")
            return 1

        # Parse filters
        sets_filter = parse_filter_list(args.sets)
        formats_filter = parse_filter_list(args.formats)

        # Generate output filename if not provided
        if args.output_file:
            try:
                output_path = Path(args.output_file).resolve()
                # Ensure output has .csv extension
                if not output_path.suffix.lower() == ".csv":
                    output_path = output_path.with_suffix(".csv")
            except (OSError, ValueError) as e:
                logger.error(f"Invalid output file path: {args.output_file} - {e}")
                return 1
        else:
            # Build output filename with filters
            filter_parts = []
            if sets_filter:
                filter_parts.append(f"sets_{'_'.join(sets_filter)}")
            if formats_filter:
                filter_parts.append(f"formats_{'_'.join(formats_filter)}")

            base_name = input_path.stem
            if filter_parts:
                output_filename = f"{base_name}_prices_{'_'.join(filter_parts)}.csv"
            else:
                output_filename = f"{base_name}_prices.csv"
            output_path = input_path.parent / output_filename

        # Read card list
        card_names = read_card_list(input_path)

        if not card_names:
            logger.error("No card names found in input file!")
            return 1

        # Build filtered query
        query, params = build_list_filtered_query(sets_filter, formats_filter)

        # Query database with progress
        import sqlite3

        conn = sqlite3.connect(db_path)

        try:
            with tqdm.tqdm(desc="Processing card list", total=3) as pbar:
                # Create temporary table
                create_temp_table_from_list(conn, "temp_card_list", card_names)
                pbar.update(1)

                # Log applied filters
                if sets_filter or formats_filter:
                    filter_msg = []
                    if sets_filter:
                        filter_msg.append(f"sets: {', '.join(sets_filter)}")
                    if formats_filter:
                        filter_msg.append(f"formats: {', '.join(formats_filter)}")
                    logger.info(f"Applied filters - {', '.join(filter_msg)}")

                # Query for cards with filters
                cursor = conn.cursor()
                cursor.execute(query, params)
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


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="MTG Data Processing Toolkit",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Initial setup (do this first!)
    mtg setup                    # Complete setup from scratch
    mtg setup --fresh            # Force clean setup

    # Keep data current
    mtg update                   # Update cards and prices

    # Export top expensive cards (with optional filters)
    mtg export-top 500
    mtg export-top 100 --sets ZEN,WWK,ROE
    mtg export-top 50 --formats Legacy,Modern
    mtg export-top 25 --sets ZEN,WWK --formats Legacy

    # Export from card list with filters
    mtg export-list cube.txt --sets ZEN,WWK,ROE
    mtg export-list deck.txt --formats Modern,Legacy
    mtg export-list cube.txt --output cube_prices.csv --sets ZEN,WWK
        """,
    )

    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Set logging level (DEBUG, INFO, WARNING, ERROR)",
    )

    # Create subparsers
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Setup command
    setup_parser = subparsers.add_parser(
        "setup", help="Complete setup: download all data, process cards, and add prices"
    )
    setup_parser.add_argument(
        "--fresh",
        action="store_true",
        help="Force fresh setup, clearing existing data",
    )

    # Update command
    subparsers.add_parser("update", help="Update database with latest cards and prices")

    # Export top cards command
    top_parser = subparsers.add_parser(
        "export-top", help="Export top N most expensive cards to CSV"
    )

    def validate_positive_int(value):
        """Validate that a value is a positive integer."""
        try:
            ivalue = int(value)
            if ivalue <= 0:
                raise argparse.ArgumentTypeError(
                    f"'{value}' must be a positive integer"
                )
            if ivalue > 100000:
                raise argparse.ArgumentTypeError(
                    f"'{value}' is too large (max: 100000)"
                )
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
    top_parser.add_argument(
        "--sets",
        help="Filter by set codes (comma-separated, e.g. ZEN,WWK,ROE)",
    )
    top_parser.add_argument(
        "--formats",
        help="Filter by formats (comma-separated, e.g. Legacy,Modern,Standard)",
    )

    # Export from list command
    list_parser = subparsers.add_parser(
        "export-list",
        help="Export cards from deck lists to CSV (supports .txt, .dec, .mwDeck, .mtgsDeck formats)",
    )
    list_parser.add_argument(
        "input_file",
        help="Deck list file (supports plain text, MTGO, .dec, .mwDeck, .mtgsDeck formats with quantities and set codes)",
    )
    list_parser.add_argument(
        "--output",
        dest="output_file",
        help="Output CSV filename (default: <input_name>_prices.csv)",
    )
    list_parser.add_argument(
        "--sets",
        help="Filter by set codes (comma-separated, e.g. ZEN,WWK,ROE)",
    )
    list_parser.add_argument(
        "--formats",
        help="Filter by formats (comma-separated, e.g. Legacy,Modern,Standard)",
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
        "setup": setup_command,
        "update": update_command,
        "export-top": export_top_command,
        "export-list": export_list_command,
    }

    command_func = commands.get(args.command)
    if command_func:
        return command_func(args)
    else:
        logger.error(f"Unknown command: {args.command}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
