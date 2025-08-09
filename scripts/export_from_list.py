#!/usr/bin/env python3
"""
Script to export MTG card prices from a provided list of card names.

This script:
1. Reads a text file containing card names (one per line)
2. Looks up each card in the database
3. Deduplicates by selecting the least expensive version of each card
4. Exports to CSV sorted by price (descending)

Requires Python 3.10+

Usage:
    python export_cards_from_list.py <input_file> [output_file]

Examples:
    python export_cards_from_list.py 2000sStandardCube.txt
    python export_cards_from_list.py cube.txt cube_prices.csv
"""

import argparse
import logging
import sqlite3
from pathlib import Path

from mtg_utils import get_project_paths, setup_environment
from mtg_utils.constants import CSV_HEADERS
from mtg_utils.database import create_temp_table_from_list
from mtg_utils.file_operations import read_card_list
from mtg_utils.reporting import (
    export_csv_preview,
    export_to_csv,
    print_collection_summary,
)

logger = logging.getLogger(__name__)


def query_cards_from_list(
    db_path: Path, card_names: list[str]
) -> tuple[list[tuple], list[str]]:
    """Query database for cards from a list, getting cheapest version of each.

    Args:
        db_path: Path to the database
        card_names: List of card names to look up

    Returns:
        Tuple of (results, missing_cards)
    """
    conn = sqlite3.connect(db_path)

    try:
        # Create temporary table for card names
        create_temp_table_from_list(conn, "card_list", card_names)

        # Query for cards, getting the cheapest version of each
        query = """
            WITH cheapest_cards AS (
                SELECT c.name, MIN(cp.average_price) as min_price
                FROM cards c
                JOIN card_prices cp ON c.uuid = cp.uuid
                JOIN card_list cl ON c.name = cl.name
                GROUP BY c.name
            )
            SELECT c.name, c.set_code, c.set_name, cp.average_price
            FROM cards c
            JOIN card_prices cp ON c.uuid = cp.uuid
            JOIN cheapest_cards cc ON c.name = cc.name AND cp.average_price = cc.min_price
            GROUP BY c.name  -- In case of tie, just pick one
            ORDER BY cp.average_price DESC
        """

        logger.info("Querying database for card prices...")
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()

        # Find cards without prices
        cursor.execute(
            """
            SELECT DISTINCT cl.name
            FROM card_list cl
            LEFT JOIN cards c ON cl.name = c.name
            LEFT JOIN card_prices cp ON c.uuid = cp.uuid
            WHERE c.uuid IS NULL OR cp.average_price IS NULL
        """
        )
        missing_cards = [row[0] for row in cursor.fetchall()]

        return results, missing_cards

    finally:
        conn.close()


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Export MTG card prices from a list of card names",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    %(prog)s 2000sStandardCube.txt
    %(prog)s cube.txt cube_prices.csv
        """,
    )
    parser.add_argument(
        "input_file", type=str, help="Text file containing card names (one per line)"
    )
    parser.add_argument(
        "output_file",
        type=str,
        nargs="?",
        help="Output CSV filename (default: <input_name>_prices.csv)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Set logging level (DEBUG, INFO, WARNING, ERROR)",
    )

    args = parser.parse_args()

    # Set up environment
    setup_environment(log_level=args.log_level)

    print("MTG Card List Price Export")
    print("=" * 40)

    try:
        # Set up paths
        db_path = get_project_paths("sets")["db"]
        input_path = Path(args.input_file)

        # Generate output filename if not provided
        if args.output_file:
            output_path = Path(args.output_file)
        else:
            output_path = input_path.with_suffix("").with_suffix(".csv")
            output_path = output_path.parent / f"{output_path.stem}_prices.csv"

        # Check if database exists
        if not db_path.exists():
            logger.error(f"Database not found: {db_path}")
            logger.error("Please run the card and price processing scripts first.")
            return

        # Check if input file exists
        if not input_path.exists():
            logger.error(f"Input file not found: {input_path}")
            return

        # Read card list
        card_names = read_card_list(input_path)

        if not card_names:
            logger.error("No card names found in input file!")
            return

        # Query for card prices
        results, missing_cards = query_cards_from_list(db_path, card_names)

        if not results:
            logger.error("No price data found for any cards in the list!")
            if missing_cards:
                logger.info("Cards not found or without prices:")
                for name in missing_cards[:20]:  # Show first 20
                    logger.info(f"  - {name}")
                if len(missing_cards) > 20:
                    logger.info(f"  ... and {len(missing_cards) - 20} more")
            return

        logger.info(f"✓ Found price data for {len(results)} cards")

        if missing_cards:
            logger.warning(f"{len(missing_cards)} cards not found or without prices")
            if len(missing_cards) <= 10:
                for name in missing_cards:
                    logger.warning(f"  - {name}")

        # Export to CSV
        export_to_csv(results, output_path, CSV_HEADERS, round_prices=True)

        # Show preview
        export_csv_preview(results, limit=10)

        # Show collection summary
        print_collection_summary(results, len(card_names))

        print("\n" + "=" * 40)
        print("✓ Export complete!")
        print(f"  Input file: {input_path}")
        print(f"  Output file: {output_path}")
        print("=" * 40)

    except Exception as e:
        logger.error(f"Export failed: {e}")
        raise


if __name__ == "__main__":
    main()
