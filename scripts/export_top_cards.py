#!/usr/bin/env python3
"""
Script to export the top N most expensive MTG cards to CSV.

This script:
1. Queries the database for the top N most expensive cards (default 100)
2. Rounds prices up to the nearest dollar
3. Exports to CSV with card name, set, and price columns

Requires Python 3.10+

Usage:
    python export_top_cards.py [number_of_cards]

Examples:
    python export_top_cards.py        # Exports top 100 cards (default)
    python export_top_cards.py 50     # Exports top 50 cards
    python export_top_cards.py 200    # Exports top 200 cards
"""

import argparse
import logging
import sqlite3
from pathlib import Path

from mtg_utils import get_project_paths, setup_environment
from mtg_utils.constants import CSV_HEADERS, DEFAULT_EXPORT_LIMIT
from mtg_utils.reporting import export_csv_preview, export_to_csv

logger = logging.getLogger(__name__)


def query_top_cards(db_path: Path, limit: int) -> list[tuple]:
    """Query for top N most expensive cards.

    Args:
        db_path: Path to the database
        limit: Number of cards to retrieve

    Returns:
        List of (name, set_code, set_name, price) tuples
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
        SELECT c.name, c.set_code, c.set_name, cp.average_price
        FROM cards c
        JOIN card_prices cp ON c.uuid = cp.uuid
        ORDER BY cp.average_price DESC
        LIMIT ?
    """

    logger.info(f"Querying top {limit} most expensive cards...")
    cursor.execute(query, (limit,))
    results = cursor.fetchall()
    conn.close()

    return results


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Export top N most expensive MTG cards to CSV",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    %(prog)s           # Export top 100 cards (default)
    %(prog)s 50        # Export top 50 cards
    %(prog)s 200       # Export top 200 cards
        """,
    )
    parser.add_argument(
        "limit",
        type=int,
        nargs="?",
        default=DEFAULT_EXPORT_LIMIT,
        help=f"Number of cards to export (default: {DEFAULT_EXPORT_LIMIT})",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Set logging level (DEBUG, INFO, WARNING, ERROR)",
    )

    args = parser.parse_args()

    # Validate the limit
    if args.limit <= 0:
        print("❌ Error: Number of cards must be greater than 0")
        return

    # Set up environment
    setup_environment(log_level=args.log_level)

    print("MTG Top Cards CSV Export")
    print("=" * 40)

    try:
        # Get database path
        db_path = get_project_paths("sets")["db"]

        # Check if database exists
        if not db_path.exists():
            logger.error(f"Database not found: {db_path}")
            logger.error("Please run the card and price processing scripts first.")
            return

        # Query top cards
        results = query_top_cards(db_path, args.limit)

        if not results:
            logger.error("No card price data found!")
            return

        logger.info(f"✓ Found {len(results)} cards")

        # Set up output path
        output_filename = f"top_{args.limit}_cards.csv"
        output_path = Path(output_filename)

        # Export to CSV
        export_to_csv(results, output_path, CSV_HEADERS, round_prices=True)

        # Show preview
        export_csv_preview(results, limit=10)

        print("\n" + "=" * 40)
        print("✓ Export complete!")
        print(f"  Output file: {output_path}")
        print("=" * 40)

    except Exception as e:
        logger.error(f"Export failed: {e}")
        raise


if __name__ == "__main__":
    main()
