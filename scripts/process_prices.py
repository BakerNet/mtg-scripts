#!/usr/bin/env python3
"""
Script to process MTG card prices from AllPrices.json.gz and store them in the database.

This script:
1. Unzips AllPrices.json.gz from prices/gzipped/ to prices/json/
2. Creates a card_prices table with foreign key to cards table
3. Calculates average TCGPlayer paper non-foil prices for each card
4. Inserts prices only for cards that exist in the database

Requires Python 3.10+
"""

import argparse
import logging
import sqlite3

from mtg_utils import (
    create_price_table,
    get_existing_card_uuids,
    get_project_paths,
    process_price_batch,
    read_json_file,
    setup_environment,
    unzip_single_file,
    verify_price_data,
)

logger = logging.getLogger(__name__)


def process_all_price_data(
    conn: sqlite3.Connection, price_data: dict
) -> tuple[int, int, int]:
    """Process all price data and insert into database.

    Args:
        conn: Database connection
        price_data: Full price data dictionary

    Returns:
        Tuple of (cards_with_prices, cards_without_prices, cards_not_in_db)
    """
    # Get existing card UUIDs
    logger.info("Loading existing card UUIDs...")
    existing_uuids = get_existing_card_uuids(conn)
    logger.info(f"✓ Found {len(existing_uuids)} cards in database")

    # Process price entries
    all_price_entries = price_data.get("data", {})
    total_entries = len(all_price_entries)
    logger.info(f"Processing {total_entries:,} price entries...")

    # Batch process prices
    price_batch = process_price_batch(all_price_entries, existing_uuids)

    cards_with_prices = len(price_batch)
    cards_not_in_db = sum(1 for uuid in all_price_entries if uuid not in existing_uuids)
    cards_without_prices = total_entries - cards_with_prices - cards_not_in_db

    # Insert price data
    if price_batch:
        cursor = conn.cursor()
        cursor.executemany(
            """
            INSERT OR REPLACE INTO card_prices (uuid, average_price, price_date)
            VALUES (?, ?, ?)
        """,
            price_batch,
        )
        conn.commit()
        logger.info(f"✓ Inserted {len(price_batch):,} price records")

    return cards_with_prices, cards_without_prices, cards_not_in_db


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Process MTG card prices from AllPrices.json.gz",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Set logging level (DEBUG, INFO, WARNING, ERROR)",
    )

    args = parser.parse_args()

    # Set up environment
    setup_environment(log_level=args.log_level)

    print("MTG Price Processing Script")
    print("=" * 50)

    try:
        # Get project paths
        paths = get_project_paths("prices")
        db_path = get_project_paths("sets")["db"]  # Database is in sets project

        # Check if source file exists
        source_file = paths["gzipped"] / "AllPrices.json.gz"
        if not source_file.exists():
            logger.error(f"Price file not found: {source_file}")
            return

        # Check if database exists
        if not db_path.exists():
            logger.error(f"Database not found: {db_path}")
            logger.error(
                "Please run process_mtg_cards.py first to create the database."
            )
            return

        # Unzip price file
        json_path = unzip_single_file(source_file, paths["json"])

        # Connect to database
        conn = sqlite3.connect(db_path)

        try:
            # Create price table
            create_price_table(conn)

            # Load and process price data
            logger.info("Loading price data...")
            price_data = read_json_file(json_path)

            # Process all price data
            with_prices, without_prices, not_in_db = process_all_price_data(
                conn, price_data
            )

            # Print summary
            print("\n✓ Price processing complete:")
            print(f"  - Cards with prices inserted: {with_prices:,}")
            print(f"  - Cards without TCGPlayer non-foil prices: {without_prices:,}")
            print(f"  - Price entries not in database: {not_in_db:,}")

            # Verify results
            verify_price_data(conn)

        finally:
            conn.close()

        print("\n" + "=" * 50)
        print("✓ Price processing complete!")
        print(f"  - Price JSON: {json_path}")
        print(f"  - Database:   {db_path}")
        print("=" * 50)

    except Exception as e:
        logger.error(f"Price processing failed: {e}")
        raise


if __name__ == "__main__":
    main()
