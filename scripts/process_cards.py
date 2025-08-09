#!/usr/bin/env python3
"""
Script to unzip MTG card JSON files and import them into a SQLite database.

This script:
1. Unzips all .json.gz files from sets/gzipped/ to sets/json/
2. Creates a SQLite database at db/cards.db
3. Imports all card data from the unzipped JSON files

Requires Python 3.10+
"""

import argparse
import logging
from pathlib import Path

from mtg_utils import (
    batch_insert_cards,
    create_database,
    get_project_paths,
    print_processing_summary,
    process_set_cards,
    read_json_file,
    setup_environment,
    unzip_files,
    verify_database,
)

logger = logging.getLogger(__name__)


def import_cards(conn, json_files: list[Path]) -> int:
    """Import card data from JSON files into the database."""
    total_new = 0
    total_updated = 0
    total_skipped = 0

    for json_file in json_files:
        logger.info(f"Processing {json_file.name}...")

        # Read and parse JSON file
        data = read_json_file(json_file)
        set_data = data.get("data", {})

        # Process cards from this set
        cards_data = process_set_cards(set_data)

        if cards_data:
            # Batch insert cards
            new, updated, skipped = batch_insert_cards(conn, cards_data)
            total_new += new
            total_updated += updated
            total_skipped += skipped

            logger.info(f"  {new} new, {updated} updated, {skipped} skipped cards")

    print_processing_summary(total_new, total_updated, total_skipped)
    return total_new


def main():
    """Main execution function."""
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="Process MTG card data from individual set files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Drop existing cards table and start fresh (WARNING: This will delete all existing data)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Set logging level (DEBUG, INFO, WARNING, ERROR)",
    )

    args = parser.parse_args()

    # Set up environment
    setup_environment(log_level=args.log_level)

    print("MTG Card Processing Script")
    print("=" * 50)

    if args.fresh:
        print("⚠️  FRESH START MODE: All existing card data will be deleted!")
        response = input("Are you sure you want to continue? (yes/no): ")
        if response.lower() != "yes":
            print("Aborted.")
            return

    try:
        # Get project paths
        paths = get_project_paths("sets")

        # Check source directory exists
        if not paths["gzipped"].exists():
            logger.error(f"Source directory not found: {paths['gzipped']}")
            return

        # Unzip files
        json_files = unzip_files(paths["gzipped"], paths["json"])

        if not json_files:
            logger.warning("No JSON files to process!")
            return

        # Create database
        conn = create_database(paths["db"], fresh_start=args.fresh)

        try:
            # Import cards
            import_cards(conn, json_files)

            # Verify results
            verify_database(conn)

        finally:
            conn.close()

        print("\n" + "=" * 50)
        print("✓ Processing complete!")
        print(f"  - JSON files: {paths['json']}/")
        print(f"  - Database:   {paths['db']}")
        print("=" * 50)

    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise


if __name__ == "__main__":
    main()
