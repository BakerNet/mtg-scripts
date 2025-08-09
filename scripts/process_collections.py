#!/usr/bin/env python3
"""
Script to unzip MTG collection JSON files and import them into a SQLite database.

This script processes collection files that contain multiple sets in a single JSON:
1. Unzips .json.gz files from collections/gzipped/ to collections/json/
2. Creates a SQLite database at db/cards.db
3. Imports all card data from all sets within the unzipped JSON files

Requires Python 3.10+
"""

import argparse
import logging
from pathlib import Path

from mtg_utils import (
    batch_insert_cards,
    create_database,
    ensure_column_exists,
    get_project_paths,
    process_set_cards,
    read_json_file,
    setup_environment,
    unzip_files,
    verify_database,
)

logger = logging.getLogger(__name__)


def import_collection(conn, json_file: Path) -> tuple[int, int, int]:
    """Import card data from a collection JSON file into the database."""
    collection_name = json_file.stem  # Use filename as collection name

    logger.info(f"Processing collection: {collection_name}")

    # Read JSON data
    data = read_json_file(json_file)
    sets_data = data.get("data", {})

    total_new = 0
    total_updated = 0
    total_skipped = 0
    sets_processed = 0

    for set_code, set_data in sets_data.items():
        # Skip if no cards in this set
        if "cards" not in set_data or not set_data.get("cards"):
            continue

        sets_processed += 1
        logger.debug(f"  Processing set: {set_code}")

        # Process cards from this set (include collection name)
        cards_data = process_set_cards(set_data, collection_name)

        if cards_data:
            # Batch insert cards
            new, updated, skipped = batch_insert_cards(conn, cards_data)
            total_new += new
            total_updated += updated
            total_skipped += skipped

    logger.info(f"✓ Processed {sets_processed} sets from {collection_name}:")
    logger.info(
        f"  - New cards: {total_new}, Updated: {total_updated}, Skipped: {total_skipped}"
    )

    return total_new, total_updated, total_skipped


def import_all_collections(conn, json_files: list[Path]) -> tuple[int, int, int]:
    """Import card data from all collection JSON files."""
    total_new = 0
    total_updated = 0
    total_skipped = 0

    for json_file in json_files:
        new, updated, skipped = import_collection(conn, json_file)
        total_new += new
        total_updated += updated
        total_skipped += skipped

    print(f"\n{'='*50}")
    print("OVERALL SUMMARY")
    print(f"{'='*50}")
    print("✓ Total processing complete:")
    print(f"  - Collections processed: {len(json_files)}")
    print(f"  - New cards imported: {total_new:,}")
    if total_updated > 0:
        print(f"  - Cards updated: {total_updated:,}")
    if total_skipped > 0:
        print(f"  - Cards skipped: {total_skipped:,}")

    return total_new, total_updated, total_skipped


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Process MTG card data from collection files containing multiple sets",
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

    print("MTG Collection Card Processing Script")
    print("=" * 50)

    if args.fresh:
        print("⚠️  FRESH START MODE: All existing card data will be deleted!")
        response = input("Are you sure you want to continue? (yes/no): ")
        if response.lower() != "yes":
            print("Aborted.")
            return

    try:
        # Get project paths
        paths = get_project_paths("collections")

        # Check if source directory exists
        if not paths["gzipped"].exists():
            logger.error(f"Source directory {paths['gzipped']} does not exist!")
            logger.error(
                "Please ensure your collection files are in collections/gzipped/"
            )
            return

        # Unzip files
        json_files = unzip_files(paths["gzipped"], paths["json"])

        if not json_files:
            logger.warning("No files to process!")
            return

        # Create database
        conn = create_database(paths["db"], fresh_start=args.fresh)

        try:
            # Ensure collection_name column exists for backward compatibility
            ensure_column_exists(conn, "cards", "collection_name", "TEXT")

            # Import cards from all collections
            total_new, total_updated, total_skipped = import_all_collections(
                conn, json_files
            )

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
