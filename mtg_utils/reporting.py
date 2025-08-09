"""Progress reporting and verification utilities for MTG data processing.

Requires Python 3.10+
"""

import csv
import logging
import math
import sqlite3
from pathlib import Path
from typing import Any

from .database import (
    execute_query,
    get_card_count,
    get_cards_by_set,
    get_rarity_distribution,
)

logger = logging.getLogger(__name__)


def print_progress(
    current: int, total: int, prefix: str = "Processing", interval: int = 1000
) -> None:
    """Print progress updates at regular intervals.

    Args:
        current: Current item number
        total: Total number of items
        prefix: Prefix for the progress message
        interval: Interval at which to print updates
    """
    if current % interval == 0:
        percentage = (current / total) * 100 if total > 0 else 0
        logger.info(f"{prefix} {current:,}/{total:,} ({percentage:.1f}%)...")


def print_section_header(title: str, width: int = 50) -> None:
    """Print a formatted section header.

    Args:
        title: Section title
        width: Width of the header line
    """
    print("\n" + "=" * width)
    print(title)
    print("=" * width)


def print_summary(title: str, stats: dict[str, int | str], width: int = 50) -> None:
    """Print a summary section with statistics.

    Args:
        title: Summary title
        stats: Dictionary of statistic name -> value pairs
        width: Width of the header line
    """
    print("\n" + "=" * width)
    print(f"✓ {title}")
    for key, value in stats.items():
        if isinstance(value, int):
            print(f"  - {key}: {value:,}")
        else:
            print(f"  - {key}: {value}")
    print("=" * width)


def verify_database(conn: sqlite3.Connection) -> dict[str, Any]:
    """Verify the database contents and return statistics.

    Args:
        conn: Database connection

    Returns:
        Dictionary containing verification statistics
    """
    stats = {}

    # Get total card count
    stats["total_cards"] = get_card_count(conn)

    # Get cards per set
    sets_data = get_cards_by_set(conn)
    stats["total_sets"] = len(sets_data)

    # Get rarity distribution
    rarity_data = get_rarity_distribution(conn)

    print_section_header("DATABASE VERIFICATION")
    print(f"\nTotal cards in database: {stats['total_cards']:,}")

    if sets_data:
        print(f"\nCards per set ({len(sets_data)} sets):")
        for set_code, set_name, count in sets_data:
            print(f"  {set_code:6} {set_name[:30]:30} {count:4,} cards")

    print("\nRarity distribution:")
    for rarity, count in rarity_data:
        print(f"  {(rarity or 'None'):12} {count:5,} cards")

    return stats


def verify_price_data(conn: sqlite3.Connection) -> dict[str, Any]:
    """Verify price data in the database and return statistics.

    Args:
        conn: Database connection

    Returns:
        Dictionary containing price statistics
    """
    stats = {}

    # Get total price count
    query = "SELECT COUNT(*) FROM card_prices"
    stats["total_prices"] = execute_query(conn, query)[0][0]

    # Get price statistics
    query = """
        SELECT
            MIN(average_price) as min_price,
            MAX(average_price) as max_price,
            AVG(average_price) as avg_price,
            COUNT(DISTINCT uuid) as unique_cards
        FROM card_prices
    """
    price_stats = execute_query(conn, query)[0]
    stats["min_price"] = price_stats[0] or 0
    stats["max_price"] = price_stats[1] or 0
    stats["avg_price"] = price_stats[2] or 0
    stats["unique_cards_with_prices"] = price_stats[3]

    # Get sample of highest priced cards
    query = """
        SELECT c.name, c.set_code, cp.average_price
        FROM card_prices cp
        JOIN cards c ON cp.uuid = c.uuid
        ORDER BY cp.average_price DESC
        LIMIT 5
    """
    top_cards = execute_query(conn, query)

    # Get sample of lowest priced cards (excluding 0)
    query = """
        SELECT c.name, c.set_code, cp.average_price
        FROM card_prices cp
        JOIN cards c ON cp.uuid = c.uuid
        WHERE cp.average_price > 0
        ORDER BY cp.average_price ASC
        LIMIT 5
    """
    bottom_cards = execute_query(conn, query)

    print_section_header("PRICE DATA VERIFICATION")
    print(f"\nTotal price records: {stats['total_prices']:,}")
    print(f"Unique cards with prices: {stats['unique_cards_with_prices']:,}")
    print(f"Price range: ${stats['min_price']:.2f} - ${stats['max_price']:.2f}")
    print(f"Average price: ${stats['avg_price']:.2f}")

    if top_cards:
        print("\nTop 5 most expensive cards:")
        for name, set_code, price in top_cards:
            print(f"  ${price:8.2f} - {name} ({set_code})")

    if bottom_cards:
        print("\nBottom 5 least expensive cards (>$0):")
        for name, set_code, price in bottom_cards:
            print(f"  ${price:8.2f} - {name} ({set_code})")

    # Check for cards without prices
    query = """
        SELECT COUNT(*)
        FROM cards c
        WHERE NOT EXISTS (
            SELECT 1 FROM card_prices cp WHERE cp.uuid = c.uuid
        )
    """
    stats["cards_without_prices"] = execute_query(conn, query)[0][0]
    print(f"\nCards without price data: {stats['cards_without_prices']:,}")

    return stats


def print_processing_summary(
    new_cards: int, updated_cards: int = 0, skipped_cards: int = 0
) -> None:
    """Print a summary of card processing results.

    Args:
        new_cards: Number of new cards processed
        updated_cards: Number of cards updated
        skipped_cards: Number of cards skipped
    """
    print("\n✓ Processing complete:")
    print(f"  - New cards imported: {new_cards:,}")
    if updated_cards > 0:
        print(f"  - Cards updated: {updated_cards:,}")
    if skipped_cards > 0:
        print(f"  - Cards skipped: {skipped_cards:,}")


def export_csv_preview(results: list[tuple], limit: int = 10) -> None:
    """Print a preview of CSV export results.

    Args:
        results: List of result tuples (name, set_code, set_name, price)
        limit: Number of items to show in preview
    """
    if not results:
        print("❌ No data to preview!")
        return

    print(f"\nPreview (first {min(limit, len(results))} cards):")
    print("-" * 70)

    for i, (name, set_code, set_name, price) in enumerate(results[:limit], 1):
        price_dollars = price if price else 0
        print(f"{i:3}. {name[:30]:30} {set_code:6} ${price_dollars:.2f}")

    if len(results) > limit:
        print("...")
        # Show last entry
        last = results[-1]
        last_price_dollars = last[3] if last[3] else 0
        print(f"{len(results):3}. {last[0][:30]:30} {last[1]:6} ${last_price_dollars:.2f}")


def export_to_csv(
    results: list[tuple],
    output_path: Path,
    headers: list[str],
    round_prices: bool = True,
) -> None:
    """Export results to CSV file.

    Args:
        results: List of result tuples
        output_path: Path to output CSV file
        headers: CSV column headers
        round_prices: Whether to format prices with decimal cents (assumes price is last column)
    """
    if not results:
        logger.error("No data to export!")
        return

    logger.info(f"Writing to CSV: {output_path}")

    with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)

        for row in results:
            if round_prices and len(row) >= 4:  # Assume price is 4th column
                row = list(row)
                row[-1] = f"{row[-1]:.2f}" if row[-1] else "0.00"
            writer.writerow(row)

    logger.info(f"✓ Successfully exported {len(results):,} records to {output_path}")


def calculate_collection_stats(results: list[tuple]) -> dict[str, Any]:
    """Calculate statistics for a collection of cards.

    Args:
        results: List of (name, set_code, set_name, price) tuples

    Returns:
        Dictionary with collection statistics
    """
    if not results:
        return {}

    prices = [price if price else 0 for _, _, _, price in results]

    stats = {
        "total_cards": len(results),
        "total_value": sum(prices),
        "average_value": sum(prices) / len(prices) if prices else 0,
        "max_value": max(prices) if prices else 0,
        "min_value": min(p for p in prices if p > 0) if prices else 0,
    }

    return stats


def print_collection_summary(results: list[tuple], total_requested: int) -> None:
    """Print summary statistics for a card collection.

    Args:
        results: List of card results with prices
        total_requested: Total number of cards that were requested
    """
    if not results:
        logger.warning("No cards found with prices!")
        return

    stats = calculate_collection_stats(results)

    print("\nSummary:")
    print(f"  Total cards with prices: {stats['total_cards']:,}/{total_requested:,}")
    print(f"  Total value: ${stats['total_value']:.2f}")
    print(f"  Average value: ${stats['average_value']:.2f}")
    print(f"  Most expensive card: ${stats['max_value']:.2f}")
    if stats["min_value"] > 0:
        print(f"  Least expensive card: ${stats['min_value']:.2f}")
