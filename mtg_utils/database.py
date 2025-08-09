"""Database operations for MTG data processing.

Requires Python 3.10+
"""

import logging
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from .constants import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_DB_DIR,
    DEFAULT_DB_NAME,
)
from .sql import (
    CARD_PRICES_TABLE_SCHEMA,
    CARDS_INDEXES,
    CARDS_TABLE_SCHEMA,
    CHECK_TABLE_EXISTS,
    DROP_CARDS_TABLE,
    DROP_CARD_PRICES_TABLE,
    GET_TABLE_COLUMNS,
    PRICE_INDEXES,
    SELECT_ALL_CARD_UUIDS,
    SELECT_CARD_BY_UUID,
    GET_CARD_COUNT,
    GET_CARDS_BY_SET,
    GET_RARITY_DISTRIBUTION,
    get_insert_cards_query,
    get_add_column_query,
    create_temp_table_query,
)

logger = logging.getLogger(__name__)


@contextmanager
def get_connection(db_path: Path | None = None):
    """Get a database connection as a context manager.

    Args:
        db_path: Path to database file (uses default if None)

    Yields:
        sqlite3.Connection object
    """
    if db_path is None:
        db_path = DEFAULT_DB_DIR / DEFAULT_DB_NAME

    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        yield conn
    finally:
        conn.close()


def create_database(
    db_path: Path | None = None, fresh_start: bool = False
) -> sqlite3.Connection:
    """Create or open SQLite database and ensure tables exist.

    Args:
        db_path: Path to the database file (uses default if None)
        fresh_start: If True, drop existing tables and start fresh

    Returns:
        sqlite3.Connection object
    """
    if db_path is None:
        db_path = DEFAULT_DB_DIR / DEFAULT_DB_NAME

    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)

    if fresh_start:
        drop_all_tables(conn)

    # Create cards table if it doesn't exist
    if not table_exists(conn, "cards"):
        create_cards_table(conn)
    else:
        logger.info(f"✓ Using existing database: {db_path}")

    # Always ensure indexes exist
    create_indexes(conn, CARDS_INDEXES)

    return conn


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """Check if a table exists in the database.

    Args:
        conn: Database connection
        table_name: Name of the table to check

    Returns:
        True if table exists, False otherwise
    """
    cursor = conn.cursor()
    cursor.execute(CHECK_TABLE_EXISTS, (table_name,))
    return cursor.fetchone() is not None


def create_cards_table(conn: sqlite3.Connection) -> None:
    """Create the cards table.

    Args:
        conn: Database connection
    """
    cursor = conn.cursor()
    cursor.execute(CARDS_TABLE_SCHEMA)
    conn.commit()
    logger.info("✓ Created cards table")


def create_price_table(conn: sqlite3.Connection) -> None:
    """Create the card_prices table.

    Args:
        conn: Database connection
    """
    cursor = conn.cursor()

    if not table_exists(conn, "card_prices"):
        cursor.execute(CARD_PRICES_TABLE_SCHEMA)
        conn.commit()
        logger.info("✓ Created card_prices table")
    else:
        logger.info("✓ Using existing card_prices table")

    # Create indexes
    create_indexes(conn, PRICE_INDEXES)


def create_indexes(conn: sqlite3.Connection, indexes: list[tuple[str, str]]) -> None:
    """Create indexes for a table.

    Args:
        conn: Database connection
        indexes: List of (index_name, create_statement) tuples
    """
    cursor = conn.cursor()
    for index_name, create_statement in indexes:
        cursor.execute(create_statement)
    conn.commit()
    logger.debug(f"✓ Created/verified {len(indexes)} indexes")


def drop_all_tables(conn: sqlite3.Connection) -> None:
    """Drop all MTG-related tables from the database.

    Args:
        conn: Database connection
    """
    cursor = conn.cursor()
    # Drop in order due to foreign key constraints
    cursor.execute(DROP_CARD_PRICES_TABLE)
    cursor.execute(DROP_CARDS_TABLE)
    conn.commit()
    logger.info("✓ Dropped existing tables for fresh start")


def ensure_column_exists(
    conn: sqlite3.Connection, table: str, column: str, column_type: str = "TEXT"
) -> bool:
    """Ensure a column exists in a table, adding it if necessary.

    Args:
        conn: Database connection
        table: Table name
        column: Column name
        column_type: SQL type for the column

    Returns:
        True if column was added, False if it already existed
    """
    cursor = conn.cursor()
    cursor.execute(GET_TABLE_COLUMNS.format(table=table))
    columns = [column_info[1] for column_info in cursor.fetchall()]

    if column not in columns:
        cursor.execute(get_add_column_query(table, column, column_type))
        conn.commit()
        logger.info(f"✓ Added {column} column to {table} table")
        return True
    return False


def batch_insert_cards(
    conn: sqlite3.Connection,
    cards_data: list[tuple],
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> tuple[int, int, int]:
    """Insert cards in batches for better performance.

    Args:
        conn: Database connection
        cards_data: List of card data tuples ready for insertion
        batch_size: Number of records to insert per batch

    Returns:
        Tuple of (new_cards, updated_cards, skipped_cards)
    """
    cursor = conn.cursor()
    new_cards = 0
    updated_cards = 0
    skipped_cards = 0

    # Get number of columns in cards table
    cursor.execute(GET_TABLE_COLUMNS.format(table="cards"))
    num_columns = len(cursor.fetchall())
    insert_query = get_insert_cards_query(num_columns)

    for i in range(0, len(cards_data), batch_size):
        batch = cards_data[i : i + batch_size]

        for card_data in batch:
            uuid = card_data[0]  # UUID is always first

            # Check if card exists
            cursor.execute(SELECT_CARD_BY_UUID, (uuid,))
            existing = cursor.fetchone()

            try:
                cursor.execute(insert_query, card_data)

                if existing:
                    updated_cards += 1
                else:
                    new_cards += 1
            except sqlite3.Error as e:
                logger.error(f"Error inserting card: {e}")
                skipped_cards += 1

        conn.commit()

        if (i + batch_size) % 1000 == 0:
            logger.debug(f"Processed {i + batch_size} cards...")

    return new_cards, updated_cards, skipped_cards


def get_existing_card_uuids(conn: sqlite3.Connection) -> set[str]:
    """Get all UUIDs from the cards table.

    Args:
        conn: Database connection

    Returns:
        Set of card UUIDs
    """
    cursor = conn.cursor()
    cursor.execute(SELECT_ALL_CARD_UUIDS)
    return {row[0] for row in cursor.fetchall()}


def get_card_count(conn: sqlite3.Connection) -> int:
    """Get total number of cards in database.

    Args:
        conn: Database connection

    Returns:
        Number of cards
    """
    cursor = conn.cursor()
    cursor.execute(GET_CARD_COUNT)
    return cursor.fetchone()[0]


def get_cards_by_set(conn: sqlite3.Connection) -> list[tuple[str, str, int]]:
    """Get card counts grouped by set.

    Args:
        conn: Database connection

    Returns:
        List of (set_code, set_name, count) tuples
    """
    cursor = conn.cursor()
    cursor.execute(GET_CARDS_BY_SET)
    return cursor.fetchall()


def get_rarity_distribution(conn: sqlite3.Connection) -> list[tuple[str, int]]:
    """Get card counts grouped by rarity.

    Args:
        conn: Database connection

    Returns:
        List of (rarity, count) tuples
    """
    cursor = conn.cursor()
    cursor.execute(GET_RARITY_DISTRIBUTION)
    return cursor.fetchall()


def execute_query(
    conn: sqlite3.Connection, query: str, params: tuple | None = None
) -> list[tuple]:
    """Execute a query and return results.

    Args:
        conn: Database connection
        query: SQL query to execute
        params: Optional parameters for the query

    Returns:
        Query results as list of tuples
    """
    cursor = conn.cursor()
    if params:
        cursor.execute(query, params)
    else:
        cursor.execute(query)
    return cursor.fetchall()


def create_temp_table_from_list(
    conn: sqlite3.Connection, table_name: str, values: list[str]
) -> None:
    """Create a temporary table from a list of values.

    Args:
        conn: Database connection
        table_name: Name for the temporary table
        values: List of values to insert
    """
    cursor = conn.cursor()
    cursor.execute(create_temp_table_query(table_name))
    cursor.executemany(
        f"INSERT INTO {table_name} VALUES (?)", [(value,) for value in values]
    )
    conn.commit()
