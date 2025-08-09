"""Centralized SQL commands and schemas for MTG data processing.

This module contains all SQL commands, table schemas, and queries used
throughout the MTG processing system.

Requires Python 3.10+
"""

# =============================================================================
# TABLE SCHEMAS
# =============================================================================

CARDS_TABLE_SCHEMA = """
    CREATE TABLE cards (
        uuid TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        set_code TEXT NOT NULL,
        set_name TEXT,
        collection_name TEXT,
        number TEXT,
        mana_cost TEXT,
        mana_value REAL,
        type TEXT,
        text TEXT,
        power TEXT,
        toughness TEXT,
        loyalty TEXT,
        colors TEXT,  -- JSON array
        color_identity TEXT,  -- JSON array
        rarity TEXT,
        artist TEXT,
        flavor_text TEXT,
        converted_mana_cost REAL,
        layout TEXT,
        frame_version TEXT,
        border_color TEXT,
        is_reprint INTEGER,
        printings TEXT,  -- JSON array
        types TEXT,  -- JSON array
        subtypes TEXT,  -- JSON array
        supertypes TEXT,  -- JSON array
        keywords TEXT,  -- JSON array
        legalities TEXT,  -- JSON object
        edhrecRank INTEGER,
        edhrecSaltiness REAL
    )
"""

CARD_PRICES_TABLE_SCHEMA = """
    CREATE TABLE card_prices (
        uuid TEXT NOT NULL,
        average_price REAL,
        price_date DATE NOT NULL,
        PRIMARY KEY (uuid, price_date),
        FOREIGN KEY (uuid) REFERENCES cards(uuid)
    )
"""

# =============================================================================
# INDEX DEFINITIONS  
# =============================================================================

CARDS_INDEXES: list[tuple[str, str]] = [
    ("idx_name", "CREATE INDEX IF NOT EXISTS idx_name ON cards(name)"),
    ("idx_set_code", "CREATE INDEX IF NOT EXISTS idx_set_code ON cards(set_code)"),
    (
        "idx_collection",
        "CREATE INDEX IF NOT EXISTS idx_collection ON cards(collection_name)",
    ),
    ("idx_rarity", "CREATE INDEX IF NOT EXISTS idx_rarity ON cards(rarity)"),
    (
        "idx_mana_value",
        "CREATE INDEX IF NOT EXISTS idx_mana_value ON cards(mana_value)",
    ),
    ("idx_type", "CREATE INDEX IF NOT EXISTS idx_type ON cards(type)"),
]

PRICE_INDEXES: list[tuple[str, str]] = [
    (
        "idx_price_uuid",
        "CREATE INDEX IF NOT EXISTS idx_price_uuid ON card_prices(uuid)",
    ),
    (
        "idx_price_date",
        "CREATE INDEX IF NOT EXISTS idx_price_date ON card_prices(price_date)",
    ),
]

# =============================================================================
# UTILITY QUERIES
# =============================================================================

CHECK_TABLE_EXISTS = """
    SELECT name FROM sqlite_master
    WHERE type='table' AND name=?
"""

GET_TABLE_COLUMNS = "PRAGMA table_info({table})"

DROP_CARD_PRICES_TABLE = "DROP TABLE IF EXISTS card_prices"
DROP_CARDS_TABLE = "DROP TABLE IF EXISTS cards"

# =============================================================================
# CARD QUERIES
# =============================================================================

SELECT_ALL_CARD_UUIDS = "SELECT uuid FROM cards"

SELECT_CARD_BY_UUID = "SELECT uuid FROM cards WHERE uuid = ?"

GET_CARD_COUNT = "SELECT COUNT(*) FROM cards"

GET_CARDS_BY_SET = """
    SELECT set_code, set_name, COUNT(*) as card_count
    FROM cards
    GROUP BY set_code
    ORDER BY set_code
"""

GET_RARITY_DISTRIBUTION = """
    SELECT rarity, COUNT(*) as count
    FROM cards
    GROUP BY rarity
    ORDER BY count DESC
"""

# =============================================================================
# PRICE QUERIES
# =============================================================================

GET_PRICE_COUNT = "SELECT COUNT(*) FROM card_prices"

GET_PRICE_STATISTICS = """
    SELECT
        MIN(average_price) as min_price,
        MAX(average_price) as max_price,
        AVG(average_price) as avg_price,
        COUNT(DISTINCT uuid) as unique_cards
    FROM card_prices
"""

GET_TOP_PRICED_CARDS = """
    SELECT c.name, c.set_code, cp.average_price
    FROM card_prices cp
    JOIN cards c ON cp.uuid = c.uuid
    ORDER BY cp.average_price DESC
    LIMIT ?
"""

GET_BOTTOM_PRICED_CARDS = """
    SELECT c.name, c.set_code, cp.average_price
    FROM card_prices cp
    JOIN cards c ON cp.uuid = c.uuid
    WHERE cp.average_price > 0
    ORDER BY cp.average_price ASC
    LIMIT ?
"""

GET_CARDS_WITHOUT_PRICES = """
    SELECT COUNT(*)
    FROM cards c
    WHERE NOT EXISTS (
        SELECT 1 FROM card_prices cp WHERE cp.uuid = c.uuid
    )
"""

# =============================================================================
# EXPORT QUERIES
# =============================================================================

GET_TOP_CARDS_WITH_PRICES = """
    SELECT c.name, c.set_code, c.set_name, cp.average_price
    FROM cards c
    JOIN card_prices cp ON c.uuid = cp.uuid
    ORDER BY cp.average_price DESC
    LIMIT ?
"""

GET_CARDS_FROM_LIST = """
    SELECT c.name, c.set_code, c.set_name, COALESCE(cp.average_price, 0) as price
    FROM cards c
    JOIN temp_card_list tcl ON LOWER(TRIM(c.name)) = LOWER(TRIM(tcl.name))
    LEFT JOIN card_prices cp ON c.uuid = cp.uuid
    ORDER BY c.name, c.set_code
"""

# =============================================================================
# PERFORMANCE OPTIMIZATION PRAGMAS
# =============================================================================

PERFORMANCE_PRAGMAS = [
    "PRAGMA journal_mode=WAL",
    "PRAGMA synchronous=NORMAL", 
    "PRAGMA cache_size=10000",
    "PRAGMA temp_store=memory",
    "PRAGMA mmap_size=268435456",  # 256MB
]

TRANSACTION_PRAGMAS = [
    "PRAGMA journal_mode=WAL",
    "PRAGMA synchronous=NORMAL",
    "PRAGMA cache_size=10000", 
    "PRAGMA temp_store=memory",
]

# =============================================================================
# TRANSACTION MANAGEMENT
# =============================================================================

BEGIN_IMMEDIATE_TRANSACTION = "BEGIN IMMEDIATE"
BEGIN_TRANSACTION = "BEGIN"
COMMIT_TRANSACTION = "COMMIT"
ROLLBACK_TRANSACTION = "ROLLBACK"

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_insert_cards_query(num_columns: int) -> str:
    """Generate INSERT OR REPLACE query for cards table.
    
    Args:
        num_columns: Number of columns in the cards table
        
    Returns:
        SQL INSERT OR REPLACE query with appropriate placeholders
    """
    placeholders = ",".join(["?" for _ in range(num_columns)])
    return f"INSERT OR REPLACE INTO cards VALUES ({placeholders})"


INSERT_PRICE_QUERY = "INSERT OR REPLACE INTO card_prices (uuid, average_price, price_date) VALUES (?, ?, ?)"


def get_batch_insert_prices_query(batch_size: int) -> str:
    """Generate batch INSERT query for card_prices.
    
    Args:
        batch_size: Number of records to insert
        
    Returns:
        SQL query for batch price insertion
    """
    values_clause = ",".join(["(?, ?, ?)"] * batch_size)
    return f"INSERT OR REPLACE INTO card_prices (uuid, average_price, price_date) VALUES {values_clause}"


def create_temp_table_query(table_name: str, column_definition: str = "name TEXT") -> str:
    """Generate CREATE TEMP TABLE query.
    
    Args:
        table_name: Name of temporary table
        column_definition: Column definition (default: "name TEXT")
        
    Returns:
        SQL CREATE TEMP TABLE query
    """
    return f"CREATE TEMP TABLE {table_name} ({column_definition})"


def get_add_column_query(table: str, column: str, column_type: str) -> str:
    """Generate ALTER TABLE ADD COLUMN query.
    
    Args:
        table: Table name
        column: Column name
        column_type: Column data type
        
    Returns:
        SQL ALTER TABLE query
    """
    return f"ALTER TABLE {table} ADD COLUMN {column} {column_type}"