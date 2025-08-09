"""Constants and configuration values for MTG data processing.

Requires Python 3.10+
"""

from pathlib import Path

# Database paths
DEFAULT_DB_DIR = Path("db")
DEFAULT_DB_NAME = "cards.db"

# Default directories
DEFAULT_DATA_DIR = Path("data")
DEFAULT_SETS_DIR = DEFAULT_DATA_DIR / "sets"
DEFAULT_PRICES_DIR = DEFAULT_DATA_DIR / "prices"
DEFAULT_COLLECTIONS_DIR = DEFAULT_DATA_DIR / "collections"

# Subdirectories
GZIPPED_SUBDIR = "gzipped"
JSON_SUBDIR = "json"

# Batch processing
DEFAULT_BATCH_SIZE = 1000
PROGRESS_INTERVAL = 1000

# Database schema
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

# Index definitions
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

# Card field mappings (JSON key -> database column)
CARD_FIELD_MAPPING = {
    "uuid": "uuid",
    "name": "name",
    "number": "number",
    "manaCost": "mana_cost",
    "manaValue": "mana_value",
    "type": "type",
    "text": "text",
    "power": "power",
    "toughness": "toughness",
    "loyalty": "loyalty",
    "colors": "colors",
    "colorIdentity": "color_identity",
    "rarity": "rarity",
    "artist": "artist",
    "flavorText": "flavor_text",
    "convertedManaCost": "converted_mana_cost",
    "layout": "layout",
    "frameVersion": "frame_version",
    "borderColor": "border_color",
    "isReprint": "is_reprint",
    "printings": "printings",
    "types": "types",
    "subtypes": "subtypes",
    "supertypes": "supertypes",
    "keywords": "keywords",
    "legalities": "legalities",
    "edhrecRank": "edhrecRank",
    "edhrecSaltiness": "edhrecSaltiness",
}

# Fields that need JSON serialization
JSON_FIELDS = {
    "colors",
    "colorIdentity",
    "printings",
    "types",
    "subtypes",
    "supertypes",
    "keywords",
    "legalities",
}

# Export settings
DEFAULT_EXPORT_LIMIT = 100
CSV_HEADERS = ["Card Name", "Set Code", "Set Name", "Price"]
