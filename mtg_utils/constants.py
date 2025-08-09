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

# Configuration defaults
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
VALID_LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


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
