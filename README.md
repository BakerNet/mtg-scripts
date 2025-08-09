# MTG Data Processing Scripts

Python scripts to download and process Magic: The Gathering card data from MTGJSON.

## Setup

### Requirements

- Python 3.10+
- Dependencies: `tqdm`, `urllib3` (for downloads)

```bash
git clone https://github.com/BakerNet/mtg-scripts
cd mtg-scripts
uv sync                           # Create virtual env and download deps
source .venv/bin/activate         # Activate virtual env
uv tool install .                 # Install mtg command to virtual env from source
```

## Quick Start

```bash
# 1. Download data from MTGJSON
mtg download-sets ZEN WWK ROE     # Download specific sets
mtg download-collection Legacy    # OR download format collection
mtg download-prices               # Download price data

# 2. Process the data
mtg process-cards                 # Process card data first
mtg process-collections           # OR process format collection first
mtg process-prices                # Then process prices

# 3. Export results
mtg export-top 100               # Top 100 expensive cards
mtg export-list my_cube.txt      # Price a specific list of cards
```

## Available Commands

### Download Commands
- `mtg download-sets SET1 SET2 ...` - Download individual sets by code
- `mtg download-collection FORMAT` - Download collection (Legacy, Modern, Vintage, etc.)
- `mtg download-prices` - Download AllPrices.json.gz

### Processing Commands
- `mtg process-cards` - Process card data from sets
- `mtg process-collections` - Process collection files 
- `mtg process-prices` - Process price data

### Export Commands
- `mtg export-top N` - Export top N expensive cards
- `mtg export-list FILE` - Price cards from text list

## File Structure

```
data/
├── sets/
│   ├── gzipped/        # Downloaded .json.gz set files
│   └── json/           # Extracted .json files
├── collections/
│   ├── gzipped/        # Downloaded collection files (Legacy, Modern, etc.)
│   └── json/           # Extracted collection files
└── prices/
    ├── gzipped/        # AllPrices.json.gz
    └── json/           # Extracted price data
db/
└── cards.db           # SQLite database (auto-created)
```

## Command Options

```bash
# Clear existing data before download
mtg download-sets ZEN WWK --fresh
mtg download-collection Legacy --fresh
mtg download-prices --fresh

# Start fresh processing (deletes existing database data)
mtg process-cards --fresh
mtg process-collections --fresh

# Debug mode
mtg --log-level DEBUG process-cards

# Custom export size
mtg export-top 500

# Custom output file
mtg export-list cube.txt --output my_cube_prices.csv
```

## Available Collections

- Legacy, Modern, Vintage, Standard
- Pioneer, Commander, Historic
- Alchemy, Explorer

## Environment Variables

```bash
# Processing
export MTG_BATCH_SIZE=2000          # Batch size for processing
export MTG_LOG_LEVEL=DEBUG          # Logging level
export MTG_LOG_FILE=mtg.log         # Log to file instead of console

# Custom directories (optional)
export MTG_DATA_DIR=./custom_data   # Change data directory
export MTG_DB_DIR=./custom_db       # Change database directory
```

## Examples

```bash
# Download and process Vintage format
mtg download-collection Vintage --fresh
mtg process-collections

# Get price data and export expensive cards
mtg download-prices --fresh
mtg process-prices
mtg export-top 200

# Price your Vintage cube
mtg export-list my_cube.txt
```

## Development

```bash
pytest          # Run tests
black .         # Format code
ruff check .    # Lint
```

## Notes

- Always run card/collection processing before price processing
- The `--fresh` flag on processing commands only clears database data, not files
- Downloaded files are cached - use download `--fresh` to re-download
