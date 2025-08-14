# MTG Price Tracker

A streamlined tool for downloading Magic: The Gathering card data and tracking current prices. Get the complete MTG database with a single command and query card prices with powerful filtering options.

## ✨ Features

- **One-command setup** - Download and process everything with `mtg setup`
- **Automatic updates** - Keep prices current with `mtg update`  
- **Flexible filtering** - Query by sets, formats, or both
- **Fast queries** - Optimized SQLite database with current prices only
- **Complete database** - All MTG cards from all sets

## 🚀 Quick Start

### Installation

```bash
git clone https://github.com/BakerNet/mtg-scripts
cd mtg-scripts
uv sync                    # Install dependencies
source .venv/bin/activate  # Activate virtual environment
uv tool install .          # Install mtg command
```

### Usage

```bash
# Initial setup (downloads ~500MB, takes 2-3 minutes)
mtg setup

# Keep data current (run weekly/monthly)
mtg update

# Export expensive cards
mtg export-top 100
mtg export-top 50 --sets ZEN,WWK,ROE
mtg export-top 25 --formats Legacy,Modern

# Price your card lists
mtg export-list cube.txt
mtg export-list deck.txt --sets ZEN,WWK --formats Legacy
```

That's it! Two commands for data management, powerful queries for analysis.

## 📖 Commands

### Core Commands

| Command | Description |
|---------|-------------|
| `mtg setup` | Complete initial setup - downloads all cards and prices, creates database |
| `mtg setup --fresh` | Clean setup from scratch (removes existing data) |
| `mtg update` | Update database with latest cards and prices |

### Export Commands

| Command | Description |
|---------|-------------|
| `mtg export-top N` | Export top N most expensive cards |
| `mtg export-list FILE` | Price cards from a text/deck list |

### Filtering Options

Both export commands support filtering:

- `--sets SET1,SET2,SET3` - Filter by set codes (e.g., ZEN,WWK,ROE)
- `--formats FORMAT1,FORMAT2` - Filter by format legality (e.g., Legacy,Modern,Standard)

Filters can be combined:
```bash
mtg export-top 50 --sets ZEN,WWK --formats Legacy
```

## 📁 File Structure

```
data/
├── sets/
│   ├── gzipped/        # AllPrintings.json.gz (complete card database)
│   └── json/           # Extracted JSON
├── prices/
│   ├── gzipped/        # AllPrices.json.gz
│   └── json/           # Extracted price data
db/
└── cards.db            # SQLite database with all cards and current prices
```

## 🎯 Example Workflows

### Track Expensive Cards in Your Favorite Sets
```bash
mtg setup
mtg export-top 100 --sets NEO,DMU,BRO > modern_sets_expensive.csv
```

### Price a Cube List
```bash
mtg setup
mtg export-list my_vintage_cube.txt --formats Vintage
```

### Monitor Price Changes
```bash
# Initial setup
mtg setup
mtg export-top 1000 > prices_january.csv

# One month later
mtg update
mtg export-top 1000 > prices_february.csv
# Compare the CSVs to see price changes
```

### Find Budget Cards for a Format
```bash
mtg setup
# Export all Legacy-legal cards, then filter in spreadsheet for < $1
mtg export-top 10000 --formats Legacy > legacy_cards.csv
```

## 🔧 Configuration

### Environment Variables

```bash
MTG_BATCH_SIZE=5000       # Database batch size (default: 1000)
MTG_LOG_LEVEL=DEBUG       # Logging level (DEBUG, INFO, WARNING, ERROR)
MTG_LOG_FILE=mtg.log      # Log to file instead of console
MTG_DATA_DIR=./data       # Custom data directory
MTG_DB_DIR=./db           # Custom database directory
```

### Supported Deck List Formats

The `export-list` command supports multiple deck list formats:
- Plain text (one card per line)
- MTGO format (e.g., "4 Lightning Bolt")
- .dec format with sideboard support
- .mwDeck format (MagicWorkstation)
- .mtgsDeck format (MTG Salvation)

## 📊 Database Schema

The tool maintains a simple, efficient database:

- **cards** table - All MTG cards with complete details
  - UUID (primary key), name, set_code, mana_cost, type, rarity, etc.
  - Legalities stored as JSON for format filtering
  
- **card_prices** table - Current TCGPlayer prices only
  - UUID (primary key), average_price, last_updated
  - No historical data - keeps queries fast and storage minimal

## 🛠 Development

### Requirements
- Python 3.10+
- uv for dependency management

### Testing & Quality
```bash
pytest                     # Run tests
pytest -m "not integration"  # Skip integration tests
ruff format .              # Format code
ruff check .               # Lint code
```

### Project Structure
```
mtg_cli.py                 # Main CLI entry point
mtg_utils/                 # Core library modules
├── database.py            # Database operations
├── io_operations.py       # File I/O and downloads
├── card_processing.py     # Card data processing
├── sql.py                 # SQL queries and schemas
└── ...
tests/                     # Test suite
```

## 📝 Notes

- **Data source**: All card data from [MTGJSON](https://mtgjson.com)
- **Prices**: TCGPlayer average prices, updated daily by MTGJSON
- **Database size**: ~500MB after initial setup
- **Update frequency**: Run `mtg update` weekly or monthly for current prices
- **Performance**: Processes ~80,000 cards in under a minute

## 🤝 Contributing

Pull requests welcome! Please ensure:
- Tests pass (`pytest`)
- Code is formatted (`ruff format .`)
- Linting passes (`ruff check .`)

## 📄 License

MIT License - see LICENSE file for details

## 🙏 Credits

- Card data provided by [MTGJSON](https://mtgjson.com)
- Price data from TCGPlayer via MTGJSON