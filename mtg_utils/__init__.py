"""MTG data processing utilities.

This package provides utilities for processing Magic: The Gathering card data
from MTGJSON format files, including database operations, file handling,
card processing, and reporting.

Requires Python 3.10+

Main modules:
- database: Database operations and connection management
- file_operations: File handling and compression utilities
- card_processing: Card data processing and validation
- reporting: Progress reporting and verification utilities
- config: Configuration management and environment setup
- constants: Shared constants and schemas
"""

__version__ = "1.0.0"
__author__ = "MTG Data Processing Team"

# Import main utilities for easy access
from .card_processing import prepare_card_data, process_price_batch, process_set_cards
from .config import get_config, get_db_path, setup_environment
from .database import (
    batch_insert_cards,
    create_database,
    create_price_table,
    ensure_column_exists,
    get_connection,
    get_existing_card_uuids,
)
from .exceptions import DatabaseError, FileOperationError, MTGProcessingError
from .file_operations import (
    get_project_paths,
    read_json_file,
    unzip_files,
    unzip_single_file,
)
from .performance import BatchProcessor, ConnectionPool, optimize_sqlite_connection
from .reporting import print_processing_summary, verify_database, verify_price_data

# Package-level constants
__all__ = [
    # Config
    "get_config",
    "setup_environment",
    "get_db_path",
    # Database
    "create_database",
    "get_connection",
    "create_price_table",
    "get_existing_card_uuids",
    "batch_insert_cards",
    "ensure_column_exists",
    # File operations
    "unzip_files",
    "unzip_single_file",
    "read_json_file",
    "get_project_paths",
    # Card processing
    "prepare_card_data",
    "process_set_cards",
    "process_price_batch",
    # Reporting
    "verify_database",
    "verify_price_data",
    "print_processing_summary",
    # Exceptions
    "MTGProcessingError",
    "DatabaseError",
    "FileOperationError",
    # Performance
    "ConnectionPool",
    "BatchProcessor",
    "optimize_sqlite_connection",
]
