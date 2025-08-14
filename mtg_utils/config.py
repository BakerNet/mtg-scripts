"""Configuration management for MTG data processing.

Requires Python 3.10+
"""

import logging
import os
from pathlib import Path
from typing import Any

from .constants import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_COLLECTIONS_DIR,
    DEFAULT_DATA_DIR,
    DEFAULT_DB_DIR,
    DEFAULT_DB_NAME,
    DEFAULT_LOG_FORMAT,
    DEFAULT_LOG_LEVEL,
    DEFAULT_PRICES_DIR,
    DEFAULT_SETS_DIR,
    PROGRESS_INTERVAL,
    VALID_LOG_LEVELS,
)

logger = logging.getLogger(__name__)


class MTGConfig:
    """Configuration manager for MTG data processing."""

    def __init__(self):
        """Initialize configuration with defaults and environment overrides."""
        # Database configuration
        self.db_dir = Path(os.getenv("MTG_DB_DIR", DEFAULT_DB_DIR))
        self.db_name = os.getenv("MTG_DB_NAME", DEFAULT_DB_NAME)
        self.db_path = self.db_dir / self.db_name

        # Data directories
        self.data_dir = Path(os.getenv("MTG_DATA_DIR", DEFAULT_DATA_DIR))
        self.sets_dir = Path(os.getenv("MTG_SETS_DIR", DEFAULT_SETS_DIR))
        self.prices_dir = Path(os.getenv("MTG_PRICES_DIR", DEFAULT_PRICES_DIR))
        self.collections_dir = Path(
            os.getenv("MTG_COLLECTIONS_DIR", DEFAULT_COLLECTIONS_DIR)
        )

        # Processing configuration
        self.batch_size = int(os.getenv("MTG_BATCH_SIZE", DEFAULT_BATCH_SIZE))
        self.progress_interval = int(
            os.getenv("MTG_PROGRESS_INTERVAL", PROGRESS_INTERVAL)
        )

        # Logging configuration
        self.log_level = os.getenv("MTG_LOG_LEVEL", DEFAULT_LOG_LEVEL).upper()
        self.log_file = os.getenv("MTG_LOG_FILE")

        # Validation
        self._validate_config()

    def _validate_config(self) -> None:
        """Validate configuration values."""
        if self.batch_size <= 0:
            raise ValueError("Batch size must be positive")

        if self.progress_interval <= 0:
            raise ValueError("Progress interval must be positive")

        if self.log_level not in VALID_LOG_LEVELS:
            raise ValueError(f"Invalid log level: {self.log_level}")

    def setup_logging(self) -> None:
        """Set up logging based on configuration."""
        # Configure basic logging
        logging.basicConfig(
            level=getattr(logging, self.log_level), format=DEFAULT_LOG_FORMAT
        )

        # Add file handler if specified
        if self.log_file:
            log_path = Path(self.log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)

            file_handler = logging.FileHandler(log_path)
            file_handler.setLevel(getattr(logging, self.log_level))
            file_handler.setFormatter(logging.Formatter(DEFAULT_LOG_FORMAT))

            # Add to root logger
            logging.getLogger().addHandler(file_handler)
            logger.info(f"Logging to file: {log_path}")

    def ensure_directories(self) -> None:
        """Ensure all configured directories exist."""
        directories = [
            self.db_dir,
            self.data_dir,
            self.sets_dir,
            self.prices_dir,
            self.collections_dir,
        ]

        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Ensured directory exists: {directory}")

    def get_paths(self, base_type: str = "sets") -> dict[str, Path]:
        """Get paths for a specific data type.

        Args:
            base_type: Type of paths ('sets', 'prices', or 'collections')

        Returns:
            Dictionary with relevant paths
        """
        base_dirs = {
            "sets": self.sets_dir,
            "prices": self.prices_dir,
            "collections": self.collections_dir,
        }

        if base_type not in base_dirs:
            raise ValueError(f"Invalid base_type: {base_type}")

        base_dir = base_dirs[base_type]

        return {
            "base": base_dir,
            "gzipped": base_dir / "gzipped",
            "json": base_dir / "json",
            "db": self.db_path,
        }

    def to_dict(self) -> dict[str, Any]:
        """Convert configuration to dictionary for display."""
        return {
            "Database Path": str(self.db_path),
            "Data Directory": str(self.data_dir),
            "Sets Directory": str(self.sets_dir),
            "Prices Directory": str(self.prices_dir),
            "Collections Directory": str(self.collections_dir),
            "Batch Size": self.batch_size,
            "Progress Interval": self.progress_interval,
            "Log Level": self.log_level,
            "Log File": self.log_file or "Console only",
        }

    def print_config(self) -> None:
        """Print current configuration."""
        print("\nCurrent Configuration:")
        print("-" * 40)
        for key, value in self.to_dict().items():
            print(f"  {key:<20}: {value}")
        print("-" * 40)


# Global configuration instance
config = MTGConfig()


def get_config() -> MTGConfig:
    """Get the global configuration instance.

    Returns:
        MTGConfig instance
    """
    return config


def setup_environment(
    log_level: str | None = None, log_file: str | None = None
) -> None:
    """Set up the environment for MTG processing.

    Args:
        log_level: Optional log level override
        log_file: Optional log file path
    """
    if log_level:
        config.log_level = log_level.upper()

    if log_file:
        config.log_file = log_file

    # Validate after potential changes
    config._validate_config()

    # Set up logging
    config.setup_logging()

    # Ensure directories exist
    config.ensure_directories()

    logger.info("MTG processing environment initialized")
    config.print_config()


def get_db_path() -> Path:
    """Get the configured database path.

    Returns:
        Path to the database file
    """
    return config.db_path


def get_batch_size() -> int:
    """Get the configured batch size.

    Returns:
        Batch size for processing
    """
    return config.batch_size


def get_progress_interval() -> int:
    """Get the configured progress interval.

    Returns:
        Progress reporting interval
    """
    return config.progress_interval
