"""File handling utilities for MTG data processing.

Requires Python 3.10+
"""

import gzip
import json
import logging
from pathlib import Path
from typing import Any

from .constants import GZIPPED_SUBDIR, JSON_SUBDIR

logger = logging.getLogger(__name__)


def create_directories(*paths: Path) -> None:
    """Create directories if they don't exist.

    Args:
        *paths: Variable number of Path objects to create
    """
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)
        logger.info(f"✓ Created/verified directory: {path}")


def unzip_files(
    source_dir: Path, dest_dir: Path, pattern: str = "*.json.gz"
) -> list[Path]:
    """Unzip all matching files from source to destination directory.

    Args:
        source_dir: Directory containing gzipped files
        dest_dir: Directory to extract files to
        pattern: Glob pattern for files to unzip (default: *.json.gz)

    Returns:
        List of paths to unzipped files

    Raises:
        FileNotFoundError: If source directory doesn't exist
    """
    if not source_dir.exists():
        raise FileNotFoundError(f"Source directory not found: {source_dir}")

    create_directories(dest_dir)

    unzipped_files = []
    gz_files = sorted(source_dir.glob(pattern))

    if not gz_files:
        logger.warning(f"No files matching pattern '{pattern}' found in {source_dir}")
        return unzipped_files

    logger.info(f"Found {len(gz_files)} gzipped files to process")

    for gz_file in gz_files:
        json_filename = gz_file.stem  # Removes .gz extension
        json_path = dest_dir / json_filename

        logger.debug(f"Unzipping {gz_file.name}...")

        try:
            with gzip.open(gz_file, "rb") as gz_in:
                with open(json_path, "wb") as json_out:
                    json_out.write(gz_in.read())

            unzipped_files.append(json_path)
            logger.debug(f"✓ Unzipped {gz_file.name}")
        except Exception as e:
            logger.error(f"Failed to unzip {gz_file.name}: {e}")
            continue

    logger.info(f"✓ Successfully unzipped {len(unzipped_files)} files")
    return unzipped_files


def unzip_single_file(source_file: Path, dest_dir: Path) -> Path:
    """Unzip a single file.

    Args:
        source_file: Path to the gzipped file
        dest_dir: Directory to extract file to

    Returns:
        Path to the unzipped file

    Raises:
        FileNotFoundError: If source file doesn't exist
    """
    if not source_file.exists():
        raise FileNotFoundError(f"Source file not found: {source_file}")

    create_directories(dest_dir)

    json_filename = source_file.stem  # Removes .gz extension
    json_path = dest_dir / json_filename

    logger.info(f"Unzipping {source_file.name}...")

    with gzip.open(source_file, "rb") as gz_in:
        with open(json_path, "wb") as json_out:
            json_out.write(gz_in.read())

    logger.info(f"✓ Successfully unzipped to: {json_path}")
    return json_path


def read_json_file(file_path: Path) -> dict[str, Any]:
    """Read and parse a JSON file.

    Args:
        file_path: Path to the JSON file

    Returns:
        Parsed JSON data as a dictionary

    Raises:
        FileNotFoundError: If file doesn't exist
        json.JSONDecodeError: If file contains invalid JSON
    """
    if not file_path.exists():
        raise FileNotFoundError(f"JSON file not found: {file_path}")

    logger.debug(f"Reading JSON file: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    return data


def write_json_file(data: dict[str, Any], file_path: Path, indent: int = 2) -> None:
    """Write data to a JSON file.

    Args:
        data: Data to write to JSON
        file_path: Path to write the file to
        indent: Indentation level for pretty printing (default: 2)
    """
    file_path.parent.mkdir(parents=True, exist_ok=True)

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=indent, ensure_ascii=False)

    logger.debug(f"✓ Wrote JSON file: {file_path}")


def read_card_list(file_path: Path) -> list[str]:
    """Read card names from a text file, one per line.

    Args:
        file_path: Path to the text file

    Returns:
        List of card names (stripped of whitespace)

    Raises:
        FileNotFoundError: If file doesn't exist
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Card list file not found: {file_path}")

    card_names = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:  # Skip empty lines
                card_names.append(line)

    logger.info(f"✓ Read {len(card_names)} card names from {file_path}")
    return card_names


def get_project_paths(base_type: str = "sets") -> dict[str, Path]:
    """Get standard project paths for a given base type.

    Args:
        base_type: Type of paths to get ("sets", "prices", or "collections")

    Returns:
        Dictionary with paths for gzipped, json, and db directories
    """
    from .constants import (
        DEFAULT_COLLECTIONS_DIR,
        DEFAULT_DB_DIR,
        DEFAULT_DB_NAME,
        DEFAULT_PRICES_DIR,
        DEFAULT_SETS_DIR,
    )

    base_dirs = {
        "sets": DEFAULT_SETS_DIR,
        "prices": DEFAULT_PRICES_DIR,
        "collections": DEFAULT_COLLECTIONS_DIR,
    }

    if base_type not in base_dirs:
        raise ValueError(
            f"Invalid base_type: {base_type}. Must be one of {list(base_dirs.keys())}"
        )

    base_dir = base_dirs[base_type]

    return {
        "base": base_dir,
        "gzipped": base_dir / GZIPPED_SUBDIR,
        "json": base_dir / JSON_SUBDIR,
        "db": DEFAULT_DB_DIR / DEFAULT_DB_NAME,
    }


def ensure_source_files_exist(
    source_dir: Path, pattern: str = "*.json.gz"
) -> list[Path]:
    """Check that source files exist and return their paths.

    Args:
        source_dir: Directory to check for files
        pattern: Glob pattern for files

    Returns:
        List of matching file paths

    Raises:
        FileNotFoundError: If no matching files found
    """
    if not source_dir.exists():
        raise FileNotFoundError(f"Source directory not found: {source_dir}")

    files = sorted(source_dir.glob(pattern))

    if not files:
        raise FileNotFoundError(f"No files matching '{pattern}' found in {source_dir}")

    return files
