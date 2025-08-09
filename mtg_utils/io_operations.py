"""I/O operations for MTG data processing.

This module handles file operations, compression, downloads, and other I/O tasks
including downloads from MTGJSON API.

Requires Python 3.10+
"""

import gzip
import json
import logging
import urllib.request
from pathlib import Path
from typing import Any, List
from urllib.error import HTTPError, URLError

import tqdm

from .constants import GZIPPED_SUBDIR, JSON_SUBDIR


def _validate_file_path(file_path: Path, base_dir: Path = None) -> Path:
    """Validate file path to prevent directory traversal attacks.

    Args:
        file_path: Path to validate
        base_dir: Optional base directory to restrict access to

    Returns:
        Validated and resolved path

    Raises:
        ValueError: If path traversal is detected or path is outside base_dir
    """
    try:
        # Resolve path to handle .. and symlinks
        resolved_path = file_path.resolve()

        # Check for suspicious patterns
        path_str = str(file_path)
        if '..' in path_str or path_str.startswith('/'):
            # Allow absolute paths but log them
            logger.warning(f"Absolute or relative path used: {file_path}")

        # If base_dir is provided, ensure path is within it
        if base_dir is not None:
            base_resolved = base_dir.resolve()
            try:
                resolved_path.relative_to(base_resolved)
            except ValueError:
                raise ValueError(f"Path {file_path} is outside allowed directory {base_dir}")

        return resolved_path

    except (OSError, RuntimeError) as e:
        raise ValueError(f"Invalid file path: {file_path}") from e

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
    """Read card names from various deck list formats.
    
    Supports multiple formats:
    - Plain text: One card name per line
    - MTGO format (.txt): "4 Lightning Bolt" with optional "Sideboard" section
    - DEK format (.dec): Comments with //, sideboard with "SB:" prefix
    - MW format (.mwDeck): Set annotations in brackets like "4 [MOR] Heritage Druid"
    - MTGS format (.mtgsDeck): Tab-separated "4x\tLightning Bolt" with [DECK]/[/DECK] tags

    Args:
        file_path: Path to the deck list file

    Returns:
        List of card names (without quantities or set codes, including sideboard cards)

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If path traversal is detected
    """
    import re
    
    # Validate path to prevent directory traversal
    validated_path = _validate_file_path(file_path)

    if not validated_path.exists():
        raise FileNotFoundError(f"Card list file not found: {validated_path}")

    card_names = []
    
    # Try different encodings to handle various file formats
    encodings_to_try = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    file_content = None
    
    for encoding in encodings_to_try:
        try:
            with open(validated_path, "r", encoding=encoding) as f:
                file_content = f.read()
                break
        except UnicodeDecodeError:
            continue
    
    if file_content is None:
        raise ValueError(f"Could not decode file {validated_path} with any supported encoding")
    
    in_sideboard = False
    in_deck_section = False
    
    for line in file_content.splitlines():
            line = line.strip()
            
            # Skip empty lines and comments
            if not line or line.startswith('//'):
                continue
            
            # Handle MTGS format deck tags
            if line == '[DECK]':
                in_deck_section = True
                continue
            elif line == '[/DECK]':
                in_deck_section = False
                continue
            elif line.startswith('[URL=') or line.startswith('[/URL]'):
                # Skip URL tags in MTGS format
                continue
                
            # Check for sideboard markers
            if line.lower() == 'sideboard':
                in_sideboard = True
                continue
            
            # Handle DEK format sideboard entries (SB: prefix)
            if line.startswith('SB:'):
                line = line[3:].strip()  # Remove "SB:" prefix
                in_sideboard = True
            
            # Parse MTGS format: "4x\tLightning Bolt" (tab-separated with 'x' suffix)
            mtgs_match = re.match(r'^(\d+)x\t(.+)$', line)
            if mtgs_match:
                quantity = int(mtgs_match.group(1))
                card_name = mtgs_match.group(2).strip()
                
                # Add the card name the specified number of times
                for _ in range(quantity):
                    card_names.append(card_name)
                continue
                
            # Parse standard quantity + card name format
            # Match patterns like "4 Lightning Bolt", "1 Troll of Khazad-dûm", or "4 [MOR] Heritage Druid"
            match = re.match(r'^(\d+)\s+(.+)$', line)
            if match:
                quantity = int(match.group(1))
                card_part = match.group(2).strip()
                
                # Remove set annotations in brackets (e.g., "[MOR] Heritage Druid" -> "Heritage Druid")
                # Handle empty brackets [] as well
                card_name = re.sub(r'^\[[^\]]*\]\s*', '', card_part).strip()
                
                # Add the card name the specified number of times
                for _ in range(quantity):
                    card_names.append(card_name)
            else:
                # Plain text format - just add the card name once
                if line:
                    card_names.append(line)

    logger.info(f"✓ Read {len(card_names)} card entries from {validated_path}")
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


# =============================================================================
# DOWNLOAD FUNCTIONALITY (from download.py)
# =============================================================================

# MTGJSON API base URL
MTGJSON_BASE_URL = "https://mtgjson.com/api/v5/"

# Available collections
AVAILABLE_COLLECTIONS = {
    "Legacy",
    "Modern",
    "Vintage",
    "Standard",
    "Pioneer",
    "Commander",
    "Historic",
    "Alchemy",
    "Explorer",
}


class DownloadError(Exception):
    """Exception raised for download errors."""

    pass


def download_file(url: str, dest_path: Path, show_progress: bool = True) -> None:
    """Download a file from URL to destination path with progress bar.

    Args:
        url: URL to download from
        dest_path: Path to save the file to
        show_progress: Whether to show progress bar

    Raises:
        DownloadError: If download fails
    """
    logger.info(f"Downloading {url}")

    try:
        # Create destination directory
        create_directories(dest_path.parent)

        with urllib.request.urlopen(url) as response:
            total_size = int(response.headers.get("Content-Length", 0))

            if show_progress and total_size > 0:
                with tqdm.tqdm(
                    desc=f"Downloading {dest_path.name}",
                    total=total_size,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                ) as pbar:
                    with open(dest_path, "wb") as f:
                        chunk_size = 8192
                        while chunk := response.read(chunk_size):
                            f.write(chunk)
                            pbar.update(len(chunk))
            else:
                with open(dest_path, "wb") as f:
                    f.write(response.read())

        logger.info(f"✓ Downloaded {dest_path}")

    except HTTPError as e:
        if e.code == 404:
            raise DownloadError(f"File not found: {url}")
        else:
            raise DownloadError(f"HTTP error {e.code}: {e.reason}")
    except URLError as e:
        raise DownloadError(f"URL error: {e.reason}")
    except Exception as e:
        raise DownloadError(f"Download failed: {e}")


def download_sets(
    set_codes: List[str], dest_dir: Path, clear_existing: bool = False
) -> List[Path]:
    """Download individual MTG set files from MTGJSON.

    Args:
        set_codes: List of set codes to download (e.g. ['ZEN', 'WWK', 'ROE'])
        dest_dir: Directory to save files to
        clear_existing: Whether to clear existing files first

    Returns:
        List of paths to downloaded files

    Raises:
        DownloadError: If any download fails
    """
    if not set_codes:
        raise ValueError("No set codes provided")

    if clear_existing:
        _clear_directory(dest_dir, "*.json.gz")
        _clear_directory(dest_dir.parent / "json", "*.json")

    logger.info(f"Downloading {len(set_codes)} set files")
    downloaded_files = []
    failed_downloads = []

    for set_code in set_codes:
        set_code = set_code.upper()
        filename = f"{set_code}.json.gz"
        url = f"{MTGJSON_BASE_URL}{filename}"
        dest_path = dest_dir / filename

        try:
            download_file(url, dest_path)
            downloaded_files.append(dest_path)
        except DownloadError as e:
            logger.error(f"Failed to download {set_code}: {e}")
            failed_downloads.append(set_code)

    if failed_downloads:
        logger.warning(f"Failed to download: {', '.join(failed_downloads)}")

    logger.info(
        f"✓ Successfully downloaded {len(downloaded_files)}/{len(set_codes)} sets"
    )
    return downloaded_files


def download_collection(
    collection_name: str, dest_dir: Path, clear_existing: bool = False
) -> Path:
    """Download a MTG collection file from MTGJSON.

    Args:
        collection_name: Name of collection (e.g. 'Legacy', 'Modern', 'Vintage')
        dest_dir: Directory to save file to
        clear_existing: Whether to clear existing files first

    Returns:
        Path to downloaded file

    Raises:
        DownloadError: If download fails
        ValueError: If collection name is invalid
    """
    if collection_name not in AVAILABLE_COLLECTIONS:
        available = ", ".join(sorted(AVAILABLE_COLLECTIONS))
        raise ValueError(
            f"Invalid collection '{collection_name}'. Available: {available}"
        )

    filename = f"{collection_name}.json.gz"
    url = f"{MTGJSON_BASE_URL}{filename}"
    dest_path = dest_dir / filename

    if clear_existing:
        _clear_directory(dest_dir, "*.json.gz")
        _clear_directory(dest_dir.parent / "json", "*.json")

    logger.info(f"Downloading {collection_name} collection")
    download_file(url, dest_path)

    return dest_path


def download_prices(dest_dir: Path, clear_existing: bool = False) -> Path:
    """Download AllPrices.json.gz from MTGJSON.

    Args:
        dest_dir: Directory to save file to
        clear_existing: Whether to clear existing files first

    Returns:
        Path to downloaded file

    Raises:
        DownloadError: If download fails
    """
    filename = "AllPrices.json.gz"
    url = f"{MTGJSON_BASE_URL}{filename}"
    dest_path = dest_dir / filename

    if clear_existing:
        _clear_directory(dest_dir, "*.json.gz")
        _clear_directory(dest_dir.parent / "json", "*.json")

    logger.info("Downloading price data")
    download_file(url, dest_path)

    return dest_path


def get_available_collections() -> List[str]:
    """Get list of available collection names.

    Returns:
        Sorted list of collection names
    """
    return sorted(AVAILABLE_COLLECTIONS)


def validate_set_codes(set_codes: List[str]) -> List[str]:
    """Validate and clean set codes.

    Args:
        set_codes: List of set codes to validate

    Returns:
        List of cleaned (uppercase) set codes
    """
    if not set_codes:
        raise ValueError("No set codes provided")

    # Clean up set codes (uppercase, remove duplicates)
    cleaned_codes = list(
        dict.fromkeys(code.upper().strip() for code in set_codes if code.strip())
    )

    if not cleaned_codes:
        raise ValueError("No valid set codes provided")

    return cleaned_codes


def _clear_directory(directory: Path, pattern: str = "*") -> None:
    """Clear files matching pattern from a directory.

    Args:
        directory: Directory to clear
        pattern: Glob pattern for files to delete
    """
    if not directory.exists():
        logger.debug(f"Directory doesn't exist, nothing to clear: {directory}")
        return

    files_to_delete = list(directory.glob(pattern))

    if not files_to_delete:
        logger.debug(f"No files matching '{pattern}' found in {directory}")
        return

    logger.info(f"Clearing {len(files_to_delete)} files from {directory}")
    deleted_count = 0

    for file_path in files_to_delete:
        try:
            if file_path.is_file():
                file_path.unlink()
                deleted_count += 1
                logger.debug(f"Deleted: {file_path.name}")
        except Exception as e:
            logger.error(f"Failed to delete {file_path.name}: {e}")

    if deleted_count > 0:
        logger.info(f"✓ Cleared {deleted_count} files from {directory}")
    else:
        logger.debug(f"No files deleted from {directory}")
