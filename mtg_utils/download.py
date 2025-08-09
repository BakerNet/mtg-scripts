"""Download utilities for MTG data from MTGJSON.

Requires Python 3.10+
"""

import logging
import urllib.request
from pathlib import Path
from typing import List
from urllib.error import HTTPError, URLError

import tqdm

from .file_operations import create_directories

logger = logging.getLogger(__name__)

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
