"""Pytest configuration and shared fixtures for MTG data processing tests.

This module provides common fixtures and test utilities used across
all test modules.

Requires Python 3.10+
"""

import sqlite3
import tempfile
from pathlib import Path
from typing import Generator
from unittest.mock import Mock, patch

import pytest

from mtg_utils.constants import CARD_PRICES_TABLE_SCHEMA, CARDS_TABLE_SCHEMA


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Provide a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as temp_path:
        yield Path(temp_path)


@pytest.fixture
def temp_db_path(temp_dir: Path) -> Path:
    """Provide a temporary database path."""
    return temp_dir / "test_cards.db"


@pytest.fixture
def test_db_connection(temp_db_path: Path) -> Generator[sqlite3.Connection, None, None]:
    """Provide a test database connection with tables created."""
    conn = sqlite3.connect(temp_db_path)
    cursor = conn.cursor()

    # Create tables
    cursor.execute(CARDS_TABLE_SCHEMA)
    cursor.execute(CARD_PRICES_TABLE_SCHEMA)

    # Create indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_name ON cards(name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_set_code ON cards(set_code)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_price_uuid ON card_prices(uuid)")

    conn.commit()

    yield conn

    conn.close()


@pytest.fixture
def sample_card_data() -> dict:
    """Provide sample card data for testing."""
    return {
        "uuid": "test-uuid-123",
        "name": "Lightning Bolt",
        "manaCost": "{R}",
        "manaValue": 1,
        "type": "Instant",
        "text": "Lightning Bolt deals 3 damage to any target.",
        "colors": ["R"],
        "colorIdentity": ["R"],
        "rarity": "common",
        "artist": "Christopher Rush",
        "types": ["Instant"],
        "legalities": {"standard": "Legal", "modern": "Legal"},
    }


@pytest.fixture
def sample_set_data() -> dict:
    """Provide sample set data for testing."""
    return {
        "code": "TST",
        "name": "Test Set",
        "cards": [
            {
                "uuid": "test-uuid-123",
                "name": "Lightning Bolt",
                "manaCost": "{R}",
                "manaValue": 1,
                "type": "Instant",
                "text": "Lightning Bolt deals 3 damage to any target.",
                "colors": ["R"],
                "colorIdentity": ["R"],
                "rarity": "common",
                "artist": "Christopher Rush",
                "types": ["Instant"],
                "legalities": {"standard": "Legal", "modern": "Legal"},
            },
            {
                "uuid": "test-uuid-456",
                "name": "Giant Growth",
                "manaCost": "{G}",
                "manaValue": 1,
                "type": "Instant",
                "text": "Target creature gets +3/+3 until end of turn.",
                "colors": ["G"],
                "colorIdentity": ["G"],
                "rarity": "common",
                "artist": "Sandra Everingham",
                "types": ["Instant"],
                "legalities": {"standard": "Legal", "modern": "Legal"},
            },
        ],
    }


@pytest.fixture
def sample_price_data() -> dict:
    """Provide sample price data for testing."""
    return {
        "data": {
            "test-uuid-123": {
                "paper": {
                    "tcgplayer": {
                        "retail": {
                            "normal": {
                                "2023-01-01": 0.25,
                                "2023-01-02": 0.30,
                                "2023-01-03": 0.28,
                            }
                        }
                    }
                }
            },
            "test-uuid-456": {
                "paper": {
                    "tcgplayer": {
                        "retail": {
                            "normal": {
                                "2023-01-01": 0.10,
                                "2023-01-02": 0.12,
                                "2023-01-03": 0.11,
                            }
                        }
                    }
                }
            },
        }
    }


@pytest.fixture
def mock_logger():
    """Provide a mock logger for testing."""
    with patch("mtg_utils.database.logger") as mock_log:
        yield mock_log


@pytest.fixture
def mock_config():
    """Provide a mock configuration for testing."""
    mock_cfg = Mock()
    mock_cfg.batch_size = 100
    mock_cfg.progress_interval = 50
    mock_cfg.db_path = Path("test.db")
    return mock_cfg


class MockCursor:
    """Mock cursor for database testing."""

    def __init__(self):
        self.executed_queries = []
        self.fetchall_result = []
        self.fetchone_result = None
        self.rowcount = 0

    def execute(self, query: str, params: tuple = ()):
        self.executed_queries.append((query, params))
        return self

    def executemany(self, query: str, params_list: list):
        for params in params_list:
            self.executed_queries.append((query, params))
        return self

    def fetchall(self):
        return self.fetchall_result

    def fetchone(self):
        return self.fetchone_result

    def close(self):
        pass


class MockConnection:
    """Mock connection for database testing."""

    def __init__(self):
        self.cursor_instance = MockCursor()
        self.committed = False
        self.closed = False

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.committed = True

    def rollback(self):
        pass

    def close(self):
        self.closed = True

    def execute(self, query: str):
        return self.cursor_instance.execute(query)


@pytest.fixture
def mock_db_connection():
    """Provide a mock database connection."""
    return MockConnection()


def assert_query_executed(cursor, expected_query_part: str):
    """Assert that a query containing the expected part was executed."""
    executed = any(expected_query_part in query for query, _ in cursor.executed_queries)
    assert (
        executed
    ), f"Expected query containing '{expected_query_part}' not found in {cursor.executed_queries}"


def create_test_file(path: Path, content: str | bytes):
    """Create a test file with given content."""
    if isinstance(content, str):
        path.write_text(content, encoding="utf-8")
    else:
        path.write_bytes(content)
