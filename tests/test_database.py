"""Tests for database operations module.

Tests database utilities including connection management,
table creation, and data operations.

Requires Python 3.10+
"""

import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

from mtg_utils.database import (
    batch_insert_cards,
    create_database,
    create_price_table,
    ensure_column_exists,
    get_connection,
    get_existing_card_uuids,
)


class TestDatabase:
    """Test database operations."""

    def test_create_database_new(self, temp_db_path: Path):
        """Test creating a new database."""
        conn = create_database(temp_db_path)

        # Check tables exist
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cursor.fetchall()}

        assert "cards" in tables
        conn.close()

    def test_create_database_existing(
        self, test_db_connection: sqlite3.Connection, temp_db_path: Path
    ):
        """Test opening existing database."""
        test_db_connection.close()

        conn = create_database(temp_db_path)

        # Should not fail
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cursor.fetchall()}

        assert "cards" in tables
        conn.close()

    def test_create_database_fresh_start(
        self, test_db_connection: sqlite3.Connection, temp_db_path: Path
    ):
        """Test creating database with fresh start."""
        # Insert some data first
        cursor = test_db_connection.cursor()
        cursor.execute(
            "INSERT INTO cards (uuid, name, set_code, set_name) VALUES (?, ?, ?, ?)",
            ("test", "Test Card", "TST", "Test Set"),
        )
        test_db_connection.commit()
        test_db_connection.close()

        # Create with fresh start
        conn = create_database(temp_db_path, fresh_start=True)

        # Check data is gone
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM cards")
        count = cursor.fetchone()[0]

        assert count == 0
        conn.close()

    def test_get_connection_context_manager(self, temp_db_path: Path):
        """Test database connection context manager."""
        with get_connection(temp_db_path) as conn:
            assert isinstance(conn, sqlite3.Connection)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            assert result[0] == 1

    def test_create_price_table(self, test_db_connection: sqlite3.Connection):
        """Test creating price table."""
        create_price_table(test_db_connection)

        cursor = test_db_connection.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='card_prices'"
        )
        result = cursor.fetchone()

        assert result is not None

    def test_batch_insert_cards(self, test_db_connection: sqlite3.Connection):
        """Test batch inserting cards."""
        cards_data = [
            (
                "uuid1",
                "Card 1",
                "SET",
                "Set Name",
                None,
                "1",
                "{R}",
                1,
                "Instant",
                "Text",
                None,
                None,
                None,
                '["R"]',
                '["R"]',
                "common",
                "Artist",
                None,
                1,
                "normal",
                "2015",
                "black",
                0,
                "[]",
                '["Instant"]',
                "[]",
                "[]",
                "[]",
                '{"standard": "Legal"}',
                None,
                None,
            ),
            (
                "uuid2",
                "Card 2",
                "SET",
                "Set Name",
                None,
                "2",
                "{U}",
                1,
                "Instant",
                "Text",
                None,
                None,
                None,
                '["U"]',
                '["U"]',
                "common",
                "Artist",
                None,
                1,
                "normal",
                "2015",
                "black",
                0,
                "[]",
                '["Instant"]',
                "[]",
                "[]",
                "[]",
                '{"standard": "Legal"}',
                None,
                None,
            ),
        ]

        new, updated, skipped = batch_insert_cards(test_db_connection, cards_data)

        assert new == 2
        assert updated == 0
        assert skipped == 0

        # Check cards were inserted
        cursor = test_db_connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM cards")
        count = cursor.fetchone()[0]
        assert count == 2

    def test_batch_insert_cards_update(self, test_db_connection: sqlite3.Connection):
        """Test updating existing cards in batch insert."""
        # Insert initial card
        cards_data = [
            (
                "uuid1",
                "Card 1",
                "SET",
                "Set Name",
                None,
                "1",
                "{R}",
                1,
                "Instant",
                "Text",
                None,
                None,
                None,
                '["R"]',
                '["R"]',
                "common",
                "Artist",
                None,
                1,
                "normal",
                "2015",
                "black",
                0,
                "[]",
                '["Instant"]',
                "[]",
                "[]",
                "[]",
                '{"standard": "Legal"}',
                None,
                None,
            )
        ]

        batch_insert_cards(test_db_connection, cards_data)

        # Update the same card
        updated_cards_data = [
            (
                "uuid1",
                "Updated Card 1",
                "SET",
                "Set Name",
                None,
                "1",
                "{R}",
                1,
                "Instant",
                "Updated Text",
                None,
                None,
                None,
                '["R"]',
                '["R"]',
                "common",
                "Artist",
                None,
                1,
                "normal",
                "2015",
                "black",
                0,
                "[]",
                '["Instant"]',
                "[]",
                "[]",
                "[]",
                '{"standard": "Legal"}',
                None,
                None,
            )
        ]

        new, updated, skipped = batch_insert_cards(
            test_db_connection, updated_cards_data
        )

        assert new == 0
        assert updated == 1
        assert skipped == 0

    def test_get_existing_card_uuids(self, test_db_connection: sqlite3.Connection):
        """Test getting existing card UUIDs."""
        # Insert some cards
        cursor = test_db_connection.cursor()
        cursor.execute(
            "INSERT INTO cards (uuid, name, set_code, set_name) VALUES (?, ?, ?, ?)",
            ("uuid1", "Card 1", "SET", "Set Name"),
        )
        cursor.execute(
            "INSERT INTO cards (uuid, name, set_code, set_name) VALUES (?, ?, ?, ?)",
            ("uuid2", "Card 2", "SET", "Set Name"),
        )
        test_db_connection.commit()

        uuids = get_existing_card_uuids(test_db_connection)

        assert uuids == {"uuid1", "uuid2"}

    def test_ensure_column_exists_add_column(
        self, test_db_connection: sqlite3.Connection
    ):
        """Test adding a new column."""
        result = ensure_column_exists(test_db_connection, "cards", "new_column", "TEXT")

        assert result is True  # Column was added

        # Check column exists
        cursor = test_db_connection.cursor()
        cursor.execute("PRAGMA table_info(cards)")
        columns = [col[1] for col in cursor.fetchall()]

        assert "new_column" in columns

    def test_ensure_column_exists_existing_column(
        self, test_db_connection: sqlite3.Connection
    ):
        """Test with existing column."""
        result = ensure_column_exists(test_db_connection, "cards", "name", "TEXT")

        assert result is False  # Column already existed

    @patch("mtg_utils.database.sqlite3.connect")
    def test_database_error_handling(self, mock_connect):
        """Test database error handling."""
        mock_connect.side_effect = sqlite3.OperationalError("Database error")

        with pytest.raises(sqlite3.OperationalError):
            create_database(Path("test.db"))


class TestDatabaseIntegration:
    """Integration tests for database operations."""

    @pytest.mark.integration
    def test_full_database_workflow(self, temp_db_path: Path, sample_card_data: dict):
        """Test complete database workflow."""
        # Create database
        conn = create_database(temp_db_path)

        # Create price table
        create_price_table(conn)

        # Insert card data
        cards_data = [
            (
                sample_card_data["uuid"],
                sample_card_data["name"],
                "TST",
                "Test Set",
                None,  # collection_name
                "1",  # number
                sample_card_data["manaCost"],
                sample_card_data["manaValue"],
                sample_card_data["type"],
                sample_card_data["text"],
                None,  # power
                None,  # toughness
                None,  # loyalty
                '["R"]',  # colors
                '["R"]',  # color_identity
                sample_card_data["rarity"],
                sample_card_data["artist"],
                None,  # flavor_text
                1,  # converted_mana_cost
                "normal",  # layout
                "2015",  # frame_version
                "black",  # border_color
                0,  # is_reprint
                "[]",  # printings
                '["Instant"]',  # types
                "[]",  # subtypes
                "[]",  # supertypes
                "[]",  # keywords
                '{"standard": "Legal", "modern": "Legal"}',  # legalities
                None,  # edhrecRank
                None,  # edhrecSaltiness
            )
        ]

        new, updated, skipped = batch_insert_cards(conn, cards_data)

        assert new == 1
        assert updated == 0
        assert skipped == 0

        # Check card exists
        uuids = get_existing_card_uuids(conn)
        assert sample_card_data["uuid"] in uuids

        conn.close()
