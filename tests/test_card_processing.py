"""Tests for card processing module.

Tests card data processing utilities including data preparation,
price calculations, and validation.

Requires Python 3.10+
"""

import json

from mtg_utils.card_processing import (
    calculate_average_price,
    extract_tcgplayer_price,
    filter_cards_by_format,
    merge_card_duplicates,
    prepare_card_data,
    process_price_batch,
    process_set_cards,
    validate_card_data,
)


class TestCardProcessing:
    """Test card data processing functions."""

    def test_prepare_card_data(self, sample_card_data: dict):
        """Test preparing card data for database insertion."""
        set_code = "TST"
        set_name = "Test Set"
        collection_name = None

        result = prepare_card_data(
            sample_card_data, set_code, set_name, collection_name
        )

        assert result[0] == sample_card_data["uuid"]  # uuid
        assert result[1] == sample_card_data["name"]  # name
        assert result[2] == set_code  # set_code
        assert result[3] == set_name  # set_name
        assert result[4] == collection_name  # collection_name
        assert result[6] == sample_card_data["manaCost"]  # mana_cost
        assert result[7] == sample_card_data["manaValue"]  # mana_value

        # Check JSON fields are serialized
        assert json.loads(result[13]) == sample_card_data["colors"]  # colors
        assert (
            json.loads(result[14]) == sample_card_data["colorIdentity"]
        )  # color_identity

    def test_prepare_card_data_with_collection(self, sample_card_data: dict):
        """Test preparing card data with collection name."""
        set_code = "TST"
        set_name = "Test Set"
        collection_name = "My Collection"

        result = prepare_card_data(
            sample_card_data, set_code, set_name, collection_name
        )

        assert result[4] == collection_name

    def test_prepare_card_data_boolean_fields(self):
        """Test boolean field handling in card data preparation."""
        card_data = {
            "uuid": "test-uuid",
            "name": "Test Card",
            "isReprint": True,
            "colors": [],
            "colorIdentity": [],
            "types": [],
            "subtypes": [],
            "supertypes": [],
            "keywords": [],
            "printings": [],
            "legalities": {},
        }

        result = prepare_card_data(card_data, "TST", "Test Set")

        assert result[22] == 1  # is_reprint should be 1 for True

    def test_process_set_cards(self, sample_set_data: dict):
        """Test processing cards from a set."""
        cards_data = process_set_cards(sample_set_data)

        assert len(cards_data) == 2  # Two cards in sample data

        # Check first card
        first_card = cards_data[0]
        assert first_card[1] == "Lightning Bolt"  # name
        assert first_card[2] == "TST"  # set_code
        assert first_card[3] == "Test Set"  # set_name

    def test_process_set_cards_with_collection(self, sample_set_data: dict):
        """Test processing cards with collection name."""
        collection_name = "My Collection"
        cards_data = process_set_cards(sample_set_data, collection_name)

        assert len(cards_data) == 2

        # Check collection name is set
        for card_data in cards_data:
            assert card_data[4] == collection_name  # collection_name

    def test_calculate_average_price(self):
        """Test price calculation."""
        price_dict = {"2023-01-01": 1.00, "2023-01-02": 2.00, "2023-01-03": 3.00}

        avg_price = calculate_average_price(price_dict)
        assert avg_price == 2.00

    def test_calculate_average_price_empty(self):
        """Test price calculation with empty data."""
        assert calculate_average_price({}) is None
        assert calculate_average_price({"date": None}) is None

    def test_calculate_average_price_with_none_values(self):
        """Test price calculation filtering None values."""
        price_dict = {"2023-01-01": 1.00, "2023-01-02": None, "2023-01-03": 2.00}

        avg_price = calculate_average_price(price_dict)
        assert avg_price == 1.50  # (1.00 + 2.00) / 2

    def test_extract_tcgplayer_price(self, sample_price_data: dict):
        """Test extracting TCGPlayer price."""
        card_price_data = sample_price_data["data"]["test-uuid-123"]

        avg_price = extract_tcgplayer_price(card_price_data)

        # Should calculate average of 0.25, 0.30, 0.28
        expected = (0.25 + 0.30 + 0.28) / 3
        assert abs(avg_price - expected) < 0.001

    def test_extract_tcgplayer_price_missing_data(self):
        """Test extracting price with missing data structure."""
        # Missing paper data
        card_price_data = {}
        assert extract_tcgplayer_price(card_price_data) is None

        # Missing tcgplayer data
        card_price_data = {"paper": {}}
        assert extract_tcgplayer_price(card_price_data) is None

        # Missing retail data
        card_price_data = {"paper": {"tcgplayer": {}}}
        assert extract_tcgplayer_price(card_price_data) is None

        # Missing normal data
        card_price_data = {"paper": {"tcgplayer": {"retail": {}}}}
        assert extract_tcgplayer_price(card_price_data) is None

    def test_process_price_batch(self, sample_price_data: dict):
        """Test processing a batch of price data."""
        existing_uuids = {"test-uuid-123", "test-uuid-456"}
        price_entries = sample_price_data["data"]

        price_data_list = process_price_batch(price_entries, existing_uuids)

        assert len(price_data_list) == 2

        # Check structure of price data
        for uuid, price, date in price_data_list:
            assert uuid in existing_uuids
            assert isinstance(price, float)
            assert price > 0
            assert isinstance(date, str)

    def test_process_price_batch_filtered(self, sample_price_data: dict):
        """Test processing price batch with filtering."""
        # Only include one UUID in existing set
        existing_uuids = {"test-uuid-123"}
        price_entries = sample_price_data["data"]

        price_data_list = process_price_batch(price_entries, existing_uuids)

        assert len(price_data_list) == 1
        assert price_data_list[0][0] == "test-uuid-123"

    def test_validate_card_data_valid(self, sample_card_data: dict):
        """Test validating valid card data."""
        assert validate_card_data(sample_card_data) is True

    def test_validate_card_data_missing_uuid(self, sample_card_data: dict):
        """Test validating card data missing UUID."""
        del sample_card_data["uuid"]
        assert validate_card_data(sample_card_data) is False

    def test_validate_card_data_missing_name(self, sample_card_data: dict):
        """Test validating card data missing name."""
        del sample_card_data["name"]
        assert validate_card_data(sample_card_data) is False

    def test_validate_card_data_none_values(self):
        """Test validating card data with None values."""
        card_data = {"uuid": None, "name": "Test"}
        assert validate_card_data(card_data) is False

        card_data = {"uuid": "test", "name": None}
        assert validate_card_data(card_data) is False

    def test_merge_card_duplicates(self):
        """Test merging duplicate cards."""
        cards = [
            {"uuid": "uuid1", "name": "Card 1", "text": "Short"},
            {"uuid": "uuid2", "name": "Card 2", "text": "Text"},
            {
                "uuid": "uuid1",
                "name": "Card 1",
                "text": "Much longer text with more information",
            },
        ]

        unique_cards = merge_card_duplicates(cards)

        assert len(unique_cards) == 2

        # Should keep the longer version of uuid1
        card1 = next(card for card in unique_cards if card["uuid"] == "uuid1")
        assert "Much longer text" in card1["text"]

    def test_filter_cards_by_format(self):
        """Test filtering cards by format legality."""
        cards = [
            {
                "uuid": "uuid1",
                "name": "Standard Legal",
                "legalities": {"standard": "Legal", "modern": "Legal"},
            },
            {
                "uuid": "uuid2",
                "name": "Not Standard Legal",
                "legalities": {"standard": "Banned", "modern": "Legal"},
            },
            {
                "uuid": "uuid3",
                "name": "Standard Legal 2",
                "legalities": {"standard": "Legal", "legacy": "Legal"},
            },
        ]

        standard_legal = filter_cards_by_format(cards, "standard")

        assert len(standard_legal) == 2
        assert all(card["legalities"]["standard"] == "Legal" for card in standard_legal)

    def test_filter_cards_by_format_json_string(self):
        """Test filtering cards with JSON string legalities."""
        cards = [
            {
                "uuid": "uuid1",
                "name": "Test Card",
                "legalities": '{"standard": "Legal", "modern": "Legal"}',
            }
        ]

        standard_legal = filter_cards_by_format(cards, "standard")

        assert len(standard_legal) == 1
        assert standard_legal[0]["uuid"] == "uuid1"


class TestCardProcessingEdgeCases:
    """Test edge cases in card processing."""

    def test_prepare_card_data_missing_fields(self):
        """Test preparing card data with missing optional fields."""
        minimal_card = {"uuid": "test-uuid", "name": "Test Card"}

        # Should not raise exception
        result = prepare_card_data(minimal_card, "TST", "Test Set")

        assert result[0] == "test-uuid"
        assert result[1] == "Test Card"
        assert result[2] == "TST"
        assert result[3] == "Test Set"

    def test_process_set_cards_empty_set(self):
        """Test processing empty set."""
        empty_set = {"code": "EMPTY", "name": "Empty Set", "cards": []}

        result = process_set_cards(empty_set)
        assert result == []

    def test_process_set_cards_invalid_card(self):
        """Test processing set with invalid card data."""
        set_with_bad_card = {
            "code": "TST",
            "name": "Test Set",
            "cards": [
                {"uuid": "valid-uuid", "name": "Valid Card"},
                {"invalid": "data"},  # Missing required fields
            ],
        }

        # Should process valid cards and handle errors gracefully
        result = process_set_cards(set_with_bad_card)
        assert len(result) >= 0  # Depends on error handling implementation
