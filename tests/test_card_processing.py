"""Tests for card processing module.

Tests card data processing utilities including data preparation,
price calculations, and validation.

Requires Python 3.10+
"""

import json

from mtg_utils.card_processing import (
    calculate_average_price,
    extract_tcgplayer_price,
    prepare_card_data,
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

