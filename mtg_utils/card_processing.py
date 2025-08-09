"""Card data processing utilities for MTG data.

Requires Python 3.10+
"""

import json
import logging
from datetime import date
from typing import Any

from .constants import CARD_FIELD_MAPPING, JSON_FIELDS

logger = logging.getLogger(__name__)


def prepare_card_data(
    card: dict[str, Any],
    set_code: str,
    set_name: str,
    collection_name: str | None = None,
) -> tuple:
    """Convert card dictionary to tuple for database insertion.

    Args:
        card: Card data dictionary from JSON
        set_code: Set code for the card
        set_name: Set name for the card
        collection_name: Optional collection name

    Returns:
        Tuple ready for database insertion
    """
    # Process fields that need JSON serialization
    processed_card = {}
    for json_key, db_key in CARD_FIELD_MAPPING.items():
        value = card.get(json_key)

        # Handle JSON fields
        if json_key in JSON_FIELDS and value is not None:
            value = json.dumps(value)

        # Handle boolean fields
        if json_key == "isReprint":
            value = 1 if value else 0

        processed_card[db_key] = value

    # Build tuple in the correct order for database insertion
    card_data = (
        processed_card.get("uuid"),
        processed_card.get("name"),
        set_code,
        set_name,
        collection_name,  # Will be None for regular sets
        processed_card.get("number"),
        processed_card.get("mana_cost"),
        processed_card.get("mana_value"),
        processed_card.get("type"),
        processed_card.get("text"),
        processed_card.get("power"),
        processed_card.get("toughness"),
        processed_card.get("loyalty"),
        processed_card.get("colors"),
        processed_card.get("color_identity"),
        processed_card.get("rarity"),
        processed_card.get("artist"),
        processed_card.get("flavor_text"),
        processed_card.get("converted_mana_cost"),
        processed_card.get("layout"),
        processed_card.get("frame_version"),
        processed_card.get("border_color"),
        processed_card.get("is_reprint"),
        processed_card.get("printings"),
        processed_card.get("types"),
        processed_card.get("subtypes"),
        processed_card.get("supertypes"),
        processed_card.get("keywords"),
        processed_card.get("legalities"),
        processed_card.get("edhrecRank"),
        processed_card.get("edhrecSaltiness"),
    )

    return card_data


def process_set_cards(
    set_data: dict[str, Any], collection_name: str | None = None
) -> list[tuple]:
    """Process all cards in a set.

    Args:
        set_data: Set data dictionary containing cards
        collection_name: Optional collection name

    Returns:
        List of card data tuples ready for insertion
    """
    set_code = set_data.get("code", "")
    set_name = set_data.get("name", "")
    cards = set_data.get("cards", [])

    card_data_list = []
    for card in cards:
        try:
            card_data = prepare_card_data(card, set_code, set_name, collection_name)
            card_data_list.append(card_data)
        except Exception as e:
            logger.error(f"Error processing card {card.get('name', 'Unknown')}: {e}")
            continue

    return card_data_list


def calculate_average_price(price_dict: dict[str, float]) -> float | None:
    """Calculate the average price from a dictionary of date->price mappings.

    Args:
        price_dict: Dictionary mapping dates to prices

    Returns:
        Average price or None if no valid prices
    """
    if not price_dict:
        return None

    # Filter out None/null values
    prices = [p for p in price_dict.values() if p is not None]

    if not prices:
        return None

    return sum(prices) / len(prices)


def extract_tcgplayer_price(card_price_data: dict[str, Any]) -> float | None:
    """Extract TCGPlayer paper non-foil price from price data.

    Args:
        card_price_data: Price data for a single card

    Returns:
        Average price or None if not available
    """
    # Navigate the nested structure: paper -> tcgplayer -> retail -> normal
    if "paper" not in card_price_data:
        return None

    paper_data = card_price_data["paper"]
    if "tcgplayer" not in paper_data:
        return None

    tcg_data = paper_data["tcgplayer"]
    if "retail" not in tcg_data:
        return None

    retail_data = tcg_data["retail"]
    if "normal" not in retail_data:
        return None

    tcg_prices = retail_data["normal"]
    return calculate_average_price(tcg_prices)


def prepare_price_data(
    uuid: str, average_price: float, price_date: str | None = None
) -> tuple[str, float, str]:
    """Prepare price data for database insertion.

    Args:
        uuid: Card UUID
        average_price: Calculated average price
        price_date: Date of price data (uses today if None)

    Returns:
        Tuple of (uuid, average_price, price_date)
    """
    if price_date is None:
        price_date = date.today().isoformat()

    return (uuid, average_price, price_date)


def process_price_batch(
    price_entries: dict[str, Any], existing_uuids: set
) -> list[tuple]:
    """Process a batch of price entries.

    Args:
        price_entries: Dictionary of UUID -> price data
        existing_uuids: Set of UUIDs that exist in the database

    Returns:
        List of price data tuples ready for insertion
    """
    price_data_list = []
    today = date.today().isoformat()

    for uuid, card_price_data in price_entries.items():
        # Skip if card not in database
        if uuid not in existing_uuids:
            continue

        # Extract TCGPlayer price
        avg_price = extract_tcgplayer_price(card_price_data)

        if avg_price is not None:
            price_data_list.append((uuid, avg_price, today))

    return price_data_list


def validate_card_data(card: dict[str, Any]) -> bool:
    """Validate that a card has required fields.

    Args:
        card: Card data dictionary

    Returns:
        True if card data is valid, False otherwise
    """
    required_fields = ["uuid", "name"]

    for field in required_fields:
        if field not in card or card[field] is None:
            logger.warning(f"Card missing required field: {field}")
            return False

    return True


def merge_card_duplicates(cards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge duplicate cards, keeping the most recent printing.

    Args:
        cards: List of card dictionaries

    Returns:
        List of unique cards
    """
    unique_cards = {}

    for card in cards:
        uuid = card.get("uuid")
        if uuid not in unique_cards:
            unique_cards[uuid] = card
        else:
            # Keep the card with more information
            existing = unique_cards[uuid]
            if len(str(card)) > len(str(existing)):
                unique_cards[uuid] = card

    return list(unique_cards.values())


def filter_cards_by_format(
    cards: list[dict[str, Any]], format_name: str
) -> list[dict[str, Any]]:
    """Filter cards by format legality.

    Args:
        cards: List of card dictionaries
        format_name: Format to filter by (e.g., 'standard', 'modern')

    Returns:
        List of cards legal in the specified format
    """
    filtered_cards = []

    for card in cards:
        legalities = card.get("legalities", {})
        if isinstance(legalities, str):
            legalities = json.loads(legalities)

        if legalities.get(format_name) == "Legal":
            filtered_cards.append(card)

    return filtered_cards
