"""Tests for I/O operations module.

Requires Python 3.10+
"""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from mtg_utils.io_operations import read_card_list


class TestReadCardList:
    """Tests for read_card_list function with various deck formats."""

    def test_plain_text_format(self, temp_dir):
        """Test reading plain text format (one card per line)."""
        test_file = temp_dir / "plain.txt"
        test_file.write_text("Lightning Bolt\nGiant Growth\nCounterspell\n")
        
        result = read_card_list(test_file)
        
        assert result == ["Lightning Bolt", "Giant Growth", "Counterspell"]

    def test_mtgo_format_with_quantities(self, temp_dir):
        """Test reading MTGO format with quantities."""
        test_content = """4 Lightning Bolt
3 Giant Growth
1 Black Lotus
2 Mox Pearl"""
        
        test_file = temp_dir / "mtgo.txt"
        test_file.write_text(test_content)
        
        result = read_card_list(test_file)
        
        expected = [
            "Lightning Bolt", "Lightning Bolt", "Lightning Bolt", "Lightning Bolt",
            "Giant Growth", "Giant Growth", "Giant Growth", 
            "Black Lotus",
            "Mox Pearl", "Mox Pearl"
        ]
        assert result == expected

    def test_mtgo_format_with_sideboard(self, temp_dir):
        """Test reading MTGO format with sideboard section."""
        test_content = """4 Abhorrent Oculus
4 Emperor of Bones
1 Spell Pierce

Sideboard
1 Pyroclasm
2 Nihil Spellbomb"""
        
        test_file = temp_dir / "mtgo_sideboard.txt"
        test_file.write_text(test_content)
        
        result = read_card_list(test_file)
        
        expected = [
            "Abhorrent Oculus", "Abhorrent Oculus", "Abhorrent Oculus", "Abhorrent Oculus",
            "Emperor of Bones", "Emperor of Bones", "Emperor of Bones", "Emperor of Bones",
            "Spell Pierce",
            "Pyroclasm",
            "Nihil Spellbomb", "Nihil Spellbomb"
        ]
        assert result == expected

    def test_dec_format_with_comments(self, temp_dir):
        """Test reading DEK format with comments."""
        test_content = """//Modern Grixis Reanimator deck by Ale_Mtg

4 Abhorrent Oculus
4 Emperor of Bones
1 Spell Pierce

// Sideboard:

SB: 1 Pyroclasm
SB: 2 Nihil Spellbomb"""
        
        test_file = temp_dir / "deck.dec"
        test_file.write_text(test_content)
        
        result = read_card_list(test_file)
        
        expected = [
            "Abhorrent Oculus", "Abhorrent Oculus", "Abhorrent Oculus", "Abhorrent Oculus",
            "Emperor of Bones", "Emperor of Bones", "Emperor of Bones", "Emperor of Bones",
            "Spell Pierce",
            "Pyroclasm",
            "Nihil Spellbomb", "Nihil Spellbomb"
        ]
        assert result == expected

    def test_unicode_card_names(self, temp_dir):
        """Test reading files with unicode characters in card names."""
        test_content = """1 Troll of Khazad-dûm
2 Jötun Grunt
1 Æther Vial"""
        
        test_file = temp_dir / "unicode.txt"
        test_file.write_text(test_content, encoding='utf-8')
        
        result = read_card_list(test_file)
        
        expected = [
            "Troll of Khazad-dûm",
            "Jötun Grunt", "Jötun Grunt",
            "Æther Vial"
        ]
        assert result == expected

    def test_empty_lines_and_whitespace(self, temp_dir):
        """Test handling of empty lines and whitespace."""
        test_content = """
4 Lightning Bolt

2 Giant Growth   

1 Black Lotus

"""
        
        test_file = temp_dir / "whitespace.txt"
        test_file.write_text(test_content)
        
        result = read_card_list(test_file)
        
        expected = [
            "Lightning Bolt", "Lightning Bolt", "Lightning Bolt", "Lightning Bolt",
            "Giant Growth", "Giant Growth",
            "Black Lotus"
        ]
        assert result == expected

    def test_mixed_case_sideboard_marker(self, temp_dir):
        """Test case-insensitive sideboard marker."""
        test_content = """4 Lightning Bolt
SIDEBOARD
1 Pyroclasm"""
        
        test_file = temp_dir / "mixed_case.txt"
        test_file.write_text(test_content)
        
        result = read_card_list(test_file)
        
        expected = [
            "Lightning Bolt", "Lightning Bolt", "Lightning Bolt", "Lightning Bolt",
            "Pyroclasm"
        ]
        assert result == expected

    def test_file_not_found(self, temp_dir):
        """Test error handling for non-existent files."""
        non_existent_file = temp_dir / "does_not_exist.txt"
        
        with pytest.raises(FileNotFoundError, match="Card list file not found"):
            read_card_list(non_existent_file)

    def test_empty_file(self, temp_dir):
        """Test reading empty files."""
        test_file = temp_dir / "empty.txt"
        test_file.write_text("")
        
        result = read_card_list(test_file)
        
        assert result == []

    def test_only_comments_and_empty_lines(self, temp_dir):
        """Test file with only comments and empty lines."""
        test_content = """//This is a comment

//Another comment

"""
        
        test_file = temp_dir / "only_comments.dec"
        test_file.write_text(test_content)
        
        result = read_card_list(test_file)
        
        assert result == []

    def test_zero_quantity_cards(self, temp_dir):
        """Test handling of zero quantity cards."""
        test_content = """4 Lightning Bolt
0 Giant Growth
2 Counterspell"""
        
        test_file = temp_dir / "zero_quantity.txt"
        test_file.write_text(test_content)
        
        result = read_card_list(test_file)
        
        expected = [
            "Lightning Bolt", "Lightning Bolt", "Lightning Bolt", "Lightning Bolt",
            "Counterspell", "Counterspell"
        ]
        assert result == expected

    def test_large_quantities(self, temp_dir):
        """Test handling of large card quantities."""
        test_content = """100 Lightning Bolt
1 Black Lotus"""
        
        test_file = temp_dir / "large_quantities.txt"
        test_file.write_text(test_content)
        
        result = read_card_list(test_file)
        
        assert len(result) == 101
        assert result[:100] == ["Lightning Bolt"] * 100
        assert result[100] == "Black Lotus"

    @patch('mtg_utils.io_operations.logger')
    def test_logging_output(self, mock_logger, temp_dir):
        """Test that appropriate logging messages are generated."""
        test_file = temp_dir / "test.txt"
        test_file.write_text("4 Lightning Bolt\n2 Giant Growth")
        
        result = read_card_list(test_file)
        
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args[0][0]
        assert "Read 6 card entries" in call_args
        assert str(test_file) in call_args

    def test_realistic_example_txt_file(self, temp_dir):
        """Test with the realistic example from the repository."""
        test_content = """4 Abhorrent Oculus
4 Emperor of Bones
4 Psychic Frog
1 Troll of Khazad-dûm
4 Archon of Cruelty
1 Spell Pierce
4 Fatal Push
4 Thought Scour
4 Persist
4 Faithless Looting
4 Thoughtseize
4 Unearth
1 Undercity Sewers
1 Raucous Theater
2 Watery Grave
1 Blood Crypt
1 Scalding Tarn
1 Steam Vents
4 Bloodstained Mire
2 Swamp
4 Polluted Delta
1 Island

Sideboard
1 Pyroclasm
2 Nihil Spellbomb
2 Mystical Dispute
1 Harvester of Misery
4 Consign to Memory
2 Meltdown
1 Into the Flood Maw
2 Harbinger of the Seas"""
        
        test_file = temp_dir / "realistic_example.txt"
        test_file.write_text(test_content)
        
        result = read_card_list(test_file)
        
        # Count total cards: main deck (60) + sideboard (15) = 75
        assert len(result) == 75
        
        # Verify specific high-quantity cards
        assert result.count("Abhorrent Oculus") == 4
        assert result.count("Consign to Memory") == 4
        assert result.count("Troll of Khazad-dûm") == 1

    def test_realistic_example_dec_file(self, temp_dir):
        """Test with the realistic .dec example from the repository."""
        test_content = """//Modern Grixis Reanimator deck by Ale_Mtg

4 Abhorrent Oculus
4 Emperor of Bones
4 Psychic Frog
1 Troll of Khazad-dûm
4 Archon of Cruelty
1 Spell Pierce
4 Fatal Push
4 Thought Scour
4 Persist
4 Faithless Looting
4 Thoughtseize
4 Unearth
1 Undercity Sewers
1 Raucous Theater
2 Watery Grave
1 Blood Crypt
1 Scalding Tarn
1 Steam Vents
4 Bloodstained Mire
2 Swamp
4 Polluted Delta
1 Island

// Sideboard:

SB: 1 Pyroclasm
SB: 2 Nihil Spellbomb
SB: 2 Mystical Dispute
SB: 1 Harvester of Misery
SB: 4 Consign to Memory
SB: 2 Meltdown
SB: 1 Into the Flood Maw
SB: 2 Harbinger of the Seas"""
        
        test_file = temp_dir / "realistic_example.dec"
        test_file.write_text(test_content)
        
        result = read_card_list(test_file)
        
        # Count total cards: main deck (60) + sideboard (15) = 75
        assert len(result) == 75
        
        # Verify specific high-quantity cards
        assert result.count("Abhorrent Oculus") == 4
        assert result.count("Consign to Memory") == 4
        assert result.count("Troll of Khazad-dûm") == 1

    def test_set_annotations_basic(self, temp_dir):
        """Test basic set annotation parsing."""
        test_content = """4 [MOR] Heritage Druid
1 [A] Black Lotus
2 [ZEN] Verdant Catacombs"""
        
        test_file = temp_dir / "set_annotations.txt"
        test_file.write_text(test_content)
        
        result = read_card_list(test_file)
        
        expected = [
            "Heritage Druid", "Heritage Druid", "Heritage Druid", "Heritage Druid",
            "Black Lotus",
            "Verdant Catacombs", "Verdant Catacombs"
        ]
        assert result == expected

    def test_set_annotations_empty_brackets(self, temp_dir):
        """Test handling of empty brackets."""
        test_content = """4 [] Lightning Bolt
2 [] Giant Growth"""
        
        test_file = temp_dir / "empty_brackets.txt"
        test_file.write_text(test_content)
        
        result = read_card_list(test_file)
        
        expected = [
            "Lightning Bolt", "Lightning Bolt", "Lightning Bolt", "Lightning Bolt",
            "Giant Growth", "Giant Growth"
        ]
        assert result == expected

    def test_set_annotations_with_sideboard(self, temp_dir):
        """Test set annotations with sideboard entries."""
        test_content = """4 [MOR] Heritage Druid
1 [A] Black Lotus

SB: 2 [RTR] Abrupt Decay
SB: 1 [TE] Choke"""
        
        test_file = temp_dir / "set_annotations_sb.mwDeck"
        test_file.write_text(test_content)
        
        result = read_card_list(test_file)
        
        expected = [
            "Heritage Druid", "Heritage Druid", "Heritage Druid", "Heritage Druid",
            "Black Lotus",
            "Abrupt Decay", "Abrupt Decay",
            "Choke"
        ]
        assert result == expected

    def test_set_annotations_mixed_formats(self, temp_dir):
        """Test mixing cards with and without set annotations."""
        test_content = """4 [MOR] Heritage Druid
2 Lightning Bolt
1 [A] Black Lotus
3 Giant Growth"""
        
        test_file = temp_dir / "mixed_formats.txt"
        test_file.write_text(test_content)
        
        result = read_card_list(test_file)
        
        expected = [
            "Heritage Druid", "Heritage Druid", "Heritage Druid", "Heritage Druid",
            "Lightning Bolt", "Lightning Bolt",
            "Black Lotus",
            "Giant Growth", "Giant Growth", "Giant Growth"
        ]
        assert result == expected

    def test_set_annotations_complex_set_codes(self, temp_dir):
        """Test various set code formats."""
        test_content = """1 [2ED] Lightning Bolt
1 [M15] Reclamation Sage
1 [FUT] Dryad Arbor
1 [AVR] Craterhoof Behemoth"""
        
        test_file = temp_dir / "complex_sets.txt"
        test_file.write_text(test_content)
        
        result = read_card_list(test_file)
        
        expected = [
            "Lightning Bolt",
            "Reclamation Sage", 
            "Dryad Arbor",
            "Craterhoof Behemoth"
        ]
        assert result == expected

    def test_realistic_mwdeck_format(self, temp_dir):
        """Test with a realistic .mwDeck format file."""
        test_content = """// Deck file created with mtgtop8.com
// NAME : Elfballz
// CREATOR : Gregory_Millon
// FORMAT : Legacy
4 [] Allosaurus Shepherd
1 [] Atraxa, Grand Unifier
2 [ON] Birchlore Rangers
1 [AVR] Craterhoof Behemoth
3 [] Eladamri, Korvecdal
1 [ALA] Elvish Visionary
4 [MOR] Heritage Druid
4 [EVE] Nettle Sentinel
2 [VI] Quirion Ranger
4 [SC] Wirewood Symbiote
3 [] Once Upon a Time
4 [CHK] Glimpse of Nature
4 [MBS] Green Sun's Zenith
3 [VI] Natural Order
1 [] Grist, the Hunger Tide
2 [A] Bayou
2 [] Boseiju, Who Endures
2 [FUT] Dryad Arbor
2 [A] Forest
4 [US] Gaea's Cradle
3 [ZEN] Verdant Catacombs
3 [ON] Windswept Heath
1 [] Yavimaya, Cradle of Growth
SB:  3 [RTR] Abrupt Decay
SB:  1 [] Assassin's Trophy
SB:  1 [TE] Choke
SB:  2 [] Collector Ouphe
SB:  1 [] Disruptor Flute
SB:  3 [] Endurance
SB:  2 [] Force of Vigor
SB:  1 [] Keen-Eyed Curator
SB:  1 [M15] Reclamation Sage"""
        
        test_file = temp_dir / "elfballz.mwDeck"
        test_file.write_text(test_content)
        
        result = read_card_list(test_file)
        
        # Count total cards: 60 + 15 = 75
        assert len(result) == 75
        
        # Verify set annotations are stripped
        assert result.count("Heritage Druid") == 4
        assert result.count("Allosaurus Shepherd") == 4
        assert result.count("Abrupt Decay") == 3
        assert result.count("Gaea's Cradle") == 4
        
        # Verify no set codes remain in card names
        for card_name in result:
            assert not '[' in card_name
            assert not ']' in card_name

    def test_mtgs_format_basic(self, temp_dir):
        """Test basic MTGS format with tab-separated values."""
        test_content = """[DECK]
4x\tLightning Bolt
2x\tGiant Growth
1x\tBlack Lotus
[/DECK]"""
        
        test_file = temp_dir / "mtgs_basic.mtgsDeck"
        test_file.write_text(test_content)
        
        result = read_card_list(test_file)
        
        expected = [
            "Lightning Bolt", "Lightning Bolt", "Lightning Bolt", "Lightning Bolt",
            "Giant Growth", "Giant Growth",
            "Black Lotus"
        ]
        assert result == expected

    def test_mtgs_format_with_sideboard(self, temp_dir):
        """Test MTGS format with sideboard section."""
        test_content = """[DECK]
4x\tLightning Bolt
2x\tGiant Growth

Sideboard
3x\tAbrupt Decay
1x\tPyroclasm
[/DECK]"""
        
        test_file = temp_dir / "mtgs_sideboard.mtgsDeck"
        test_file.write_text(test_content)
        
        result = read_card_list(test_file)
        
        expected = [
            "Lightning Bolt", "Lightning Bolt", "Lightning Bolt", "Lightning Bolt",
            "Giant Growth", "Giant Growth",
            "Abrupt Decay", "Abrupt Decay", "Abrupt Decay",
            "Pyroclasm"
        ]
        assert result == expected

    def test_mtgs_format_with_url_tags(self, temp_dir):
        """Test MTGS format with URL tags that should be ignored."""
        test_content = """[DECK]
4x\tLightning Bolt
2x\tGiant Growth
[/DECK]
[URL="http://example.com"]Link to deck[/URL]"""
        
        test_file = temp_dir / "mtgs_url.mtgsDeck"
        test_file.write_text(test_content)
        
        result = read_card_list(test_file)
        
        expected = [
            "Lightning Bolt", "Lightning Bolt", "Lightning Bolt", "Lightning Bolt",
            "Giant Growth", "Giant Growth"
        ]
        assert result == expected

    def test_mtgs_format_realistic_example(self, temp_dir):
        """Test with realistic MTGS format file."""
        test_content = """[DECK]
4x\tAncient Stirrings
2x\tBreeding Pool
4x\tDeathrender
4x\tDeathrite Shaman
3x\tEmrakul, the Aeons Torn
4x\tFauna Shaman
1x\tForest
2x\tLiliana of the Veil
1x\tMarsh Flats
4x\tMisty Rainforest
4x\tNoble Hierarch
3x\tOvergrown Tomb
4x\tSerum Visions
2x\tSteelshaper's Gift
1x\tSwamp
4x\tSylvan Caryatid
1x\tTemple Garden
4x\tVerdant Catacombs
4x\tViscera Seer
2x\tWatery Grave
2x\tWild Cantor

Sideboard
3x\tAbrupt Decay
3x\tDamnation
4x\tLeyline of Sanctity
3x\tLeyline of the Void
2x\tRemand
[/DECK]
[URL="http://tappedout.net/mtg-decks/deathrender-a-combo-concept/"]Link to deck @ TappedOut.net[/URL]"""
        
        test_file = temp_dir / "realistic_mtgs.mtgsDeck"
        test_file.write_text(test_content)
        
        result = read_card_list(test_file)
        
        # Count total cards: 60 + 15 = 75
        assert len(result) == 75
        
        # Verify specific card counts
        assert result.count("Ancient Stirrings") == 4
        assert result.count("Emrakul, the Aeons Torn") == 3
        assert result.count("Abrupt Decay") == 3
        assert result.count("Leyline of Sanctity") == 4

    def test_mixed_formats_in_single_file(self, temp_dir):
        """Test file with mixed format styles."""
        test_content = """[DECK]
4x\tAncient Stirrings
2 Lightning Bolt
1 [MOR] Heritage Druid

Sideboard
SB: 2x\tAbrupt Decay
SB: 1 Force of Will
[/DECK]"""
        
        test_file = temp_dir / "mixed_formats.txt"
        test_file.write_text(test_content)
        
        result = read_card_list(test_file)
        
        expected = [
            "Ancient Stirrings", "Ancient Stirrings", "Ancient Stirrings", "Ancient Stirrings",
            "Lightning Bolt", "Lightning Bolt",
            "Heritage Druid",
            "Abrupt Decay", "Abrupt Decay",
            "Force of Will"
        ]
        assert result == expected