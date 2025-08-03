import unittest
from unittest.mock import patch, MagicMock
from ufcscraper.scripts import consolidate_bet365_odds

class TestConsolidateBet365Odds(unittest.TestCase):
    @patch("ufcscraper.scripts.consolidate_bet365_odds.Bet365Odds")
    def test_main_invokes_consolidate(self, MockBet365Odds):
        mock_reader = MockBet365Odds.return_value
        args = MagicMock()
        args.data_folder = "dummy_folder"
        args.max_date_diff_days = 5
        args.min_match_score = 80
        args.log_level = "INFO"

        consolidate_bet365_odds.main(args)
        MockBet365Odds.assert_called_once_with("dummy_folder")
        mock_reader.consolidate_odds.assert_called_once_with(
            max_date_diff_days=5,
            min_match_score=80,
        )

    @patch("argparse.ArgumentParser.parse_args")
    @patch("ufcscraper.scripts.consolidate_bet365_odds.Bet365Odds")
    def test_main_with_no_args(self, MockBet365Odds, mock_parse_args):
        mock_reader = MockBet365Odds.return_value
        mock_args = MagicMock()
        mock_args.data_folder = "dummy_folder"
        mock_args.max_date_diff_days = 3
        mock_args.min_match_score = 90
        mock_args.log_level = "INFO"
        mock_parse_args.return_value = mock_args

        consolidate_bet365_odds.main(None)
        mock_reader.consolidate_odds.assert_called_once_with(
            max_date_diff_days=3,
            min_match_score=90,
        )

if __name__ == "__main__":
    unittest.main()
