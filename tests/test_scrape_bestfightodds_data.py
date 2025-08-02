import unittest
from unittest.mock import patch, MagicMock, ANY
from ufcscraper.scripts import scrape_bestfightodds_data
from datetime import date

class TestScrapeBestFightOddsData(unittest.TestCase):
    @patch("ufcscraper.scripts.scrape_bestfightodds_data.BestFightOddsScraper")
    def test_main_invokes_scraper(self, MockScraper):
        mock_scraper = MockScraper.return_value
        args = MagicMock()
        args.data_folder = "dummy_folder"
        args.n_sessions = 2
        args.delay = 1
        args.min_date = "2020-01-01"
        args.log_level = "INFO"
        
        scrape_bestfightodds_data.main(args)
        MockScraper.assert_called_once_with(
            data_folder="dummy_folder",
            n_sessions=2,
            delay=1,
            min_date=date(2020, 1, 1)
        )
        mock_scraper.scrape_BFO_odds.assert_called_once()

        scraper_call = MockScraper.call_args
        assert scraper_call is not None
        kwargs = scraper_call.kwargs
        assert kwargs["data_folder"] == "dummy_folder"
        assert kwargs["n_sessions"] == 2
        assert kwargs["delay"] == 1
        assert isinstance(kwargs["min_date"], date)
        assert kwargs["min_date"] == date(2020, 1, 1)

    @patch("argparse.ArgumentParser.parse_args")
    @patch("ufcscraper.scripts.scrape_bestfightodds_data.BestFightOddsScraper")
    def test_main_with_no_args(self, MockScraper, mock_parse_args):
        mock_scraper = MockScraper.return_value
        mock_args = MagicMock()
        mock_args.data_folder = "dummy_folder"
        mock_args.n_sessions = 1
        mock_args.delay = 0
        mock_args.min_date = "2021-01-01"
        mock_args.log_level = "INFO"
        mock_parse_args.return_value = mock_args
        
        scrape_bestfightodds_data.main(None)
        mock_scraper.scrape_BFO_odds.assert_called_once()

if __name__ == "__main__":
    unittest.main()
