import unittest
from unittest.mock import patch, MagicMock
from ufcscraper.scripts import scrape_ufcstats_data

class TestScrapeUFCStatsData(unittest.TestCase):
    @patch("ufcscraper.scripts.scrape_ufcstats_data.UFCScraper")
    def test_main_invokes_scraper(self, MockScraper):
        mock_scraper = MockScraper.return_value
        args = MagicMock()
        args.data_folder = "dummy_folder"
        args.n_sessions = 2
        args.delay = 1
        args.log_level = "INFO"
        args.scrape_replacements = False
        
        scrape_ufcstats_data.main(args)
        MockScraper.assert_called_once_with(
            data_folder="dummy_folder",
            n_sessions=2,
            delay=1
        )
        mock_scraper.fighter_scraper.scrape_fighters.assert_called_once()
        mock_scraper.event_scraper.scrape_events.assert_called_once()
        mock_scraper.fight_scraper.scrape_fights.assert_called_once()
        # replacement_scraper should not be called
        if hasattr(mock_scraper, "replacement_scraper"):
            if hasattr(mock_scraper.replacement_scraper, "scrape_replacements"):
                mock_scraper.replacement_scraper.scrape_replacements.assert_not_called()

    @patch("argparse.ArgumentParser.parse_args")
    @patch("ufcscraper.scripts.scrape_ufcstats_data.UFCScraper")
    def test_main_with_scrape_replacements(self, MockScraper, mock_parse_args):
        mock_scraper = MockScraper.return_value
        mock_args = MagicMock()
        mock_args.data_folder = "dummy_folder"
        mock_args.n_sessions = 1
        mock_args.delay = 0
        mock_args.log_level = "INFO"
        mock_args.scrape_replacements = True
        mock_parse_args.return_value = mock_args
        
        scrape_ufcstats_data.main(None)
        mock_scraper.fighter_scraper.scrape_fighters.assert_called_once()
        mock_scraper.event_scraper.scrape_events.assert_called_once()
        mock_scraper.fight_scraper.scrape_fights.assert_called_once()
        if hasattr(mock_scraper, "replacement_scraper"):
            if hasattr(mock_scraper.replacement_scraper, "scrape_replacements"):
                mock_scraper.replacement_scraper.scrape_replacements.assert_called_once()

if __name__ == "__main__": # pragma: no cover
    unittest.main()
