import pandas as pd
import logging
import unittest
from unittest.mock import patch, MagicMock

from ufcscraper.scripts import check_missing_records

def make_fight_data():
    return pd.DataFrame({
        "fight_id": ["f1", "f2", "f3"],
        "fighter_1": ["id1", "id2", "id3"],
        "fighter_2": ["id4", "id5", "id6"],
        "event_id": ["e1", "e2", "e3"],
        "weight_class": ["Bantamweight", "Catchweight", "InvalidWeight"],
        # "fighter_id": ["id1", "id2", "id3"],  # for merge/join
    })

def make_event_data():
    return pd.DataFrame({
        "event_id": ["e1", "e2", "e3"],
        "event_date": [pd.Timestamp("2025-07-01"), pd.Timestamp("2025-07-02"), pd.Timestamp("2025-07-03")],
    })

def make_bfo_data():
    return pd.DataFrame({
        "fight_id": ["f1", "f2", "f3"],
        "fighter_id": ["id1", "id2", "id3"],
        "opening": [None, 1.5, None],
        "closing_range_min": [None, 1.2, 1.3],
        "closing_range_max": [None, 1.7, None],
    })

def make_catch_weights_data():
    return pd.DataFrame({
        "fight_id": ["f1", "f2", "f3"],
        "catch_weight": [None, 145, None],
    })

class DummyScraper:
    def __init__(self):
        self.fight_scraper = MagicMock()
        self.fight_scraper.data = make_fight_data()
        self.event_scraper = MagicMock()
        self.event_scraper.data = make_event_data()
        self.catch_weights = MagicMock()
        self.catch_weights.data = make_catch_weights_data()

class DummyBfoScraper:
    def __init__(self):
        self.data = make_bfo_data()

class TestCheckMissingRecords(unittest.TestCase):
    @patch("ufcscraper.scripts.check_missing_records.UFCScraper", new=lambda data_folder: DummyScraper())
    @patch("ufcscraper.scripts.check_missing_records.BestFightOddsScraper", new=lambda data_folder, n_sessions: DummyBfoScraper())
    def test_main_missing_odds_and_weights(self):
        with self.assertLogs(check_missing_records.logger, level="WARNING") as cm:
            args = MagicMock()
            args.data_folder = "dummy"
            args.log_level = "WARNING"
            check_missing_records.main(args)
        log_text = "\n".join(cm.output)
        self.assertIn("Found", log_text)
        self.assertTrue("missing odds" in log_text or "invalid catch weights" in log_text)

    def test_get_args_defaults(self):
        with patch("argparse.ArgumentParser.parse_args", return_value=MagicMock(log_level="WARNING", data_folder="dummy")):
            args = check_missing_records.get_args()
            self.assertTrue(hasattr(args, "log_level"))
            self.assertTrue(hasattr(args, "data_folder"))