from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from ufcscraper.fighter_names import FighterNames
from ufcscraper.odds_reader import Odds


class TestFighterNames(unittest.TestCase):
    def test_check_missing_records_only_writes_missing_fighters_once(self) -> None:
        ufc_stats_data = pd.DataFrame(
            {
                "fighter_id": ["fighter-1", "fighter-1", "fighter-2", "fighter-2"],
                "fighter_name": ["First Fighter", "First Fighter", "Second Fighter", "Second Fighter"],
            }
        )

        with tempfile.TemporaryDirectory() as data_folder:
            fighter_names = FighterNames(data_folder)
            fighter_names.data = pd.DataFrame(
                [["fighter-1", "UFCStats", "First Fighter", "fighter-1"]],
                columns=fighter_names.dtypes,
            )
            fighter_names.data.to_csv(fighter_names.data_file, index=False)

            with patch.object(
                fighter_names, "get_ufcstats_data", return_value=ufc_stats_data
            ):
                fighter_names.check_missing_records()
                first_result = Path(fighter_names.data_file).read_bytes()
                fighter_names.check_missing_records()

            self.assertEqual(Path(fighter_names.data_file).read_bytes(), first_result)
            data = pd.read_csv(fighter_names.data_file, dtype=str)
            self.assertEqual(
                data.to_dict("records"),
                [
                    {
                        "fighter_id": "fighter-1",
                        "database": "UFCStats",
                        "name": "First Fighter",
                        "database_id": "fighter-1",
                    },
                    {
                        "fighter_id": "fighter-2",
                        "database": "UFCStats",
                        "name": "Second Fighter",
                        "database_id": "fighter-2",
                    },
                ],
            )

    def test_add_fighter_names_reloads_new_aliases(self) -> None:
        with tempfile.TemporaryDirectory() as data_folder:
            odds = Odds(data_folder)
            odds.fighter_names.data = pd.DataFrame(
                [["fighter-1", "UFCStats", "First Fighter", "fighter-1"]],
                columns=odds.fighter_names.dtypes,
            )
            odds.fighter_names.data.to_csv(odds.fighter_names.data_file, index=False)
            raw_odds_folder = Path(data_folder) / "raw_odds"
            raw_odds_folder.mkdir()
            pd.DataFrame(
                {
                    "fighter_name": ["First Fighter"],
                    "opponent_name": ["First Fighter"],
                }
            ).to_csv(raw_odds_folder / "bet365_odds_raw.csv", index=False)

            with patch.object(odds.fighter_names, "check_missing_records"):
                odds.add_fighter_names("Bet365")

            self.assertEqual(
                odds.fighter_names.check_fighter_id("First Fighter", "Bet365"),
                "fighter-1",
            )
