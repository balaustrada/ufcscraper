"""
This module provides a reader for Bet365 odds data provided
as a HTML file.

Classes:
- `Bet365OddsReader`: A reader for Bet365 odds data.
"""

from __future__ import annotations

import csv
from fuzzywuzzy import process, fuzz
from pathlib import Path
import logging
from bs4 import BeautifulSoup
import pandas as pd
from typing import TYPE_CHECKING
from datetime import datetime

from tomlkit import table

from ufcscraper.base import BaseHTMLReader
from ufcscraper.fighter_names import FighterNames

if TYPE_CHECKING:
    from typing import Dict


logger = logging.getLogger(__name__)


class Bet365OddsReader(BaseHTMLReader):
    """
    A reader for Bet365 odds data provided as a HTML file.
    """

    dtypes: Dict[str, type | pd.core.arrays.integer.Int64Dtype] = {
        "html_datetime": "datetime64[ns]",
        "fight_date": "datetime64[ns]",
        "fighter_name": str,
        "opponent_name": str,
        "fighter_odds": float,
        "opponent_odds": float,
    }

    sort_fields = ["html_datetime", "fight_date", "fighter_name", "opponent_name", "fighter_odds", "opponent_odds"]
    data = pd.DataFrame({col: pd.Series(dtype=dt) for col, dt in dtypes.items()})
    filename = "bet365_odds.csv"

    def __init__(self, html_file: Path | str, data_folder: Path | str):
        """
        Initializes the Bet365OddsReader with the specified data folder.

        Args:
            html_file (Path | str): The path to the HTML file containing the odds data.
            data_folder (Path | str): The folder where the CSV file is stored
            or will be created.
        """
        super().__init__(html_file=html_file, data_folder=data_folder)
        self.fighter_names = FighterNames(data_folder)
        # self._load_fighter_names_data()

    # def _load_fighter_names_data(self) -> None:
    #     """
    #     Loads fighter names from the fighter_names handler.
    #     """
    #     self.fighter_names.load_data()
    #     self.bet365_names = self.fighter_names.data[
    #         self.fighter_names.data["database"] == "bet365"
    #     ]
    #     self.ufcstats_names = self.fighter_names.data[
    #         self.fighter_names.data["database"] == "UFCStats"
    #     ]

    # def _get_fighter_id(self, name: str) -> str:
    #     """
    #     Retrieves the fighter ID for a given fighter name.

    #     Args:
    #         name (str): The name of the fighter.

    #     Returns:
    #         str: The fighter ID, or an empty string if not found.
    #     """
    #     fighter_id = self.fighter_names.check_fighter_id(name, "bet365")

    #     if not fighter_id:
    #         # If fighter not in database, try to find a close match in UFCStats
    #         best_name, score = process.extractOne(
    #             name,
    #             self.ufcstats_names["name"].tolist(),
    #             scorer=fuzz.token_sort_ratio,
    #         )
    #         if score > 90:
    #             row = self.ufcstats_names[self.ufcstats_names["name"] == best_name].iloc[0]
    #             return row["fighter_id"]
    #         else:
    #             logger.warning(f"Fighter ID not found for name: {name}")
    #     else:
    #         return fighter_id

    def scrape_odds(self) -> None:
        """
        Scrapes the odds data from the HTML file and saves it to a CSV file.
        """
        soup = BeautifulSoup(self.read_html(), "lxml")
        table = soup.find_all("div", class_="gl-MarketGroupContainer")[-1]
        rows = table.find_all("div", recursive=False)

        fights = {}
        for elem in rows[0].find_all("div", recursive=False):
            if not elem.text:
                continue

            elif "rcl-MarketHeaderLabel" in elem.get("class"):
                # Handle date header
                datestr = elem.text

                # Correct format adding year
                if len(datestr.split(" ")) == 3:
                    datestr += "  " + str(self.html_datetime.year)
                elif len(datestr.split(" ")) == 4:
                    pass
                else:
                    raise ValueError("Read invalid date format: ", datestr)
                date = datetime.strptime(datestr, "%a %d %b %Y")

                # If fight date month is lower than the HTML datetime month,
                # it means the fight is in the next year.
                if self.html_datetime.month > date.month:
                    date = date.replace(year=self.html_datetime.year + 1)

                fights[date] = []

            else:
                fighters = []
                for fighter in elem.find_all(
                    "div", class_="src-ParticipantFixtureDetailsHigher_TeamWrapper"
                ):
                    fighters.append(fighter.text.strip())
                fights[date].append(fighters)

        odds = []
        for odd in soup.find_all("span", class_="src-ParticipantOddsOnly50_Odds"):
            odds.append(odd.text.strip())

        odds = [odds[i : i + 2] for i in range(0, len(odds), 2)]

        odds_dict = {}
        i = 0
        for key, val in fights.items():
            n = len(val)
            odds_dict[key] = odds[i : i + n]
            i += n

        with open(self.data_file, "a") as file:
            writer = csv.writer(file)

            for date in fights.keys():
                for fight, odds in zip(fights[date], odds_dict[date]):
                    fighter, opponent = fight
                    fighter_odds, opponent_odds = odds
                    writer.writerow(
                        [
                            self.html_datetime.strftime("%Y-%m-%d %H:%M:%S"),
                            date.strftime("%Y-%m-%d"),
                            fighter,
                            opponent,
                            fighter_odds,
                            opponent_odds,
                        ]
                    )

        self.remove_duplicates_from_file()
