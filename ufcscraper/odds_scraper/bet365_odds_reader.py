"""
This module provides a reader for Bet365 odds data provided
as a HTML file.

Classes:
- `Bet365OddsReader`: A reader for Bet365 odds data.
"""

from __future__ import annotations

import csv
from locale import setlocale, LC_TIME
import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Tuple

from fuzzywuzzy import fuzz
import pandas as pd
from bs4 import BeautifulSoup

from ufcscraper.base import BaseFileHandler, BaseHTMLReader
from ufcscraper.fighter_names import FighterNames
from ufcscraper.ufc_scraper import UFCScraper

if TYPE_CHECKING:
    from typing import Dict, List


logger = logging.getLogger(__name__)

class Bet365Odds(BaseFileHandler):
    """
    Class to handle Bet365 odds data associated with existing fights.
    """
    
    dtypes: Dict[str, type | pd.core.arrays.integer.Int64Dtype] = {
        "scrape_datetime": "datetime64[ns]",
        "fight_id": "datetime64[ns]",
        "fighter_id": str,
        "odds": float,
    }

    sort_fields = ["scrape_datetime", "fight_id", "fighter_id", "odds"]
    data = pd.DataFrame({col: pd.Series(dtype=dt) for col, dt in dtypes.items()})
    filename = "bet365_odds.csv"


    def consolidate_odds(self, max_date_diff_days: int = 3, min_match_score: int = 90) -> None:
        """
        Read raw Bet365 odds data and consolidate it into a structured format.

        Args:
            max_date_diff_days: Maximum allowed difference in days between fight date and event date
            min_match_score: Minimum fuzzy match score to consider a name match valid
        """
        scraper = UFCScraper(self.data_folder)
        odds = pd.read_csv(self.data_folder / "bet365_odds_raw.csv")

        fight_data = scraper.fight_scraper.data
        fighter_data = scraper.fighter_scraper.data
        event_data = scraper.event_scraper.data
    
        fighter_data["fighter_full_name"] = fighter_data["fighter_f_name"] + " " + fighter_data["fighter_l_name"]

        fight_data = pd.concat(
            [
                fight_data[["fight_id", "event_id", "fighter_1"]].rename(
                    columns={"fighter_1": "fighter_id"}
                ),
                fight_data[["fight_id", "event_id", "fighter_2"]].rename(
                    columns={"fighter_2": "fighter_id"}
                ),
            ]
        )

        data = (
            fight_data.merge(
                fighter_data[["fighter_id", "fighter_full_name"]],
                on="fighter_id",
            )
            .merge(event_data[["event_id", "event_date"]], on="event_id")
        )[[
            "fight_id",
            "fighter_id",
            "fighter_full_name",
            "event_date",
        ]]

        odds = pd.concat(
            [
                odds[["html_datetime", "fight_date", "fighter_name", "fighter_odds"]],
                odds[["html_datetime", "fight_date", "opponent_name", "opponent_odds"]].rename(
                    columns={"opponent_name": "fighter_name", "opponent_odds": "fighter_odds"}
                ),
            ]
        )

        odds["fight_date"] = pd.to_datetime(odds["fight_date"])

        # Map fight_dates to valid event_dates
        unique_event_date = data["event_date"].unique()
        date_mapping = {}
        unmatched_dates = set()
        for odd_date in pd.to_datetime(odds["fight_date"].unique()):
            closest = min(unique_event_date, key=lambda d: abs(odd_date - d))
            distance = abs(odd_date - closest).days

            if distance <= max_date_diff_days:
                date_mapping[odd_date] = closest
            else:
                unmatched_dates.add(odd_date)

        for unmatched_date in sorted(unmatched_dates):
            logger.warning(f"Unmatched fight at date {unmatched_date}")

        odds["fight_date"] = odds["fight_date"].map(date_mapping)
        odds = odds.rename(columns={"fight_date": "event_date"})

        # Create index to select best match
        odds = odds.reset_index().rename(columns={"index": "odds_row_id"})

        # Merge odds with fight data
        merged = odds.merge(
            data,
            on="event_date",
        )
        # Compute fuzzy match score
        merged["match_score"] = merged.apply(
            lambda row: fuzz.token_set_ratio(row["fighter_name"], row["fighter_full_name"]),
            axis=1,
        )
        best_matches = merged.loc[merged.groupby("odds_row_id")["match_score"].idxmax()]

        below_threshold = best_matches["match_score"] < min_match_score
        for _, row in best_matches[below_threshold].iterrows():
            logger.warning(
                f"Low match score ({row['match_score']}) for '{row['fighter_name']}' vs '{row['fighter_full_name']}'"
            )

        final_data = best_matches[["html_datetime", "fight_id", "fighter_id", "fighter_odds"]].rename(
            columns={
                "html_datetime": "scrape_datetime",
                "fighter_odds": "odds",
            }
        )

        final_data["scrape_datetime"] = pd.to_datetime(final_data["scrape_datetime"])

        final_data.to_csv(self.data_file, index=False)
        self.remove_duplicates_from_file()
        logger.info(f"Consolidated Bet365 odds data saved to {self.data_file}")

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
    filename = "bet365_odds_raw.csv"

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

    def scrape_odds(self, locales:list[str] = ['en_US.utf8']) -> None:
        """
        Scrapes the odds data from the HTML file and saves it to a CSV file.
        """
        soup = BeautifulSoup(self.read_html(), "lxml")
        table = soup.find_all("div", class_="gl-MarketGroupContainer")[-1]
        rows = table.find_all("div", recursive=False)

        database_length = len(self.data)

        fights: Dict[datetime, List[List[str]]] = {}
        for elem in rows[0].find_all("div", recursive=False):
            if not elem.text:
                continue

            elif "rcl-MarketHeaderLabel" in elem.get("class"):
                # Handle date header
                datestr = elem.text

                # Correct format adding year
                if len(datestr.split(" ")) == 3:
                    datestr += " " + str(self.html_datetime.year)
                elif len(datestr.split(" ")) == 4:
                    pass
                else:
                    raise ValueError("Read invalid date format: ", datestr)

                date = None
                for loc in locales:
                    try:
                        setlocale(LC_TIME, loc)
                        date = datetime.strptime(datestr, "%a %d %b %Y")
                        logger.debug(f"Parsed date '{datestr}' with locale '{loc}'")
                    except ValueError:
                        pass
                if date is None:
                    raise ValueError(f"Could not parse date '{datestr}' with any locale: {locales}")

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

                if not date:
                    raise ValueError("No date found for fighters: ", fighters)
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
        
        # Prepare rows to be added
        rows_to_add = []
        for date in fights.keys():
            for fight, odds in zip(fights[date], odds_dict[date]):
                fighter, opponent = fight
                fighter_odds, opponent_odds = odds
                row = [
                    self.html_datetime.strftime("%Y-%m-%d %H:%M:%S"),
                    date.strftime("%Y-%m-%d"),
                    fighter,
                    opponent,
                    fighter_odds,
                    opponent_odds,
                ]
                rows_to_add.append(row)

        logger.info(f"Rows to be written: {len(rows_to_add)}")

        with open(self.data_file, "a") as file:
            writer = csv.writer(file)
            for row in rows_to_add:
                writer.writerow(row)

        self.remove_duplicates_from_file()
        self.load_data()
        logger.info(f"Rows added to database: {len(self.data) - database_length}")