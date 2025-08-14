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
from typing import TYPE_CHECKING

from bs4 import BeautifulSoup
import dateparser

from ufcscraper.odds_reader.base import OddsReader

if TYPE_CHECKING:
    from typing import Dict, List


logger = logging.getLogger(__name__)

class Bet365OddsReader(OddsReader):
    """
    A reader for Bet365 odds data provided as a HTML file.
    """

    filename = "raw_odds/bet365_odds_raw.csv"

    def scrape_odds(self, languages: list[str] = ["es", "en"]) -> None:
        """
        Scrapes the odds data from the HTML file and saves it to a CSV file.
        """
        soup = BeautifulSoup(self.read_html(), "lxml")
        table = soup.find_all("div", class_="gl-MarketGroupContainer")[-1]
        rows = table.find_all("div", recursive=False)

        fights: Dict[datetime, List[List[str]]] = {}
        for elem in rows[0].find_all("div", recursive=False):
            if not elem.text:
                continue

            elif "rcl-MarketHeaderLabel" in elem.get("class"):
                # Handle date header
                datestr = elem.text

                date = dateparser.parse(
                    datestr,
                    languages=languages,
                    settings = {
                        "PREFER_DATES_FROM": "future",
                        "RELATIVE_BASE": self.html_datetime,
                    }
                )

                if date is None:
                    raise ValueError(f"Could not parse date from: {datestr}")

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
                row = (
                    date,
                    fighter,
                    opponent,
                    fighter_odds,
                    opponent_odds,
                )
                rows_to_add.append(row)


        self.write_odds(rows_to_add)