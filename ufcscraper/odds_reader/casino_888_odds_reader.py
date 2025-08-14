"""
This module provides a reader for casino888 odds data provided
as a HTML file.
    
Classes:
- `Casino888OddsReader`: A reader for Casino888 odds data.
"""

from __future__ import annotations

import csv
import dateparser
from locale import setlocale, LC_TIME
import logging
from datetime import datetime, time
from typing import TYPE_CHECKING

from bs4 import BeautifulSoup

from ufcscraper.odds_reader.base import OddsReader

if TYPE_CHECKING:
    from typing import Dict, List


logger = logging.getLogger(__name__)


class Casino888OddsReader(OddsReader):
    """
    A reader for BetWay odds data provided as a HTML file.
    """

    filename = "raw_odds/casino888_odds_raw.csv"

    def scrape_odds(self, languages: list[str] = ["es", "en"]) -> None:
        """
        Scrapes the odds data from the HTML file and saves it to a CSV file.
        """
        soup = BeautifulSoup(self.read_html(), "lxml")

        rows_to_add = []

        for table in soup.find_all("div", class_="tournamentEventsList"):
            date_elem = table.find("div", class_="schema-header__container").find("span")
            datestr = date_elem.text.strip()

            for language in languages:
                date = dateparser.parse(
                    datestr,
                    languages=[language,],
                    locales=[language,],
                    settings = {
                        "PREFER_DATES_FROM": "future",
                        "RELATIVE_BASE": self.html_datetime,
                    }
                )
                if date is not None:
                    break
            else:
                raise ValueError(f"Could not parse date from: {datestr}")

            for fight in table.find_all("div", class_="bet-card"):
                fighters = []
                odds = []
                
                for fighter in fight.find_all("span", class_="event-name__text"):
                    fighters.append(fighter.text.strip())

                for odd in fight.find_all("div", class_="bet-button-new"):
                    odds.append(float(odd.text))

                rows_to_add.append((
                    date,
                    fighters[0],
                    fighters[1],
                    odds[0],
                    odds[1]
                ))
        

        self.write_odds(rows_to_add)