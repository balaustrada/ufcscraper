"""
This module provides a reader for sportium odds data provided
as a HTML file.
    
Classes:
- `SportiumOddsReader`: A reader for Sportium odds data.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import dateparser
from bs4 import BeautifulSoup

from ufcscraper.odds_reader.base import OddsReader

if TYPE_CHECKING:
    from typing import Dict, List


logger = logging.getLogger(__name__)


class SportiumOddsReader(OddsReader):
    """
    A reader for BetWay odds data provided as a HTML file.
    """

    filename = "raw_odds/sportium_odds_raw.csv"

    def scrape_odds(self, languages: list[str] = ["es", "en"]) -> None:
        """
        Scrapes the odds data from the HTML file and saves it to a CSV file.
        """
        soup = BeautifulSoup(self.read_html(), "lxml")

        rows_to_add = []


        for table in soup.find_all("div", class_="ta-FlexPane ta-EventListItems"):
            for fight in table.find_all("div", class_="ta-FlexPane ta-EventListItem"):
                date_div = fight.find("div", class_="ta-Button")
                datestr = date_div.text.strip()

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
                
                fighters = []
                odds = []
                for fighter in fight.find_all("div", class_="ta-participantName"):
                    fighters.append(fighter.text.strip())
                for odd in fight.find_all("div", class_="ta-price_text"):
                    odds.append(float(odd.text.strip()))

                rows_to_add.append((
                    date, 
                    fighters[0],
                    fighters[1],
                    odds[0],
                    odds[1],
                ))

        self.write_odds(rows_to_add)