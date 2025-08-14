"""
This module provides a reader for betway odds data provided
as a HTML file.
    
Classes:
- `BetwayOddsReader`: A reader for Betway odds data.
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


class BetwayOddsReader(OddsReader):
    """
    A reader for BetWay odds data provided as a HTML file.
    """

    filename = "raw_odds/betway_odds_raw.csv"

    def scrape_odds(self, languages: list[str] = ["es", "en"]) -> None:
        """
        Scrapes the odds data from the HTML file and saves it to a CSV file.
        """
        soup = BeautifulSoup(self.read_html(), "lxml")
        table = soup.find_all("div", class_="event")

        rows_to_add = []
        for section in soup.find_all("section", {"data-testid": "event-table-section"}):        
            date = section.find("span", {"data-testid": "table-header-title"})
            
            for language in languages:
                date = dateparser.parse(
                    date.text,
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
                raise ValueError(f"Could not parse date from: {date.text}")

            for fight_section in section.find_all("div", {"data-testid": "table-section"}):
                fight_records = fight_section.find_all("span")

                rows_to_add.append((
                    date,
                    fight_records[0].text.strip(),
                    fight_records[3].text.strip(),
                    float(fight_records[5].text.strip().replace(',', '.')),
                    float(fight_records[7].text.strip().replace(',', '.')),   
                ))


        self.write_odds(rows_to_add)