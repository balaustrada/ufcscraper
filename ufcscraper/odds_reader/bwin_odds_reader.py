"""
This module provides a reader for bwin odds data provided
as a HTML file.
    
Classes:
- `BwinOddsReader`: A reader for Bwin odds data.
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


class BwinOddsReader(OddsReader):
    """
    A reader for BetWay odds data provided as a HTML file.
    """

    filename = "raw_odds/bwin_odds_raw.csv"

    def scrape_odds(self, languages: list[str] = ["es", "en"]) -> None:
        """
        Scrapes the odds data from the HTML file and saves it to a CSV file.
        """
        soup = BeautifulSoup(self.read_html(), "lxml")
        tables = soup.find_all("ms-event-group")

        rows_to_add = []
        for table in tables:
            header = table.find("ms-league-header")
            
            if "UFC" not in header.text:
                continue

            for fight in table.find_all("ms-event"):
                fight_url = fight.find("a")["href"]

                # Extract fighter names from URL
                slug = urlparse(fight_url).path.split("/")[-1]
                slug = re.sub(r"-\d+$", "", slug)
                # Remove trailing country code from the last fighter
                slug = re.sub(r"-[a-z]{3}$", "", slug)
                # Now split on country codes
                parts = re.split(r"-[a-z]{3}-", slug)
                # Clean names
                fighters = [p.replace("-", " ").title() for p in parts]
                
                odds = [o.get_text(strip=True) for o in fight.find_all("ms-font-resizer")]

                datestr = fight.find("ms-prematch-timer").text

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

                rows_to_add.append((
                    date,
                    fighters[0],
                    fighters[1],
                    float(odds[0]),
                    float(odds[1]),
                ))
                

        self.write_odds(rows_to_add)