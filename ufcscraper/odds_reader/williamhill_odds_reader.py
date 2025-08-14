"""
This module provides a reader for Wiilliam Hill odds data provided
as a HTML file.

Classes:
- `WilliamHill5OddsReader`: A reader for William Hill odds data.
"""

from __future__ import annotations

import logging

from bs4 import BeautifulSoup
import dateparser

from ufcscraper.odds_reader.base import OddsReader

logger = logging.getLogger(__name__)


class WilliamHillOddsReader(OddsReader):
    """
    A reader for William Hill odds data provided as a HTML file.
    """

    filename = "raw_odds/williamhill_odds_raw.csv"

    def scrape_odds(self, languages: list[str] = ["es", "en"]) -> None:
        """
        Scrapes the odds data from the HTML file and saves it to a CSV file.
        """
        soup = BeautifulSoup(self.read_html(), "lxml")
        table = soup.find_all("div", class_="event")

        rows_to_add = []

        for row in table:
            time_tag = row.find("time", class_="eventStartTime localisable")
            datestr = time_tag.text.strip()

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

            # it means the fight is in the next year.
            if self.html_datetime.month > date.month:
                date = date.replace(year=self.html_datetime.year + 1)


            names = row.find(
                "div",
                class_="btmarket__link-name btmarket__link-name--ellipsis show-for-desktop-medium",
            )
            odds = row.find_all("div", class_="btmarket__selection")


            rows_to_add.append(
                (   
                    date,
                    names.text.split(' v ')[0].strip(),
                    names.text.split(' v ')[1].strip(),
                    float(odds[0].text.strip()),
                    float(odds[1].text.strip()),
                )
            )

        self.write_odds(rows_to_add)