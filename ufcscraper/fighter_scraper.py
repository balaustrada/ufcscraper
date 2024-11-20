"""
This module defines a `FighterScraper` class for scraping and processing fighter
data from UFCStats.

The `FighterScraper` class inherits from the `BaseScraper` class and is designed
to retrieve detailed information about UFC fighters, including personal details, 
physical attributes, and fight records. The scraped data is processed and saved 
into a CSV file for later analysis. The module also provides methods for parsing 
and converting specific attributes like height, weight, reach, and more from the
scraped HTML content.
"""

from __future__ import annotations

import csv
import datetime
import logging
from typing import TYPE_CHECKING

import pandas as pd

from ufcscraper.base import BaseScraper
from ufcscraper.utils import links_to_soups

if TYPE_CHECKING:  # pragma: no cover
    import bs4
    from typing import Dict, List

logger = logging.getLogger(__name__)


class FighterScraper(BaseScraper):
    """Scrapes and stores fighter data from UFCStats.

    This class handles scraping fighter details from UFCStats, including
    personal information, physical attributes, and fight records. The data
    is saved to a CSV file for further analysis.
    """

    dtypes: Dict[str, type | pd.core.arrays.integer.Int64Dtype] = {
        "fighter_id": str,
        "fighter_f_name": str,
        "fighter_l_name": str,
        "fighter_nickname": str,
        "fighter_height_cm": float,
        "fighter_weight_lbs": float,
        "fighter_reach_cm": float,
        "fighter_stance": str,
        "fighter_dob": "datetime64[ns]",
        "fighter_w": pd.Int64Dtype(),
        "fighter_l": pd.Int64Dtype(),
        "fighter_d": pd.Int64Dtype(),
        "fighter_nc_dq": pd.Int64Dtype(),
    }
    sort_fields = ["fighter_l_name", "fighter_f_name", "fighter_id"]
    data = pd.DataFrame({col: pd.Series(dtype=dt) for col, dt in dtypes.items()})
    filename = "fighter_data.csv"

    @classmethod
    def url_from_id(cls, id_: str) -> str:
        """Constructs the URL for a fighter's details page based on their ID.

        Args:
            id_: The fighter's unique identifier.

        Returns:
            The URL for the fighter's details page.
        """
        return f"{cls.web_url}/fighter-details/{id_}"

    def scrape_fighters(self) -> None:
        """Scrapes fighter details from URLs and saves the data to a CSV file.

        This method retrieves fighter URLs, scrapes details from each URL,
        and appends the data to the CSV file. Handles errors and logs progress.
        """
        existing_urls = set(map(self.url_from_id, self.data["fighter_id"]))
        ufcstats_fighter_urls = self.get_fighter_urls()
        urls_to_scrape = set(ufcstats_fighter_urls) - existing_urls

        logger.info(f"Scraping {len(urls_to_scrape)} fighters...")

        with open(self.data_file, "a+") as f:
            writer = csv.writer(f)

            for i, (url, soup) in enumerate(
                links_to_soups(list(urls_to_scrape), self.n_sessions, self.delay)
            ):
                try:
                    name = soup.select("span")[0].text.split()
                    nickname = soup.select("p.b-content__Nickname")[0]
                    details = soup.select("li.b-list__box-list-item")
                    record = (
                        soup.select("span.b-content__title-record")[0]
                        .text.split(":")[1]
                        .strip()
                        .split("-")
                    )

                    f_name = name[0].strip()
                    l_name = self.parse_l_name(name).strip()
                    nickname_str = self.parse_nickname(nickname).strip()
                    height = self.parse_height(details[0])
                    weight = self.parse_weight(details[1])
                    reach = self.parse_reach(details[2])
                    stance = self.parse_stance(details[3])
                    dob = self.parse_dob(details[4])
                    w = record[0]
                    l = record[1]
                    d = record[-1][0] if len(record[-1]) > 1 else record[-1]
                    nc_dq = record[-1].split("(")[-1][0] if len(record[-1]) > 1 else ""

                    writer.writerow(
                        [
                            self.id_from_url(url),
                            f_name,
                            l_name,
                            nickname_str,
                            height,
                            weight,
                            reach,
                            stance,
                            dob,
                            w,
                            l,
                            d,
                            nc_dq,
                        ]
                    )

                    logger.info(f"Scraped {i+1}/{len(urls_to_scrape)} fighters...")
                except Exception as e:
                    logger.error(f"Error saving data from url: {url}\nError: {e}")

        self.remove_duplicates_from_file()

    def add_name_column(self) -> None:
        """
        Adds a combined name column to the DataFrame.

        The new column is created by concatenating the fighter's first
            and last names.
        """
        self.data["fighter_name"] = (
            self.data["fighter_f_name"] + " " + self.data["fighter_l_name"].fillna("")
        ).str.strip()

    def get_fighter_urls(self) -> List[str]:
        """
        Retrieves the URLs for fighter profiles.

        Returns:
            A list of URLs to fighter profiles.
        """
        logger.info("Scraping fighter links...")

        # Search fighters by letter
        urls = [
            f"{self.web_url}/statistics/fighters?char={letter}&page=all"
            for letter in "abcdefghijklmnopqrstuvwxyz"
        ]

        soups = [result[1] for result in links_to_soups(urls, self.n_sessions)]

        # Collect fighter URLs from each page
        fighter_urls = []
        for soup in soups:
            if soup is not None:
                for link in soup.select("a.b-link")[1::3]:
                    fighter_urls.append(str(link.get("href")))

        logger.info(f"Got {len(fighter_urls)} urls...")
        return fighter_urls

    @staticmethod
    def parse_l_name(name: List[str]) -> str:
        """
        Parses the last name from a list of name parts.

        Args:
            name: List of name parts.

        Returns:
            The parsed last name, or "" if it cannot be determined.
        """
        if len(name) == 2:
            return name[-1]
        elif len(name) == 1:
            return ""
        elif len(name) == 3:
            return name[-2] + " " + name[-1]
        elif len(name) == 4:
            return name[-3] + " " + name[-2] + " " + name[-1]
        else:
            return ""

    @staticmethod
    def parse_nickname(nickname: bs4.element.Tag) -> str:
        """
        Parses the fighter's nickname.

        Args:
            nickname: BeautifulSoup tag containing the nickname.

        Returns:
            The parsed nickname, or "" if not available.
        """
        if nickname.text == "\n":
            return ""
        else:
            return nickname.text.strip()

    @staticmethod
    def parse_height(height: bs4.element.Tag) -> str:
        """
        Parses and converts fighter's height from feet and inches to cm.

        Args:
            height: BeautifulSoup tag containing the height in feet and inches.

        Returns:
            The height in centimeters, or "" if not available.
        """
        height_text = height.text.split(":")[1].strip()
        if "--" in height_text.split("'"):
            return ""
        else:
            height_ft = int(height_text[0])
            height_in = int(height_text.split("'")[1].strip().strip('"'))
            height_cm = (height_ft * 12.0 * 2.54) + (height_in * 2.54)
            return str(height_cm)

    @staticmethod
    def parse_reach(reach: bs4.element.Tag) -> str:
        """
        Parses and converts fighter's reach from inches to cm.

        Args:
            reach: BeautifulSoup tag containing the reach in inches.

        Returns:
            The reach in centimeters, or "" if not available.
        """
        reach_text = reach.text.split(":")[1]
        if "--" in reach_text:
            return ""
        else:
            return str(round(int(reach_text.strip().strip('"')) * 2.54, 2))

    @staticmethod
    def parse_weight(weight_element: bs4.element.Tag) -> str:
        """
        Parses the fighter's weight.

        Args:
            weight_element: BeautifulSoup tag containing the weight.

        Returns:
            The weight in pounds, or "" if not available.
        """
        weight_text = weight_element.text.split(":")[1]
        if "--" in weight_text:
            return ""
        else:
            return weight_text.split()[0].strip()

    @staticmethod
    def parse_stance(stance: bs4.element.Tag) -> str:
        """
        Parses the fighter's stance.

        Args:
            stance: BeautifulSoup tag containing the stance.

        Returns:
            The stance, or "" if not available.
        """
        stance_text = stance.text.split(":")[1]
        if stance_text == "":
            return ""
        else:
            return stance_text.strip()

    @staticmethod
    def parse_dob(dob: bs4.element.Tag) -> str:
        """
        Parses and formats the fighter's date of birth.

        Args:
            dob: BeautifulSoup tag containing the date of birth.

        Returns:
            The date of birth in YYYY-MM-DD format, or "" if not available.
        """
        dob_text = dob.text.split(":")[1].strip()
        if dob_text == "--":
            return ""
        else:
            return str(datetime.datetime.strptime(dob_text, "%b %d, %Y"))[0:10]
