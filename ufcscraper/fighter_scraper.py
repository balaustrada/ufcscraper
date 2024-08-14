"""
    Module to scrape and handle fighter data
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
    from typing import List

logger = logging.getLogger(__name__)


class FighterScraper(BaseScraper):
    columns: List[str] = [
        "fighter_id",
        "fighter_f_name",
        "fighter_l_name",
        "fighter_nickname",
        "fighter_height_cm",
        "fighter_weight_lbs",
        "fighter_reach_cm",
        "fighter_stance",
        "fighter_dob",
        "fighter_w",
        "fighter_l",
        "fighter_d",
        "fighter_nc_dq",
    ]
    data = pd.DataFrame(columns=columns)
    filename = "fighter_data.csv"

    @classmethod
    def url_from_id(cls, id_: str) -> str:
        return f"{cls.web_url}/fighter-details/{id_}"

    def scrape_fighters(self) -> None:
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
                    nc_dq = (
                        record[-1].split("(")[-1][0] if len(record[-1]) > 1 else "NULL"
                    )

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

    def add_name_column(self) -> None:
        """
        Add to data name column as in UFCStats.
        """
        self.data["fighter_name"] = (
            self.data["fighter_f_name"] + " " + self.data["fighter_l_name"].fillna("")
        ).str.strip()

    def get_fighter_urls(self) -> List[str]:
        """
        Get the urls of the fighters.

        :return: The urls of the fighters.
        """
        logger.info("Scraping fighter links...")

        # We search fighters by letter
        urls = [
            f"{self.web_url}/statistics/fighters?char={letter}&page=all"
            for letter in "abcdefghijklmnopqrstuvwxyz"
        ]

        soups = [result[1] for result in links_to_soups(urls, self.n_sessions)]

        # Now we iterate over each page and scrape fighter links
        fighter_urls = []
        for soup in soups:
            if soup is not None:
                for link in soup.select("a.b-link")[1::3]:
                    fighter_urls.append(str(link.get("href")))

        logger.info(f"Got {len(fighter_urls)} urls...")
        return fighter_urls

    @staticmethod
    def parse_l_name(name: List[str]) -> str:
        if len(name) == 2:
            return name[-1]
        elif len(name) == 1:
            return "NULL"
        elif len(name) == 3:
            return name[-2] + " " + name[-1]
        elif len(name) == 4:
            return name[-3] + " " + name[-2] + " " + name[-1]
        else:
            return "NULL"

    @staticmethod
    def parse_nickname(nickname: bs4.element.Tag) -> str:
        if nickname.text == "\n":
            return "NULL"
        else:
            return nickname.text.strip()

    @staticmethod
    def parse_height(height: bs4.element.Tag) -> str:
        # Converts height in feet/inches to height in cm
        height_text = height.text.split(":")[1].strip()
        if "--" in height_text.split("'"):
            return "NULL"
        else:
            height_ft = height_text[0]
            height_in = height_text.split("'")[1].strip().strip('"')
            height_cm = ((int(height_ft) * 12.0) * 2.54) + (int(height_in) * 2.54)
            return str(height_cm)

    @staticmethod
    def parse_reach(reach: bs4.element.Tag) -> str:
        # Converts reach in inches to reach in cm
        reach_text = reach.text.split(":")[1]
        if "--" in reach_text:
            return "NULL"
        else:
            return str(round(int(reach_text.strip().strip('"')) * 2.54, 2))

    @staticmethod
    def parse_weight(weight_element: bs4.element.Tag) -> str:
        weight_text = weight_element.text.split(":")[1]
        if "--" in weight_text:
            return "NULL"
        else:
            return weight_text.split()[0].strip()

    @staticmethod
    def parse_stance(stance: bs4.element.Tag) -> str:
        stance_text = stance.text.split(":")[1]
        if stance_text == "":
            return "NULL"
        else:
            return stance_text.strip()

    @staticmethod
    def parse_dob(dob: bs4.element.Tag) -> str:
        # Converts string containing date of birth to datetime object
        dob_text = dob.text.split(":")[1].strip()
        if dob_text == "--":
            return "NULL"
        else:
            return str(datetime.datetime.strptime(dob_text, "%b %d, %Y"))[0:10]
