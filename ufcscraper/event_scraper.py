"""
This module contains the `EventScraper` class, which is responsible for scraping
event data from the UFCStats website.

The `EventScraper` class inherits from `BaseScraper` and provides functionality
to retrieve and process event details such as event name, date, city, state, and 
country. The scraped data is stored in a CSV file (`event_data.csv`) and can be 
used for further analysis.
"""

from __future__ import annotations

import csv
import datetime
import logging
from typing import TYPE_CHECKING

import pandas as pd

from ufcscraper.base import BaseScraper
from ufcscraper.utils import link_to_soup, links_to_soups

if TYPE_CHECKING:  # pragma: no cover
    from typing import Dict, List

logger = logging.getLogger(__name__)


class EventScraper(BaseScraper):
    """Scrapes event data from the UFCStats website.

    This class handles scraping event details such as event name, date, city,
    state, and country, and stores them in a CSV file. It inherits basic
    scraping functionality from `BaseScraper`.
    """

    dtypes: Dict[str, type | pd.core.arrays.integer.Int64Dtype] = {
        "event_id": str,
        "event_name": str,
        "event_date": "datetime64[ns]",
        "event_city": str,
        "event_state": str,
        "event_country": str,
    }
    sort_fields = ["event_date", "event_name"]
    data = pd.DataFrame({col: pd.Series(dtype=dt) for col, dt in dtypes.items()})
    filename = "event_data.csv"

    @classmethod
    def url_from_id(cls, id_: str) -> str:
        """Constructs the event URL using the event ID.

        Args:
            id_: The unique identifier for the event.

        Returns:
            The full URL to the event's details page on UFCStats.
        """
        return f"{cls.web_url}/event-details/{id_}"

    def scrape_events(self) -> None:
        """Scrapes event data and saves it to a CSV file.

        This method compares existing event URLs with those available on the
        UFCStats website, scrapes details of new events, and appends them to
        the CSV file. Logs the progress and any errors encountered.
        """
        existing_urls = set(map(self.url_from_id, self.data["event_id"]))
        ufcstats_event_urls = self.get_event_urls()
        urls_to_scrape = set(ufcstats_event_urls) - existing_urls

        logger.info(f"Scraping {len(urls_to_scrape)} events...")

        with open(self.data_file, "a+") as f:
            writer = csv.writer(f)

            i = 0
            for i, (url, soup) in enumerate(
                links_to_soups(list(urls_to_scrape), self.n_sessions, self.delay)
            ):
                try:
                    full_location = (
                        soup.select("li")[4].text.split(":")[1].strip().split(",")
                    )
                    event_name = soup.select("h2")[0].text
                    event_date = str(
                        datetime.datetime.strptime(
                            soup.select("li")[3].text.split(":")[-1].strip(),
                            "%B %d, %Y",
                        )
                    )
                    event_city = full_location[0]
                    event_country = full_location[-1]

                    # Check event location contains state details
                    if len(full_location) > 2:
                        event_state = full_location[1]
                    else:
                        event_state = ""

                    writer.writerow(
                        [
                            self.id_from_url(url),
                            event_name.strip(),
                            event_date[0:10],
                            event_city.strip(),
                            event_state.strip(),
                            event_country.strip(),
                        ]
                    )

                    logger.info(f"Scraped {i+1}/{len(urls_to_scrape)} events...")
                except Exception as e:
                    logger.error(f"Error saving data from url: {url}\nError: {e}")

        self.remove_duplicates_from_file()

    def get_event_urls(self) -> List[str]:
        """Retrieves the URLs of all completed events from UFCStats.

        This method scrapes the UFCStats website for event URLs that contain
        the keyword 'event-details'. It returns a list of these URLs.

        Returns:
            A list of URLs for completed events.
        """
        logger.info("Scraping event links...")

        soup = link_to_soup(f"{self.web_url}/statistics/events/completed?page=all")

        # Adds href to list if href contains a link with keyword 'event-details'
        event_urls = [
            item.get("href")
            for item in soup.find_all("a")
            if type(item.get("href")) == str and "event-details" in item.get("href")
        ]

        logger.info(f"Got {len(event_urls)} event links...")
        return event_urls
