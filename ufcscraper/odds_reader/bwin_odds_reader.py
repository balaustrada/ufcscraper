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

        # If the page is a JS-rendered SPA (no ms-event-group in static HTML),
        # try a Selenium fallback to render the page and obtain the DOM.
        if not tables:
            # Try to find a canonical URL in the HTML to render the live page.
            canonical = soup.find("link", rel="canonical")
            page_url = (
                canonical["href"] if canonical and canonical.get("href") else None
            )

            if page_url is None:
                logger.info(
                    "No ms-event-group found and no canonical URL available to render JS."
                )
            else:
                logger.info(
                    "No ms-event-group found in static HTML. Attempting Selenium render of %s",
                    page_url,
                )

                try:
                    # Import lazily to avoid hard dependency when not needed
                    from selenium import webdriver
                    from selenium.webdriver.chrome.options import Options
                    from selenium.webdriver.chrome.service import Service
                    from webdriver_manager.chrome import ChromeDriverManager
                    from selenium.webdriver.support.ui import WebDriverWait
                    from selenium.webdriver.support import expected_conditions as EC
                    from selenium.webdriver.common.by import By
                except Exception as e:
                    logger.error(
                        "Selenium/webdriver-manager not available; cannot render JS for bwin page: %s",
                        e,
                    )
                    tables = []
                else:
                    options = Options()
                    options.add_argument("--headless=new")
                    options.add_argument("--no-sandbox")
                    options.add_argument("--disable-dev-shm-usage")
                    options.add_argument("--disable-gpu")

                    driver = None
                    try:
                        driver = webdriver.Chrome(
                            service=Service(ChromeDriverManager().install()),
                            options=options,
                        )
                        driver.get(page_url)

                        # Wait up to 15s for ms-event-group to appear in the rendered DOM
                        try:
                            WebDriverWait(driver, 15).until(
                                lambda d: d.find_elements(By.TAG_NAME, "ms-event-group")
                            )
                        except Exception:
                            logger.info(
                                "Timed out waiting for ms-event-group; continuing with whatever was rendered."
                            )

                        rendered = driver.page_source
                        soup = BeautifulSoup(rendered, "lxml")
                        tables = soup.find_all("ms-event-group")
                        logger.info(
                            "Selenium render yielded %d ms-event-group elements",
                            len(tables),
                        )
                    except Exception as e:
                        logger.error("Selenium render failed: %s", e)
                    finally:
                        try:
                            if driver:
                                driver.quit()
                        except Exception:
                            pass

        rows_to_add = []
        for table in tables:
            # Try several candidate tags for the competition header; if none
            # exist, fallback to the table text snippet.
            header = (
                table.find("ms-league-header")
                or table.find("ms-competition-group-details")
                or table.find("ms-competition")
                or table.find("ms-competition-name")
            )

            if header is not None:
                header_text = header.get_text(" ", strip=True)
            else:
                header_text = table.get_text(" ", strip=True)[:200]

            if "ufc" not in header_text.lower():
                logger.debug(
                    "Skipping non-UFC group (header snippet): %s", header_text[:120]
                )
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

                odds = [
                    o.get_text(strip=True) for o in fight.find_all("ms-font-resizer")
                ]

                datestr = fight.find("ms-prematch-timer").text

                for language in languages:
                    date = dateparser.parse(
                        datestr,
                        languages=[
                            language,
                        ],
                        locales=[
                            language,
                        ],
                        settings={
                            "PREFER_DATES_FROM": "future",
                            "RELATIVE_BASE": self.html_datetime,
                        },
                    )
                    if date is not None:
                        break
                else:
                    raise ValueError(f"Could not parse date from: {datestr}")

                rows_to_add.append(
                    (
                        date,
                        fighters[0],
                        fighters[1],
                        float(odds[0]),
                        float(odds[1]),
                    )
                )

        self.write_odds(rows_to_add)
