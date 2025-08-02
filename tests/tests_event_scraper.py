from __future__ import annotations

import unittest
from pathlib import Path
from shutil import copy, rmtree
from unittest.mock import Mock, patch

import requests

from ufcscraper.event_scraper import *

THIS_DIR = Path(__file__).parent


def mock_get(url: str) -> str | None:
    mock = Mock()

    if url == "http://www.ufcstats.com/statistics/events/completed?page=all":
        page = Path(THIS_DIR / "test_files/htmls/event_search_page.html")
    elif url == "http://example.com/event-details/event1":
        page = Path(THIS_DIR / "test_files/htmls/event_page1.html")
    elif url == "http://example.com/event-details/event2":
        page = Path(THIS_DIR / "test_files/htmls/event_page2.html")
    elif url == "http://example.com/event-details/event3":
        page = Path(THIS_DIR / "test_files/htmls/event_page3.html")
    elif url == "http://example.com/event-details/fail":
        page = Path(THIS_DIR / "test_files/htmls/event_pagefail.html")
    else:
        page = Path(THIS_DIR / "test_files/htmls/empty_page.html")

    mock.text = Path(page).read_text()
    return mock


class TestEventScraper(unittest.TestCase):
    def setUp(self) -> None:
        Path(THIS_DIR / "test_files/run_files").mkdir(exist_ok=True)
        self.scraper = EventScraper(
            data_folder=THIS_DIR / "test_files/run_files",
            n_sessions=1,
            delay=0,
        )
        copy(
            THIS_DIR / "test_files/fighter_data.csv",
            THIS_DIR / "test_files/run_files/.",
        )

    def tearDown(self) -> None:
        rmtree(THIS_DIR / "test_files/run_files/")

    @patch.object(requests.Session, "get", side_effect=mock_get)
    def test_get_event_urls(self, mock_get: Mock) -> None:
        urls = self.scraper.get_event_urls()

        self.assertEqual(
            sorted(urls),
            [
                "http://example.com/event-details/event1",
                "http://example.com/event-details/event2",
                "http://example.com/event-details/event3",
                "http://example.com/event-details/fail",
                "http://example.com/event-details/none",
            ],
        )

    @patch.object(requests.Session, "get", side_effect=mock_get)
    def test_scrape_events(self, mock_get: Mock) -> None:
        with self.assertLogs("ufcscraper.event_scraper", level="ERROR") as cm:
            self.scraper.scrape_events()

        for error in [
            "ERROR:ufcscraper.event_scraper:Error saving data from url: "
            "http://example.com/event-details/fail\nError: list index out of range",
            "ERROR:ufcscraper.event_scraper:Error saving data from url: "
            "http://example.com/event-details/none\nError: list index out of range",
        ]:
            self.assertIn(error, cm.output)

        self.assertEqual(
            sorted(
                Path(THIS_DIR / "test_files/run_files/event_data.csv")
                .read_text()
                .splitlines()
            ),
            sorted(
                Path(THIS_DIR / "test_files/event_data.csv").read_text().splitlines()
            ),
        )

    def test_minor_methods(self) -> None:
        self.assertEqual(
            "http://www.ufcstats.com/event-details/event1",
            self.scraper.url_from_id("event1"),
        )


if __name__ == "__main__":
    unittest.main()
