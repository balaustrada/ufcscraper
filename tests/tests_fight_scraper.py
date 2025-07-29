from __future__ import annotations

import unittest
from pathlib import Path
from shutil import copy, rmtree
from unittest.mock import MagicMock, Mock, patch

import bs4
import requests

import ufcscraper
from ufcscraper.fight_scraper import *
from ufcscraper.tests.tests_event_scraper import mock_get as mock_event_get

THIS_DIR = Path(__file__).parent


def mock_get(url: str) -> str | None:
    mock = Mock()

    if url == "http://www.ufcstats.com/statistics/events/completed?page=all":
        page = Path(THIS_DIR / "test_files/htmls/event_search_page.html")
    elif url == "http://www.example.com/fight-details/fight1":
        page = Path(THIS_DIR / "test_files/htmls/fight_page1.html")
    elif url == "http://www.example.com/fight-details/fight2":
        page = Path(THIS_DIR / "test_files/htmls/fight_page2.html")
    elif url == "http://www.example.com/fight-details/fight3":
        page = Path(THIS_DIR / "test_files/htmls/fight_page3.html")
    elif url == "http://www.example.com/fight-details/fight4":
        page = Path(THIS_DIR / "test_files/htmls/fight_page4.html")
    elif url == "http://www.example.com/fight-details/fight5":
        page = Path(THIS_DIR / "test_files/htmls/fight_page5.html")
    elif url == "http://www.example.com/fight-details/fight6":
        page = Path(THIS_DIR / "test_files/htmls/fight_page6.html")
    elif url == "http://www.example.com/fight-details/fight7fail":
        page = Path(THIS_DIR / "test_files/htmls/fight_page7fail.html")
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


def mock_url_from_id_event(id_: str) -> str:
    return f"http://example.com/event-details/{id_}"


def mock_url_from_id_fight(id_: str) -> str:
    return f"http://www.example.com/fight-details/{id_}"


class TestFightScraper(unittest.TestCase):
    def setUp(self) -> None:
        Path(THIS_DIR / "test_files/run_files").mkdir(exist_ok=True)
        self.scraper = FightScraper(
            data_folder=THIS_DIR / "test_files/run_files",
            n_sessions=1,
            delay=0,
        )
        copy(
            THIS_DIR / "test_files/fighter_data.csv",
            THIS_DIR / "test_files/run_files/.",
        )
        copy(
            THIS_DIR / "test_files/event_data.csv",
            THIS_DIR / "test_files/run_files/.",
        )

    def tearDown(self) -> None:
        rmtree(THIS_DIR / "test_files/run_files/")

    @patch.object(
        ufcscraper.event_scraper.EventScraper,
        "url_from_id",
        side_effect=mock_url_from_id_event,
    )
    @patch.object(requests.Session, "get", side_effect=mock_event_get)
    def test_get_fight_urls(self, mock_get: Mock, mock_url_from_id: Mock) -> None:
        urls = self.scraper.get_fight_urls(get_all_events=True)

        self.assertEqual(
            sorted(urls),
            [f"http://www.example.com/fight-details/fight{i}" for i in range(1, 7)]
            + [
                "http://www.example.com/fight-details/fight7fail",
            ],
        )

    @patch.object(
        ufcscraper.event_scraper.EventScraper,
        "url_from_id",
        side_effect=mock_url_from_id_event,
    )
    @patch.object(
        ufcscraper.fight_scraper.FightScraper,
        "url_from_id",
        side_effect=mock_url_from_id_fight,
    )
    @patch.object(requests.Session, "get", side_effect=mock_get)
    def test_scrape_events(
        self, mock_get: Mock, mock_url_from_id: Mock, mock_url_from_id_2: Mock
    ) -> None:
        with self.assertLogs("ufcscraper.fight_scraper", level="ERROR") as cm:
            self.scraper.scrape_fights()

        for error in [
            "ERROR:ufcscraper.fight_scraper:Error saving data from url: http://"
            "www.example.com/fight-details/fight7fail\nError: Couldn't find "
            "header in the soup.",
        ]:
            self.assertIn(error, cm.output)

        for file in "fight_data", "round_data":
            self.assertEqual(
                sorted(
                    Path(THIS_DIR / f"test_files/run_files/{file}.csv")
                    .read_text()
                    .splitlines()
                ),
                sorted(
                    Path(THIS_DIR / f"test_files/{file}.csv").read_text().splitlines()
                ),
            )

    @patch.object(
        ufcscraper.event_scraper.EventScraper,
        "url_from_id",
        side_effect=mock_url_from_id_event,
    )
    @patch.object(
        ufcscraper.fight_scraper.FightScraper,
        "url_from_id",
        side_effect=mock_url_from_id_fight,
    )
    @patch.object(requests.Session, "get", side_effect=mock_get)
    def test_scrape_events_partial(
        self, mock_get: Mock, mock_url_from_id: Mock, mock_url_from_id_2: Mock
    ) -> None:
        self.scraper.scrape_fights(get_all_events=False)

        for file in "fight_data", "round_data":
            self.assertEqual(
                sorted(
                    Path(THIS_DIR / f"test_files/run_files/{file}.csv")
                    .read_text()
                    .splitlines()
                ),
                sorted(
                    Path(THIS_DIR / f"test_files/{file}.csv").read_text().splitlines()
                ),
            )

    def test_minor_methods(self) -> None:
        self.assertEqual(
            "http://www.ufcstats.com/fight-details/fight1",
            self.scraper.url_from_id("fight1"),
        )

        win_lose = MagicMock()
        win_lose.__iter__.return_value = iter([Mock(), Mock()])
        win_lose.__len__.return_value = 2
        win_lose[0].text = "D"
        win_lose[1].text = "D"
        self.assertEqual("Draw", self.scraper.get_winner("a", "b", win_lose))

        win_lose = MagicMock()
        win_lose.__iter__.return_value = iter([Mock(), Mock()])
        win_lose.__len__.return_value = 2
        win_lose[0].text = "C"
        win_lose[1].text = "L"
        self.assertEqual(
            "",
            self.scraper.get_winner("a", "b", win_lose),
        )

        fight_type = MagicMock()
        fight_type.__iter__.return_value = iter([Mock(), Mock()])
        fight_type.__len__.return_value = 2
        fight_type[0].text = "MIddlewieght Title"
        self.assertEqual(
            "T",
            self.scraper.get_title_fight(fight_type),
        )

        overview = MagicMock()
        overview.__iter__.return_value = iter([Mock() for i in range(3)])
        overview.__len__.return_value = 2
        overview[3].text = "f"
        self.assertEqual(
            "",
            self.scraper.get_referee(overview),
        )

        soup = bs4.BeautifulSoup(
            Path(THIS_DIR / "test_files/htmls/fight_page8.html").read_text(), "xml"
        )
        fight_stats_select = soup.select("p.b-fight-details__table-text")

        self.assertEqual(
            ("",) * 22,
            RoundsHandler.get_stats(fight_stats_select, 0, 0, 0),
        )

        with self.assertRaises(ValueError):
            RoundsHandler.get_stats(fight_stats_select, 2, 0, 2)


if __name__ == "__main__":
    unittest.main()
