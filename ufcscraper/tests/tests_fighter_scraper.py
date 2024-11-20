from __future__ import annotations

import unittest
from pathlib import Path
from shutil import rmtree, copy
from pathlib import Path
import requests

from unittest.mock import patch, Mock
from ufcscraper.fighter_scraper import *


THIS_DIR = Path(__file__).parent


def mock_get(url: str) -> str | None:

    mock = Mock()

    if url == "http://www.ufcstats.com/statistics/fighters?char=a&page=all":
        page = THIS_DIR / "test_files/htmls/fighter_search_page.html"
    elif url == "http://example.com/fighter1":
        page = THIS_DIR / "test_files/htmls/fighter_page1.html"
    elif url == "http://example.com/fighter2":
        page = THIS_DIR / "test_files/htmls/fighter_page2.html"
    elif url == "http://example.com/fighter3":
        page = THIS_DIR / "test_files/htmls/fighter_page3.html"
    elif url == "http://example.com/fighter4":
        page = THIS_DIR / "test_files/htmls/fighter_page4.html"
    elif url == "http://example.com/fighter5":
        page = THIS_DIR / "test_files/htmls/fighter_page5.html"
    elif url == "http://example.com/fighter6":
        page = THIS_DIR / "test_files/htmls/fighter_page6.html"
    elif url == "http://example.com/fail":
        page = THIS_DIR / "test_files/htmls/fighter_page_fail.html"
    else:
        page = THIS_DIR / "test_files/htmls/empty_page.html"

    mock.text = Path(page).read_text()
    return mock


class TestFighterScrape(unittest.TestCase):
    def setUp(self) -> None:
        Path(THIS_DIR / "test_files/run_files").mkdir(exist_ok=True)
        self.scraper = FighterScraper(
            data_folder=THIS_DIR / "test_files/run_files",
            n_sessions=1,
            delay=0,
        )

    def tearDown(self) -> None:
        rmtree(THIS_DIR / "test_files/run_files/")

    @patch.object(requests.Session, "get", side_effect=mock_get)
    def test_get_fighter_urls(self, mock_get: Mock) -> None:

        urls = self.scraper.get_fighter_urls()

        self.assertEqual(
            sorted(urls),
            [
                "http://example.com/fail",
                "http://example.com/fighter1",
                "http://example.com/fighter2",
                "http://example.com/fighter3",
                "http://example.com/fighter4",
                "http://example.com/fighter5",
                "http://example.com/fighter6",
                "http://example.com/none",
            ],
        )

    @patch.object(requests.Session, "get", side_effect=mock_get)
    def test_scrape_fighters(self, mock_get: Mock) -> None:
        with self.assertLogs("ufcscraper.fighter_scraper", level="ERROR") as cm:
            self.scraper.scrape_fighters()

        for error in [
            "ERROR:ufcscraper.fighter_scraper:Error saving data from url: "
            "http://example.com/fail\nError: list index out of range",
            "ERROR:ufcscraper.fighter_scraper:Error saving data from url: "
            "http://example.com/none\nError: list index out of range",
        ]:
            self.assertIn(error, cm.output)

        self.assertEqual(
            sorted(
                Path(THIS_DIR / "test_files/run_files/fighter_data.csv")
                .read_text()
                .splitlines()
            ),
            sorted(
                Path(THIS_DIR / "test_files/fighter_data.csv").read_text().splitlines()
            ),
        )

        self.scraper.load_data()
        self.scraper.add_name_column()

        self.assertEqual(
            sorted(self.scraper.data["fighter_name"].tolist()),
            [
                "Emily Brown",
                "Jane Smith",
                "John Doe",
                "Liam Jones",
                "Max Power",
                "Sophia Wilson",
            ],
        )

    def test_minor_methods(self) -> None:
        self.assertEqual(
            "http://www.ufcstats.com/fighter-details/fighter1",
            self.scraper.url_from_id("fighter1"),
        )

        self.assertEqual(
            "fighter1",
            self.scraper.id_from_url(
                "http://www.ufcstats.com/fighter-details/fighter1/"
            ),
        )

        self.assertEqual(
            [
                self.scraper.parse_l_name("Name".split(" ")),
                self.scraper.parse_l_name("Name Lastname".split(" ")),
                self.scraper.parse_l_name("Name S Second".split(" ")),
                self.scraper.parse_l_name("Name S Second Third".split(" ")),
                self.scraper.parse_l_name(
                    "Name S Second Third Fourth".split(" "),
                ),
            ],
            [
                "",
                "Lastname",
                "S Second",
                "S Second Third",
                "",
            ],
        )

        nickname = Mock()
        nickname.text = "\n"
        self.assertEqual(self.scraper.parse_nickname(nickname), "")

        nickname.text = "quantity:--"
        for x in (
            self.scraper.parse_height,
            self.scraper.parse_reach,
            self.scraper.parse_weight,
            self.scraper.parse_dob,
        ):
            self.assertEqual(x(nickname), "")

        nickname.text = "quantity:"
        self.assertEqual(self.scraper.parse_stance(nickname), "")

    def test_use_existing_data(self) -> None:
        copy(
            THIS_DIR / "test_files/fighter_data.csv", THIS_DIR / "test_files/run_files"
        )

        with self.assertLogs("ufcscraper.base", level="INFO") as cm:
            self.scraper.check_data_file()

        self.assertIn("Using existing file", cm.output[0])


if __name__ == "__main__":
    unittest.main()
