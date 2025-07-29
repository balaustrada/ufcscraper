from __future__ import annotations

import datetime
import logging
import multiprocessing
import unittest
from pathlib import Path
from shutil import copy, rmtree
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
from selenium import webdriver

from ufcscraper.event_scraper import *
from ufcscraper.odds_scraper import BestFightOddsScraper

if TYPE_CHECKING:
    import datetime
    from typing import Callable, List, Optional, Tuple


THIS_DIR = Path(__file__).parent

fighters_ids = {
    "John Doe": "fighter1",
    "Jane Smith": "fighter2",
    "Max Power": "fighter3",
    "Emily Brown": "fighter4",
    "Liam Jones": "fighter5",
    "Sophia Wilson": "fighter6",
}

fighters_names = {v: k for k, v in fighters_ids.items()}


def mock_search_fighter(
    search_fighter: str, driver: Optional[webdriver.Chrome]
) -> Optional[Tuple[str, str]]:
    if search_fighter in fighters_ids and search_fighter != "Max Power":
        return (search_fighter, fighters_ids[search_fighter])
    else:
        return None


def mock_get_odds(
    fighter_BFO_ids: Optional[List[str]],
    fighter_search_names: Optional[List[str]],
    driver: Optional[webdriver.Chrome],
) -> Tuple[
    List[datetime.date | None],
    List[str],
    List[str],
    List[str],
    List[str],
    List[int | None],
    List[int | None],
    List[int | None],
]:
    if fighter_BFO_ids is None:
        fighter_BFO_ids = []
    if fighter_search_names is None:
        fighter_search_names = []

    found_fighter_BFO_ids = []
    found_fighter_BFO_names = []
    found_dates = []
    found_opponents_ids = []
    found_opponents_names = []
    found_openings = []
    found_closing_range_mins = []
    found_closing_range_maxs = []

    new_ids = []
    for search_name in fighter_search_names:
        profile = mock_search_fighter(search_name, driver)
        if profile is not None:
            new_ids.append(profile[1])

    odds_lookup = pd.read_csv(THIS_DIR / "test_files/BestFightOdds_odds.csv")
    # We may have multiple ids for the fighter, we should
    # try all of them
    for fighter_BFO_id in fighter_BFO_ids + new_ids:
        id_BFO_name = fighters_names[fighter_BFO_id]
        id_dates: List[datetime.date | None] = []
        id_opponents_name = []
        id_opponents_id = []
        id_openings: List[int | None] = []
        id_closing_range_mins: List[int | None] = []
        id_closing_range_maxs: List[int | None] = []
        for _, row in odds_lookup[
            odds_lookup["fighter_id"] == fighter_BFO_id
        ].iterrows():
            fight_id = row["fight_id"]
            if fight_id in ["fight1", "fight2"]:
                id_dates.append(datetime.date(2020, 6, 1))
            elif fight_id in ["fight3", "fight4"]:
                id_dates.append(datetime.date(2020, 8, 1))
            else:
                id_dates.append(datetime.date(2020, 9, 2))

            if fighter_BFO_id == "fighter1":
                id_opponents_id.append("fighter2")
                id_opponents_name.append(fighters_names["fighter2"])
            elif fighter_BFO_id == "fighter2":
                id_opponents_id.append("fighter1")
                id_opponents_name.append(fighters_names["fighter1"])
            elif fighter_BFO_id == "fighter3":
                id_opponents_id.append("fighter4")
                id_opponents_name.append(fighters_names["fighter4"])
            elif fighter_BFO_id == "fighter4":
                id_opponents_id.append("fighter3")
                id_opponents_name.append(fighters_names["fighter3"])
            elif fighter_BFO_id == "fighter5":
                id_opponents_id.append("fighter6")
                id_opponents_name.append(fighters_names["fighter6"])
            elif fighter_BFO_id == "fighter6":
                id_opponents_id.append("fighter5")
                id_opponents_name.append(fighters_names["fighter5"])

            id_openings.append(row["opening"])
            id_closing_range_mins.append(row["closing_range_min"])
            id_closing_range_maxs.append(row["closing_range_max"])

        found_fighter_BFO_ids += [fighter_BFO_id] * len(id_dates)
        found_fighter_BFO_names += [id_BFO_name] * len(id_dates)
        found_dates += id_dates
        found_opponents_names += id_opponents_name
        found_opponents_ids += id_opponents_id
        found_openings += id_openings
        found_closing_range_mins += id_closing_range_mins
        found_closing_range_maxs += id_closing_range_maxs

    return (
        found_dates,
        found_fighter_BFO_ids,
        found_fighter_BFO_names,
        found_opponents_ids,
        found_opponents_names,
        found_openings,
        found_closing_range_mins,
        found_closing_range_maxs,
    )


def mock_worker_constructor(method: Callable) -> Callable:
    print("mock constructor")

    def worker(
        task_queue: multiprocessing.Queue,
        result_queue: multiprocessing.Queue,
        driver: webdriver.Chrome,
    ) -> None:
        while True:
            try:
                task = task_queue.get()
                if task is None:
                    break

                args: Tuple[Optional[List[str]], Optional[List[str]]]
                args, id_ = task
                result = None

                for attempt in range(1):
                    try:
                        result = mock_get_odds(*args, driver)
                        result_queue.put((result, id_))
                        break
                    except Exception as e:
                        logging.error(
                            f"Attempt {attempt + 1} failed for task {task}: {e}"
                        )
                        logging.exception("Exception occurred")

                        # Reset the driver after a failed attempt
                        driver.quit()
                        driver = webdriver.Chrome()

            except Exception as e:
                logging.error(f"Error processing task {task}: {e}")
                logging.exception("Exception ocurred")

                # Reset the driver after a failed attempt
                driver.quit()
                driver = webdriver.Chrome()

                # Send None to the result because task failed
                result_queue.put(None)

    return worker


class TestOddsScraper(unittest.TestCase):
    def setUp(self) -> None:
        Path(THIS_DIR / "test_files/run_files").mkdir(exist_ok=True)

        self.scraper = BestFightOddsScraper(
            data_folder=THIS_DIR / "test_files/run_files",
            n_sessions=-1,  # to avoid selenium
            delay=0,
        )

        for file in (
            "event_data",
            "fight_data",
            "fight_data_partial",
            "fighter_data",
            "round_data",
        ):
            copy(
                THIS_DIR / f"test_files/{file}.csv",
                THIS_DIR / "test_files/run_files/.",
            )

    def tearDown(self) -> None:
        rmtree(THIS_DIR / "test_files/run_files/")

    @patch("ufcscraper.odds_scraper.WebDriverWait")
    @patch("ufcscraper.odds_scraper.EC")
    def test_extract_odds_from_fighter_profile(
        self, MockEC: Mock, MockWait: Mock
    ) -> None:
        mock_elements = [MagicMock()]
        mock_elements[0].get_attribute.return_value = Path(
            THIS_DIR / "test_files/htmls/bfo_profile.html"
        ).read_text()

        mock_wait = MockWait.return_value
        mock_wait.until.return_value = mock_elements

        mock_presence = MockEC.presence_of_all_elements_located.return_value

        mock_driver = MagicMock()

        result = self.scraper.extract_odds_from_fighter_profile(mock_driver)

        self.assertEqual(
            result,
            (
                "Alex Pereira",
                [datetime.date(2024, 6, 30), datetime.date(2024, 4, 14)],
                ["Jiri Prochazka", "Jamahal Hill"],
                ["Jiri-Prochazka-6058", "Jamahal-Hill-9288"],
                [-135, -163],
                [-155, -155],
                [-145, -130],
            ),
        )

    @patch.object(BestFightOddsScraper, "captcha_indicator", return_value=False)
    @patch("ufcscraper.odds_scraper.WebDriverWait")
    @patch("ufcscraper.odds_scraper.EC")
    def test_search_fighter_profile(
        self, MockEC: Mock, MockWait: Mock, mock_check_captcha: Mock
    ) -> None:
        search_html = Path(THIS_DIR / "test_files/htmls/bfo_search.html").read_text()
        mock_elements = [MagicMock()]
        mock_elements[0].get_attribute.side_effect = lambda x: (
            "content-list"
            if x == "class"
            else search_html if x == "innerHTML" else None
        )

        mock_wait = MockWait.return_value
        mock_wait.until.return_value = mock_elements

        mock_driver = MagicMock()

        result = self.scraper.search_fighter_profile("alex pereira", mock_driver)

        self.assertEqual(
            result,
            (
                "Alex Pereira",
                "https://www.bestfightodds.com/fighters/Alex-Pereira-10463",
            ),
        )

    @patch.object(BestFightOddsScraper, "captcha_indicator", return_value=False)
    @patch("ufcscraper.odds_scraper.WebDriverWait")
    @patch("ufcscraper.odds_scraper.EC")
    def test_search_fighter_profile_direct(
        self, MockEC: Mock, MockWait: Mock, mock_check_captcha: Mock
    ) -> None:
        search_html = Path(THIS_DIR / "test_files/htmls/bfo_profile.html").read_text()
        mock_elements = [MagicMock()]
        mock_elements[0].get_attribute.side_effect = lambda x: (
            "team-stats-table" if x == "class" else None
        )

        mock_wait = MockWait.return_value
        mock_wait.until.return_value = mock_elements

        mock_driver = MagicMock()
        mock_driver.current_url = (
            "https://www.bestfightodds.com/fighters/Alex-Pereira-10463"
        )

        name = Mock()
        mock_driver.find_element.return_value = name
        name.text = "Alex Pereira"
        result = self.scraper.search_fighter_profile("alex pereira", mock_driver)

        self.assertEqual(
            result,
            (
                "Alex Pereira",
                "https://www.bestfightodds.com/fighters/Alex-Pereira-10463",
            ),
        )

    @patch.object(
        BestFightOddsScraper,
        "worker_constructor_target",
        side_effect=mock_worker_constructor,
    )
    def test_scrape_BFO_odds(self, mock_constructor: Mock) -> None:
        import sys

        logging.basicConfig(
            stream=sys.stdout,
            level="INFO",
            format="%(levelname)s:%(message)s",
        )
        mock_driver = MagicMock()
        mock_driver.quit.return_value = None
        self.scraper.drivers = [mock_driver, mock_driver, mock_driver]
        self.scraper.n_sessions = 3
        self.scraper.scrape_BFO_odds()

        for file in "BestFightOdds_odds", "fighter_names":
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


if __name__ == "__main__":
    unittest.main()
