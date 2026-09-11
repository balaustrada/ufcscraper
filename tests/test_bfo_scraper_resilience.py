from __future__ import annotations

import multiprocessing
import unittest
from unittest.mock import MagicMock, patch

from ufcscraper.odds_scraper.bfo_scraper import BaseBestFightOddsScraper


class TestBfoScraperResilience(unittest.TestCase):
    def test_worker_reports_failed_task_after_retries(self) -> None:
        def fail(*args: object) -> None:
            raise RuntimeError("BFO unavailable")

        task_queue: multiprocessing.Queue = multiprocessing.Queue()
        result_queue: multiprocessing.Queue = multiprocessing.Queue()
        task_queue.put((([], []), "fighter-1"))
        task_queue.put(None)
        driver = MagicMock()

        with patch(
            "ufcscraper.odds_scraper.bfo_scraper.webdriver.Chrome",
            return_value=MagicMock(),
        ):
            worker = BaseBestFightOddsScraper.worker_constructor_target(fail)
            worker(task_queue, result_queue, driver)

        self.assertEqual(result_queue.get(timeout=1), (None, "fighter-1"))

    def test_profile_parser_skips_future_events_rows(self) -> None:
        html = """
            <table>
              <tr></tr><tr></tr>
              <tr><td class="moneyline">-120</td><td class="moneyline">-125</td><td class="moneyline">-115</td></tr>
              <tr><td class="item-non-mobile">Future Events</td><td><a href="/fighters/opponent">Opponent</a></td></tr>
            </table>
        """
        profile_element = MagicMock()
        profile_element.get_attribute.side_effect = lambda attribute: (
            html if attribute == "innerHTML" else ""
        )
        driver = MagicMock()
        fighter_name = MagicMock()
        fighter_name.text = "Fighter"
        driver.find_elements.return_value = [fighter_name]

        with patch("ufcscraper.odds_scraper.bfo_scraper.WebDriverWait") as wait:
            wait.return_value.until.return_value = [profile_element]
            result = BaseBestFightOddsScraper.extract_odds_from_fighter_profile(driver)

        self.assertEqual(result[0], "Fighter")
        self.assertEqual(result[1:], ([], [], [], [], [], []))

    def test_captcha_wait_has_a_deadline(self) -> None:
        driver = MagicMock()
        with (
            patch.object(
                BaseBestFightOddsScraper, "captcha_indicator", return_value=True
            ),
            patch("ufcscraper.odds_scraper.bfo_scraper.time.monotonic", side_effect=[0, 121]),
            patch("ufcscraper.odds_scraper.bfo_scraper.time.sleep"),
        ):
            with self.assertRaises(TimeoutError):
                BaseBestFightOddsScraper.wait_for_captcha(driver)
