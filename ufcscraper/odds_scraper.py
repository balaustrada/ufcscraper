"""
This module provides tools for scraping, managing, and processing UFC fight Odds
obtained from BestFightOdds (BFO)..

Classes:
--------

- `BestFightOddsScraper`:
    This class handles the process of scraping betting odds from the Best Fight Odds 
    (BFO) website. It is designed to work iteratively, continually checking for and 
    updating missing fighter records by checking data from UFCStats. The results are 
    then stored and organized for further analysis.

- `FighterNames`:
    This class is responsible for managing the records of UFC fighters, including
    their IDs, names, and associated database identifiers from different sources.
    It is filled at the same time as the odds data is scraped and facilitates
    the completeness of the scraping process. The class also handles the storage 
    of this data.
"""

from __future__ import annotations

import csv
import datetime
import logging
import multiprocessing
import time
import urllib
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
from bs4 import BeautifulSoup
from fuzzywuzzy import fuzz, process
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from ufcscraper.base import BaseScraper, BaseFileHandler
from ufcscraper.ufc_scraper import UFCScraper
from ufcscraper.utils import element_present_in_list, parse_date

if TYPE_CHECKING:  # pragma: no cover
    import datetime
    from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class BestFightOddsScraper(BaseScraper):
    """A scraper for Best Fight Odds data.

    This class is responsible for scraping betting odds data for fighters
    from Best Fight Odds. It supports parallel processing to speed up
    the scraping process and handles captcha detection (not solving).

    Attributes:
        min_score: Minimum score for matching fighter names.
        max_exception_retries: Maximum number of retries for failed
            requests.
        wait_time: Time to wait for elements to load.
    """

    dtypes: Dict[str, type | pd.core.arrays.integer.Int64Dtype] = {
        "fight_id": str,
        "fighter_id": str,
        "opening": pd.Int64Dtype(),
        "closing_range_min": pd.Int64Dtype(),
        "closing_range_max": pd.Int64Dtype(),
    }
    sort_fields = ["fight_id", "fighter_id"]
    data = pd.DataFrame({col: pd.Series(dtype=dt) for col, dt in dtypes.items()})
    filename = "BestFightOdds_odds.csv"
    n_sessions = 1  # New default value
    min_score = 90
    max_exception_retries = 3
    wait_time = 20
    web_url = "https://www.bestfightodds.com"

    def __init__(
        self,
        data_folder: Path | str,
        n_sessions: Optional[int] = None,
        delay: Optional[float] = None,
        min_score: Optional[int] = None,
        min_date: datetime.date = datetime.date(2008, 8, 1),
    ):
        """Initialize the BestFightOddsScraper.

        It extends the BaseScraper class by adding score for
        naming matching, date filtering and initializes fighter_names
        class correspondent to the data_folder.

        Args:
            data_folder: Path to the folder where data is stored.
            n_sessions: Number of concurrent browser sessions.
            delay: Delay between requests.
            min_score: Minimum score for name matching.
            min_date: Minimum date for filtering events. Events prior to August
                2008 are not available.
        """
        super().__init__(data_folder, n_sessions, delay)

        # For this scraper it is better to not continuously reload the driver
        self.drivers = [webdriver.Chrome() for _ in range(self.n_sessions)]
        self.fighter_names = FighterNames(self.data_folder)
        self.min_score = min_score or self.min_score
        self.min_date = min_date

    @classmethod
    def create_search_url(cls, query: str) -> str:
        """Create the search URL for a fighter.

        Args:
            query: Name of the fighter.

        Returns:
            Search URL as a string.
        """
        encoded_query = urllib.parse.quote_plus(query)
        search_url = f"{cls.web_url}/search?query={encoded_query}"

        return search_url

    @staticmethod
    def captcha_indicator(driver: webdriver.Chrome) -> bool:  # pragma: no cover
        """Check if there is a captcha.

        Args:
            driver: The web driver instance.

        Returns:
            True if captcha is detected, otherwise False.
        """
        elements = driver.find_elements(By.ID, "hfmr8")
        if len(elements) > 0:
            if (
                elements[0].text
                == "Verify you are human by completing the action below."
            ):
                return True
        return False

    @classmethod
    def worker_constructor_target(
        cls,
        method: Callable[..., Any],
    ) -> Callable[
        [multiprocessing.Queue, multiprocessing.Queue, webdriver.Chrome], None
    ]:
        """Construct the worker target function for parallel processing.

        Args:
            method: Method to be used by worker function.

        Returns:
            A worker function that processes tasks from a queue and puts
                results in another queue.
        """

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

                    args, id_ = task
                    result = None

                    for attempt in range(cls.max_exception_retries + 1):
                        try:
                            result = method(*args, driver)
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

    def get_odds_from_profile_urls(
        self,
        fighter_BFO_ids: Optional[List[str]] = None,
        fighter_search_names: Optional[List[str]] = None,
        driver: Optional[webdriver.Chrome] = None,
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
        """Get odds data from multiple fighter profile URLs.

        Given the structure of BFO, it is possible and likely
        for a fighter to have multiple profile URLs. This method
        ensures that all the profiles are scraped.

        Args:
            fighter_BFO_ids: List of fighter IDs from Best Fight Odds.
            fighter_search_names: List of fighter search names to be
                searched for.
            driver: The web driver instance.

        Returns:
            Tuple containing lists of dates, fighter IDs, fighter names,
            opponent IDs, opponent names, opening odds, closing range
            mins, and closing range maxs.
        """
        if driver is None:
            driver = self.drivers[0]

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
            profile = self.search_fighter_profile(search_name, driver)
            if profile is not None:
                new_ids.append(self.id_from_url(profile[1]))

        # We may have multiple ids for the fighter, we should
        # try all of them
        for fighter_BFO_id in fighter_BFO_ids + new_ids:
            driver.get(self.url_from_id(fighter_BFO_id))
            (
                id_BFO_name,
                id_dates,
                id_opponents_name,
                id_opponents_id,
                id_openings,
                id_closing_range_mins,
                id_closing_range_maxs,
            ) = self.extract_odds_from_fighter_profile(driver)

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

    def get_parallel_odds_from_profile_urls(
        self,
        fighters_id: List[str],
        fighters_search_names: List[Set[str]],
        fighters_BFO_ids: List[Set[str]],
    ) -> Tuple[
        multiprocessing.Queue, multiprocessing.Queue, List[multiprocessing.Process]
    ]:
        """Scrape odds data in parallel from fighter profile URLs.

        Args:
            fighters_id: List of fighter IDs.
            fighters_search_names: Search names to try for each fighter.
            fighters_BFO_ids: BestFightOdds known IDs for each fighter.

        Returns:
            Tuple containing result queue, task queue, and list of worker
            processes.
        """
        task_queue: multiprocessing.Queue = multiprocessing.Queue()
        result_queue: multiprocessing.Queue = multiprocessing.Queue()

        # Adding tasks
        for (
            fighter_id,
            fighter_search_names,
            fighter_BFO_id,
        ) in zip(fighters_id, fighters_search_names, fighters_BFO_ids):
            task_queue.put(
                (
                    (fighter_BFO_id, fighter_search_names),
                    fighter_id,
                )
            )

        # Define worker around get_odds_from_profile_urls
        worker_target = self.worker_constructor_target(self.get_odds_from_profile_urls)

        # Starting workers
        workers = [
            multiprocessing.Process(
                target=worker_target,
                args=(task_queue, result_queue, driver),
            )
            for driver in self.drivers
        ]
        for worker in workers:
            worker.start()

        # Return queues and workers to handle outside of the function
        return result_queue, task_queue, workers

    @classmethod
    def extract_odds_from_fighter_profile(
        cls,
        driver: webdriver.Chrome,
    ) -> Tuple[
        str,
        List[datetime.date | None],
        List[str],
        List[str],
        List[int | None],
        List[int | None],
        List[int | None],
    ]:
        """Extract odds data from a single fighter's profile page.

        Args:
            driver: The web driver instance.

        Returns:
            Tuple containing fighter name, dates, opponent names, opponent
            IDs, opening odds, closing range mins, and closing range maxs.
        """
        # Wait for profile table to be there
        element = WebDriverWait(driver, cls.wait_time).until(
            element_present_in_list(
                (By.CLASS_NAME, "team-stats-table"), (By.ID, "hfmr8")
            )
        )[
            0
        ]  # type: ignore[index]

        if element.get_attribute("id") == "hfmr8":  # pragma: no cover
            while BestFightOddsScraper.captcha_indicator(driver):
                logging.warning("Human recognition page detected, stalling...")
                time.sleep(5)

        # Extract table
        soup = BeautifulSoup(
            element.get_attribute("innerHTML"),
            "html.parser",
        )

        rows = soup.find_all("tr")

        rows_f = rows[2::3]
        rows_s = rows[3::3]

        assert len(rows_f) == len(rows_s)

        dates = []
        opponents_name: List[str] = []
        opponents_id = []
        openings = []
        closing_range_min = []
        closing_range_max = []

        fighter_name: str = rows_f[0].select_one("a").get_text(strip=True)

        for row_f, row_s in zip(rows_f, rows_s):
            date_string = row_s.find(class_="item-non-mobile").text
            if date_string == "":
                continue
            else:
                date = parse_date(date_string)

            opponent = row_s.select_one("a")

            moneyline_elements = row_f.find_all("td", class_="moneyline")
            moneyline_values = [
                elem.get_text(strip=True) for elem in moneyline_elements
            ]

            if moneyline_values[0] == "":
                openings.append("")
                closing_range_min.append("")
                closing_range_max.append("")
            else:
                openings.append(moneyline_values[0])
                closing_range_min.append(moneyline_values[1])
                closing_range_max.append(moneyline_values[2])

            dates.append(date)
            opponents_name.append(opponent.get_text(strip=True))
            opponents_id.append(cls.id_from_url(opponent["href"]))

        openings_int = list(map(lambda x: int(x) if x != "" else None, openings))
        closing_range_min_int = list(
            map(lambda x: int(x) if x != "" else None, closing_range_min)
        )
        closing_range_max_int = list(
            map(lambda x: int(x) if x != "" else None, closing_range_max)
        )

        return (
            fighter_name,
            dates,
            opponents_name,
            opponents_id,
            openings_int,
            closing_range_min_int,
            closing_range_max_int,
        )

    @classmethod
    def url_from_id(cls, id_: str) -> str:
        """Constructs the BFO URL for a fighter's profile based on their ID.

        Args:
            id_: The fighter's unique identifier.

        Returns:
            The URL for the fighter's details page.
        """
        return f"{cls.web_url}/fighters/{id_}"

    def search_fighter_profile(
        self, search_fighter: str, driver: webdriver.Chrome
    ) -> Optional[Tuple[str, str]]:
        """Search for a fighter's profile.

        Searches for a fighter's profile using the BFO search engine.

        Args:
            search_fighter: Name of the fighter to search for.
            driver: The web driver instance.

        Returns:
            Tuple containing the profile URL and profile ID, or None if
            not found.
        """
        url = self.create_search_url(search_fighter)

        driver.get(url)

        while self.captcha_indicator(driver):
            logging.warning("Human recognition page detected, stalling..")
            time.sleep(5)

        # Three possible outputs
        element = WebDriverWait(driver, self.wait_time).until(
            element_present_in_list(
                (By.CLASS_NAME, "content-list"),  # Search result
                (By.CLASS_NAME, "team-stats-table"),  # Direct redirect to fighter page
                (By.CSS_SELECTOR, "p"),  # No results found.
                (By.ID, "hfmr8"),  # Captcha
            )
        )[
            0
        ]  # type: ignore[index]

        if element.get_attribute("id") == "hfmr8":  # pragma: no cover
            while self.captcha_indicator(driver):
                logging.warning("Human recognition page detected, stalling..")
                time.sleep(5)

        if element.get_attribute("class") == "team-stats-table":
            fighter = driver.find_element(By.ID, "team-name").text

            return (fighter, driver.current_url)

        elif (
            element.get_attribute("class") == "content-list"
            or "Showing results for search query" in element.text
        ):
            soup = BeautifulSoup(element.get_attribute("innerHTML"), "html.parser")

            fighters_names = []
            fighters_urls = []

            rows = soup.find_all("tr")
            for row in rows:
                link_element = row.find("a")
                if link_element:
                    fighters_names.append(link_element.text)
                    fighters_urls.append(link_element["href"])

            best_name, score = process.extractOne(
                search_fighter, fighters_names, scorer=fuzz.token_sort_ratio
            )

            if score > self.min_score:
                logger.info(f"Found {best_name} ({search_fighter}) with score {score}")
                fighter_url = (
                    self.web_url + fighters_urls[fighters_names.index(best_name)]
                )
                driver.get(fighter_url)

                return (best_name, fighter_url)

        logger.info(f"Couldn't find profile for {search_fighter}")
        return None

    def get_ufcstats_data(self) -> pd.DataFrame:
        """Load UFCStats data.

        Mixes all the data from the scraped UFCStats website to create
        a dataframe where each row corresponds to a single fight,fighter
        pair.

        These are the records that will be saved as odds data later.

        Returns:
            DataFrame containing UFCStats data.
        """
        logger.info("Loading UFCStats data...")
        ufc_stats_data = UFCScraper(self.data_folder)

        events = ufc_stats_data.event_scraper.data
        fights = ufc_stats_data.fight_scraper.data

        fighters_object = ufc_stats_data.fighter_scraper
        fighters_object.add_name_column()

        data = pd.concat(
            [
                fights.rename(
                    columns={"fighter_1": "opponent_id", "fighter_2": "fighter_id"}
                ),
                fights.rename(
                    columns={"fighter_2": "opponent_id", "fighter_1": "fighter_id"}
                ),
            ]
        )

        # Now with events to get dates
        data = data.merge(
            events,
            on="event_id",
            how="left",
        )[["fight_id", "event_id", "fighter_id", "opponent_id", "event_date"]]

        data["event_date"] = pd.to_datetime(data["event_date"])
        logger.info("Applying date mask...")
        logger.info(f"Previous size: {len(data)}")
        data = data[data["event_date"].dt.date >= self.min_date]
        logger.info(f"New size: {len(data)}")

        # aggregate fighter names, same id: list of names and list of urls.
        fighter_names_aggregated = (
            self.fighter_names.data.groupby(["fighter_id", "database"])[
                ["name", "database_id"]
            ]
            .agg(list)
            .reset_index()
        )
        # Now we change it further, removing database column and integrating it into name and database_id columns
        # BestFightOdds_names, UFCStats_names, BestFightOdds_database_id, UFCStats_database_id
        fighter_names_aggregated = fighter_names_aggregated.pivot(
            index="fighter_id", columns="database", values=["name", "database_id"]
        )
        fighter_names_aggregated.columns = pd.Index(
            [f"{col[1]}_{col[0]}" for col in fighter_names_aggregated.columns]
        )
        fighter_names_aggregated.columns = pd.Index(
            [
                col + "s" if col != "fighter_id" else col
                for col in fighter_names_aggregated.columns
            ]
        )

        # Finally this have columns # fighter_id, BestFightOdds_names, UFCStats_names, BestFightOdds_database_ids, UFCStats_database_ids
        fighter_names_aggregated = fighter_names_aggregated.reset_index()

        # Rename it:
        fighter_names_aggregated = fighter_names_aggregated.rename(
            columns={
                "BestFightOdds_names": "BFO_names",
                "UFCStats_names": "UFC_names",
                "BestFightOdds_database_ids": "BFO_database_ids",
                "UFCStats_database_ids": "UFC_database_ids",
            }
        )

        data = data.merge(
            fighter_names_aggregated,
            on="fighter_id",
            how="left",
        )

        data = data.merge(
            fighter_names_aggregated.rename(
                columns={
                    "fighter_id": "opponent_id",
                    "BFO_names": "opponent_BFO_names",
                    "UFC_names": "opponent_UFC_names",
                    "BFO_database_ids": "opponent_BFO_database_ids",
                    "UFC_database_ids": "opponent_UFC_database_ids",
                }
            ),
            on="opponent_id",
            how="left",
        )

        # Check if all columns are there (if no data some may be missing)
        for col in "BFO_names", "UFC_names", "BFO_database_ids", "UFC_database_ids":
            if col not in data.columns:
                data[col] = None
            if "opponent_" + col not in data.columns:
                data["opponent_" + col] = None

        # Convert NaNs to list(None) to homogenize types
        for col in "BFO_names", "UFC_names", "BFO_database_ids", "UFC_database_ids":
            data[col] = (
                data[col]
                .apply(lambda x: [] if not isinstance(x, list) and pd.isna(x) else x)
                .values
            )
            data["opponent_" + col] = (
                data["opponent_" + col]
                .apply(lambda x: [] if not isinstance(x, list) and pd.isna(x) else x)
                .values
            )

        # Return just reorganizing fields
        return data[
            [
                "event_id",
                "fight_id",
                "fighter_id",
                "opponent_id",
                "event_date",
                "UFC_names",
                "opponent_UFC_names",
                "BFO_names",
                "opponent_BFO_names",
                "UFC_database_ids",
                "opponent_UFC_database_ids",
                "BFO_database_ids",
                "opponent_BFO_database_ids",
            ]
        ]

    def remove_scraped_records(self, data: pd.DataFrame) -> pd.DataFrame:
        """Remove records that have already been scraped.

        Args:
            data: DataFrame containing new data to be scraped.

        Returns:
            DataFrame with already scraped records removed.
        """
        return (
            data.merge(
                self.data,  # This is the class data.
                on=["fight_id", "fighter_id"],
                indicator=True,
                how="outer",
            )
            .query('_merge == "left_only"')
            .drop("_merge", axis=1)
            .drop(
                columns=[
                    "opening",
                    "closing_range_min",
                    "closing_range_max",
                ]
            )
        )

    def extract_valid_fights_from_odds_data(
        self,
        fighter_missing_data: pd.DataFrame,
        odds_data: Tuple,
    ) -> Tuple[List[List[str]], Set[Tuple[str, str, str]]]:
        """Extract valid fights from odds data.

        Odds data contains all the fights odds and information that was
        found for a given fighter. This method will find the ones correspondent
        to the missing fights, and return them as valid fights to be saved
        to file.

        It also returns the list of names and IDs from BFO which can be
        added to the FighterName class instance to improve the completeness
        of the scraping process.

        Args:
            fighter_missing_data: DataFrame with missing fighter data.
            odds_data: Tuple containing odds data:
                - dates: List of dates.
                - fighter_BFO_ids: List of fighter BFO IDs.
                - fighter_BFO_names: List of fighter BFO names.
                - opponents_BFO_ids: List of opponent BFO IDs.
                - opponents_BFO_names: List of opponent BFO names.
                - openings: List of opening odds.
                - closing_range_mins: List of minimum closing odds.
                - closing_range_maxs: List of maximum closing odds.

        Returns:
            Tuple containing:
                - odds_records: List of valid odds records.
                - BFO_names: Set of tuples with BFO IDs and names.
        """
        (
            dates,
            fighter_BFO_ids,
            fighter_BFO_names,
            opponents_BFO_ids,
            opponents_BFO_names,
            openings,
            closing_range_mins,
            closing_range_maxs,
        ) = odds_data

        BFO_names: Set[Tuple[str, str, str]] = set()
        odds_records: List[List[str]] = []
        for _, row in fighter_missing_data.iterrows():
            date = row["event_date"].date()

            candidates_indxs = [
                i
                for i, candidate_date in enumerate(dates)
                if abs((candidate_date - date).days) <= 1.5
            ]

            if len(candidates_indxs) == 0:
                logger.info(
                    f"Unable to find opponent {row['opponent_UFC_names'][0]} for "
                    f"{row['UFC_names'][0]} on {date}"
                )
            else:
                possible_opponents = [opponents_BFO_names[i] for i in candidates_indxs]

                scores = [
                    process.extractOne(
                        opponent,
                        possible_opponents,
                        scorer=fuzz.token_sort_ratio,
                    )
                    for opponent in possible_opponents
                ]

                best_name, score = max(scores, key=lambda x: x[1])

                # Iterate to find the position of the match
                # date and name
                for best_index, (date_, name_) in enumerate(
                    zip(dates, opponents_BFO_names)
                ):
                    if (abs((date - date_).days) <= 1.5) and name_ == best_name:
                        break
                else:
                    # Unable to find, let's set the score to 0
                    # and nothing will be added
                    score = 0

                if score > self.min_score:
                    odds_records.append(
                        [
                            row["fight_id"],
                            row["fighter_id"],
                            openings[best_index],
                            closing_range_mins[best_index],
                            closing_range_maxs[best_index],
                        ]
                    )
                    BFO_names.add(
                        (
                            row["opponent_id"],
                            opponents_BFO_ids[best_index],
                            opponents_BFO_names[best_index],
                        )
                    )
                    BFO_names.add(
                        (
                            row["fighter_id"],
                            fighter_BFO_ids[best_index],
                            fighter_BFO_names[best_index],
                        )
                    )
                else:
                    logger.info(
                        f"Unable to find opponent {row['opponent_UFC_names'][0]} for "
                        f"{row['UFC_names'][0]} on {date}."
                    )

        return odds_records, BFO_names

    def scrape_BFO_odds(self) -> None:
        """Scrape Best Fight Odds (BFO) and update records.

        This method performs the following:
            1. Checks for missing fighter records.
            2. Retrieves UFCStats data.
            3. Removes already scraped records.
            4. Collects data for scraping.
            5. Scrapes data in parallel from BFO URLs.
            6. Updates records and retries if necessary.

        Each time a record is added, the BFO IDs and therefore
        URLs are stored, therefore in the next iteration it will
        capture records from fighters that were missing in the previous.
        This is why the scraping process is iterative until a iteration
        is performed with no new records found.
        """
        self.fighter_names.check_missing_records()

        # Get data, this can have up to 4 entries for each fighter
        # x2 for each fighter
        # x2 for each database (UFCStats, BestFightOdds)
        ufc_stats_data = self.get_ufcstats_data()

        data_to_scrape = self.remove_scraped_records(ufc_stats_data)
        logger.info(f"Number of rows to scrape: {len(data_to_scrape)}")

        ########################################################
        # First parallel process: read data from BFO URLs:
        ########################################################
        #################
        # Collect data
        #################
        ids = []
        bfo_ids = []
        search_names = []

        grouped_data = data_to_scrape.groupby("fighter_id")
        for fighter_id, group in grouped_data:
            group = group[~group["BFO_database_ids"].isna()]

            if len(group) > 0:
                ids.append(str(fighter_id))
                bfo_ids.append(group["BFO_database_ids"].values[0])

                bfo_names = group["BFO_names"].values[0]

                if bfo_names == []:
                    # If there are no BFO names, I add a search request.
                    search_names.append(group["UFC_names"].values[0])
                else:
                    search_names.append(None)

        #################
        # Scrape data
        #################
        fighters_scraped = 0
        fighters_to_scrape = len(ids)
        records_added = 0
        records_to_add = len(data_to_scrape)

        result_queue, task_queue, workers = self.get_parallel_odds_from_profile_urls(
            ids,
            search_names,
            bfo_ids,
        )
        with (
            open(self.data_file, "a") as f_odds,
            open(self.fighter_names.data_file, "a") as f_names,
        ):
            writer_odds = csv.writer(f_odds)
            writer_names = csv.writer(f_names)

            while fighters_scraped < fighters_to_scrape:
                result, fighter_id = result_queue.get()
                fighters_scraped += 1

                if result is not None:
                    odds_records, BFO_names = self.extract_valid_fights_from_odds_data(
                        grouped_data.get_group(fighter_id),
                        result,
                    )

                    # Write records
                    [writer_odds.writerow(record) for record in odds_records]
                    records_added += len(odds_records)

                    logger.info(
                        f"{fighters_scraped} out of {fighters_to_scrape} fighters."
                        f"\n\t{records_added} out of {records_to_add} records added."
                    )

                    # Check if the valid names are already in the names table
                    # and if not, add them
                    for id_, bfo_id, name in BFO_names:
                        if not self.fighter_names.fighter_in_database(
                            id_,
                            "BestFightOdds",
                            name,
                            bfo_id,
                        ):
                            writer_names.writerow([id_, "BestFightOdds", name, bfo_id])

                else:
                    logger.info(
                        f"{fighters_scraped} out of {fighters_to_scrape} fighters - Error"
                    )

        for _ in range(self.n_sessions):
            task_queue.put(None)

        for worker in workers:
            worker.join()

        logger.info("Finished scraping BFO odds.")
        logger.info("Scraped {} records".format(records_added))

        if records_added != 0:
            logger.info("Rerunning scraping to fill in missing records...")
            self.load_data()
            self.fighter_names.load_data()
            self.scrape_BFO_odds()
        else:
            logger.info("0 records scraped, unable to add more by rerunning scraping.")

        self.fighter_names.remove_duplicates_from_file()
        self.remove_duplicates_from_file()


class FighterNames(BaseFileHandler):
    """A class to manage and handle fighter names and their associated data.

    This class is responsible for loading and checking records of fighters
    from various data sources. It manages the storage of fighter information,
    including IDs, names, and database identifiers.
    """

    dtypes: Dict[str, type | pd.core.arrays.integer.Int64Dtype] = {
        "fighter_id": str,
        "database": str,
        "name": str,
        "database_id": str,
    }
    sort_fields = ["database", "name", "fighter_id"]
    data = pd.DataFrame({col: pd.Series(dtype=dt) for col, dt in dtypes.items()})
    filename = "fighter_names.csv"

    def check_missing_records(self) -> None:
        """Check for missing records in the UFCStats data.

        Compares the current data with the UFCStats data to identify
        missing records. Appends new records to the CSV file if any are found.
        """
        logger.info("Checking missing records...")
        ufc_stats_data = self.get_ufcstats_data()

        # Remove existing records from dataframe
        existing_records = self.data[self.data["database"] == "UFCStats"][
            "fighter_id"
        ].tolist()

        missing_records = ufc_stats_data[
            ~ufc_stats_data["fighter_id"].isin(existing_records)
        ]

        if len(missing_records) > 0:
            logger.info("Missing UFCStats records for some fighters, recomputing...")

            with open(self.data_file, "a+") as f:
                writer = csv.writer(f)
                for fighter_id, name in ufc_stats_data[
                    ["fighter_id", "fighter_name"]
                ].values:
                    writer.writerow([fighter_id, "UFCStats", name, fighter_id])

            print()
            logger.info("Reloading data after adding missing records")
            self.load_data()

    def fighter_in_database(
        self, fighter_id: str, database: str, name: str, database_id: str
    ) -> bool:
        """Check if a fighter is present in the database.

        Args:
            fighter_id: The ID of the fighter.
            database: The name of the database.
            name: The name of the fighter.
            database_id: The ID of the fighter in the database.

        Returns:
            True if the fighter is in the database, False otherwise.
        """
        return bool(
            (
                (self.data["fighter_id"] == fighter_id)
                & (self.data["database"] == database)
                & (self.data["name"] == name)
                & (self.data["database_id"] == database_id)
            ).any()
        )

    def get_ufcstats_data(self) -> pd.DataFrame:
        """Retrieve and prepare UFCStats data.

        Loads and processes data from UFCStats, including fights, and
        fighter details. Merges the data to create a comprehensive DataFrame.

        This can be later compared with the data in the CSV file to identify
        missing records.

        Returns:
            DataFrame containing processed UFCStats data.
        """
        logger.info("Loading UFCStats data...")
        ufc_stats_data = UFCScraper(self.data_folder)

        fights = ufc_stats_data.fight_scraper.data

        fighters_object = ufc_stats_data.fighter_scraper
        fighters_object.add_name_column()
        fighters = fighters_object.data

        data = pd.concat(
            [
                fights.rename(
                    columns={"fighter_1": "opponent_id", "fighter_2": "fighter_id"}
                ),
                fights.rename(
                    columns={"fighter_2": "opponent_id", "fighter_1": "fighter_id"}
                ),
            ]
        )

        fighter_fields = ["fighter_id", "fighter_name", "fighter_nickname"]
        data = data.merge(
            fighters[fighter_fields],
            on="fighter_id",
            how="left",
        )
        return data
