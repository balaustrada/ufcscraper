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

from ufcscraper.base import BaseScraper
from ufcscraper.ufc_scraper import UFCScraper
from ufcscraper.utils import element_present_in_list, parse_date

if TYPE_CHECKING:
    import datetime
    from typing import Any, Callable, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class BestFightOddsScraper(BaseScraper):
    columns: List[str] = [
        "fight_id",
        "fighter_id",
        "opening",
        "closing_range_min",
        "closing_range_max",
    ]
    data = pd.DataFrame(columns=columns)
    filename = "BestFightOdds_odds.csv"
    n_sessions = 1  # New default value
    min_score = 90
    max_exception_retries = 3
    web_url = "https://www.bestfightodds.com"

    def __init__(
        self,
        data_folder: Path | str,
        n_sessions: Optional[int] = None,
        delay: Optional[float] = None,
        min_score: Optional[int] = None,
        min_date: datetime.date = datetime.date(2008, 8, 1),
    ):
        super().__init__(data_folder, n_sessions, delay)

        # For this scraper it is better to not continuously reload the driver
        self.drivers = [webdriver.Chrome() for _ in range(self.n_sessions)]
        self.fighter_names = FighterNames(self.data_folder)
        self.min_score = min_score or self.min_score
        self.min_date = min_date

    @classmethod
    def create_search_url(cls, query: str) -> str:
        """
        Create the search url

        :param query: The name of the fighter

        :return: The search url
        """
        encoded_query = urllib.parse.quote_plus(query)
        search_url = f"{cls.web_url}/search?query={encoded_query}"

        return search_url

    def captcha_indicator(self, driver: webdriver.Chrome) -> bool:
        """
        Check if there is a captcha

        :param driver: The webdriver

        :return: True if there is a captcha else False
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

    def get_odds_from_profile_url(
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
    ) -> Tuple[multiprocessing.Queue, multiprocessing.Queue, List[multiprocessing.Process]]:
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

        # Define worker around get_odds_from_profile_url
        worker_target = self.worker_constructor_target(self.get_odds_from_profile_url)

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
        # Wait for profile table to be there
        table = WebDriverWait(driver, 60).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "team-stats-table"))
        )

        # Extract table
        soup = BeautifulSoup(
            table[0].get_attribute("innerHTML"),
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

        fighter_name:str = rows_f[0].select_one("a").get_text(strip=True)

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
        return f"{cls.web_url}/fighters/{id_}"

    def search_fighter_profile(
        self, search_fighter: str, driver: webdriver.Chrome
    ) -> Optional[Tuple[str, str]]:
        """
        Search for the given fighter and return the name and the url of the profile

        :param search_fighter: The name of the fighter
        :param driver: The webdriver

        :return: (name, url) if found else None
        """
        url = self.create_search_url(search_fighter)

        driver.get(url)

        while self.captcha_indicator(driver):
            logging.warning("Human recognition page detected, stalling..")
            time.sleep(5)

        # Three possible outputs
        element = WebDriverWait(driver, 60).until(
            element_present_in_list(
                (By.CLASS_NAME, "content-list"),  # Search result
                (By.CLASS_NAME, "team-stats-table"),  # Direct redirect to fighter page
                (By.CSS_SELECTOR, "p"),  # No results found.
                (By.ID, "hfmr8"),  # Captcha
            )
        )[0] # type: ignore[index]

        if element.get_attribute("id") == "hfmr8":
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
        fighter_names_aggregated.columns = [
            f"{col[1]}_{col[0]}" for col in fighter_names_aggregated.columns
        ]
        fighter_names_aggregated.columns = [
            col + "s" if col != "fighter_id" else col
            for col in fighter_names_aggregated.columns
        ]

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
                .apply(
                    lambda x: [] if not isinstance(x, list) and pd.isna(x) else x
                )
                .values
            )
            data["opponent_" + col] = (
                data["opponent_" + col]
                .apply(
                    lambda x: [] if not isinstance(x, list) and pd.isna(x) else x
                )
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

    @staticmethod
    def remove_scraped_records(data:pd.DataFrame, odds_data: pd.DataFrame) -> pd.DataFrame:
        return (
            data.merge(
                odds_data,
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
        

    def scrape_BFO_odds(self) -> None:
        self.fighter_names.check_missing_records()

        # Get data, this can have up to 4 entries for each fighter
        # x2 for each fighter
        # x2 for each database (UFCStats, BestFightOdds)
        ufc_stats_data = self.get_ufcstats_data()

        data_to_scrape = self.remove_scraped_records(ufc_stats_data, self.data)
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
                ids.append(fighter_id)
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

        result_queue, task_queue, workers = self.get_parallel_odds_from_profile_urls(
            ids, search_names, bfo_ids,
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
                    (
                        dates,
                        fighter_BFO_ids,
                        fighter_BFO_names,
                        opponents_BFO_ids,
                        opponents_BFO_names,
                        openings,
                        closing_range_mins,
                        closing_range_maxs,
                    ) = result

                    group = grouped_data.get_group(fighter_id)
                    new_BFO_names = set()

                    fighter_records = 0
                    for _, row in group.iterrows():
                        date = row["event_date"].date()

                        candidates_indxs = [
                            i
                            for i, candidate_date in enumerate(dates)
                            if abs((candidate_date - date).days) <= 1.5
                        ]

                        if len(candidates_indxs) == 0:
                            logger.info(
                                f"Unable to find opponent {row["opponent_UFC_names"][0]} for "
                                f"{row['UFC_names'][0]} on {date}"
                            )
                        else:
                            possible_opponents = [
                                opponents_BFO_names[i] for i in candidates_indxs
                            ]

                            scores = [
                                process.extractOne(
                                    opponent,
                                    possible_opponents,
                                    scorer=fuzz.token_sort_ratio,
                                )
                                for opponent in possible_opponents
                            ]

                            best_name, score = max(scores, key=lambda x: x[1])
                            best_index = opponents_BFO_names.index(best_name)

                            if score > self.min_score:
                                writer_odds.writerow(
                                    [
                                        row["fight_id"],
                                        row["fighter_id"],
                                        openings[best_index],
                                        closing_range_mins[best_index],
                                        closing_range_maxs[best_index],
                                    ]
                                )
                                fighter_records += 1

                                # We add the names as valid to be added if they are not in the database yet
                                new_BFO_names.add(
                                    (
                                        row["opponent_id"],
                                        opponents_BFO_ids[best_index],
                                        opponents_BFO_names[best_index],
                                    )
                                )
                                new_BFO_names.add(
                                    (
                                        row["fighter_id"],
                                        fighter_BFO_ids[best_index],
                                        fighter_BFO_names[best_index],
                                    )
                                )
                            else:
                                logger.info(
                                    f"Unable to find opponent {opponents_BFO_names} for "
                                    f"{row['UFC_names'][0]} on {date}"
                                )

                    logger.info(
                        f"{fighters_scraped} out of {fighters_to_scrape} fighters."
                        f"\n\t{fighter_records} records added"
                    )

                    # Check if the valid names are already in the database and if not, add them
                    for id_, bfo_id, name in new_BFO_names:
                        in_database = bool(
                            (
                                (self.fighter_names.data["fighter_id"] == id_)
                                & (
                                    self.fighter_names.data["database"]
                                    == "BestFightOdds"
                                )
                                & (self.fighter_names.data["database_id"] == bfo_id)
                                & (self.fighter_names.data["name"] == name)
                            ).any()
                        )

                        if not in_database:
                            writer_names.writerow([id_, "BestFightOdds", name, bfo_id])
                else:
                    logger.info(
                        f"{fighters_scraped} out of {fighters_to_scrape} fighters - Error"
                    )

        for _ in range(self.n_sessions):
            task_queue.put(None)

        for worker in workers:
            worker.join()


class FighterNames(BaseScraper):
    columns: List[str] = [
        "fighter_id",
        "database",
        "name",
        "database_id",
    ]
    data = pd.DataFrame(columns=columns)
    filename = "fighter_names.csv"

    def check_missing_records(self) -> None:
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

    def get_ufcstats_data(self) -> pd.DataFrame:
        logger.info("Loading UFCStats data...")
        ufc_stats_data = UFCScraper(self.data_folder)

        events_ = ufc_stats_data.event_scraper.data
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
