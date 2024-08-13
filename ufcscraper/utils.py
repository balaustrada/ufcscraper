from __future__ import annotations

import logging
import multiprocessing
import re
import time
from typing import TYPE_CHECKING

import bs4
import requests
from dateutil import parser
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

if TYPE_CHECKING:
    import datetime
    from typing import Callable, Generator, List, Optional, Tuple, TypeVar, Any
    from selenium import webdriver
    from selenium.webdriver.remote.webelement import WebElement

    T = TypeVar("T")

logger = logging.getLogger(__name__)


def get_session() -> requests.Session:
    retry_strategy = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry on
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)

    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


def links_to_soups(
    urls: List[str], n_sessions: int = 1, delay: float = 0
) -> Generator[Tuple[str, bs4.BeautifulSoup]]:
    task_queue: multiprocessing.Queue = multiprocessing.Queue()
    result_queue: multiprocessing.Queue = multiprocessing.Queue()

    urls_scraped = 0
    urls_to_scrape = len(urls)
    # Adding tasks
    for url in urls:
        task_queue.put((url,))

    # Define worker around link_to_soup
    worker_target = worker_constructor(
        lambda x, session: (x, link_to_soup(x, session, delay))
    )

    sessions = [get_session() for _ in range(n_sessions)]
    # Starting workers
    workers = [
        multiprocessing.Process(
            target=worker_target,
            args=(task_queue, result_queue, session),
        )
        for session in sessions
    ]

    for worker in workers:
        worker.start()

    try:
        while urls_scraped < urls_to_scrape:
            result = result_queue.get()
            urls_scraped += 1

            if result is not None:
                yield result
    finally:
        for session in sessions:
            session.close()
            task_queue.put(None)

        for worker in workers:
            worker.join()


def link_to_soup(
    url: str, session: Optional[requests.Session] = None, delay: float = 0
) -> bs4.BeautifulSoup:
    """
    Given a url, return the BeautifulSoup object

    :param url: The url to scrape
    :param session: The requests session
    :param delay: The delay to wait before scraping

    :return: The BeautifulSoup object
    """
    if delay > 0:
        time.sleep(delay)

    if session is None:
        session = get_session()
        soup = bs4.BeautifulSoup(session.get(url).text, "lxml")
        session.close()
        return soup
    else:
        return bs4.BeautifulSoup(session.get(url).text, "lxml")


def worker_constructor(
    method: Callable[..., T],
    max_exception_retries: int = 4,
) -> Callable[[multiprocessing.Queue, multiprocessing.Queue, requests.Session], None]:
    def worker(
        task_queue: multiprocessing.Queue,
        result_queue: multiprocessing.Queue,
        session: requests.Session,
    ) -> None:
        while True:
            try:
                task = task_queue.get()
                if task is None:
                    break

                args = task
                result: Optional[T] = None

                for attempt in range(max_exception_retries + 1):
                    try:
                        result = method(*args, session)
                        result_queue.put(result)
                        break
                    except Exception as e:
                        logging.error(
                            f"Attempt {attempt + 1} failed for task {task}: {e}"
                        )
                        logging.exception("Exception occurred")

                        # Reset the driver after a failed attempt
                        session.close()
                        session = get_session()

            except Exception as e:
                logging.error(f"Error processing task {task}: {e}")
                logging.exception("Exception ocurred")

                # Reset the driver after a failed attempt
                session.close()
                session = get_session()

                # Send None to the result because task failed
                result_queue.put(None)

    return worker


class element_present_in_list(object):
    """
    Custom function to check if an element is present in a list of elements
    """

    def __init__(self, *locators: Tuple[str, str]):
        self.locators = locators

    def __call__(self, driver: webdriver.Chrome) -> bool | List[WebElement]:
        for locator in self.locators:
            elements = driver.find_elements(*locator)
            if elements:
                return elements
        return False


def clean_date_string(date_str: str) -> str:
    """
    Clean the date string to be parsed into a datetime object

    Args:
        date_str (str): The date string to be cleaned

    Returns:
        str: The cleaned date string
    """
    # Replace incorrect ordinal suffixes
    date_str = re.sub(r"(\d)(nd|st|rd|th)", r"\1", date_str)
    return date_str


def parse_date(date_str: str) -> Optional[datetime.date]:
    """
    Parse the date string into a datetime object

    Args:
        date_str (str): The date string to be parsed

    Returns:
        date: The parsed date object
    """
    # Clean the date string
    cleaned_date_str = clean_date_string(date_str)

    # Parse the cleaned date string into a datetime object
    try:
        date_obj = parser.parse(cleaned_date_str)
        return date_obj.date()
    except ValueError as e:
        print(f"Error parsing date: {e}")
        return None
