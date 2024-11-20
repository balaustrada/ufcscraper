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
    """
    Create and configure a new `requests.Session` object with retry functionality.

    Returns:
        requests.Session: A configured session object with retry strategy.
    """
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
    """Parse the HTML content from given URLs into a BeautifulSoup objects.

    Create a generator that yields tuples of URLs and their corresponding
    BeautifulSoup objects.

    This function uses multiple processes to fetch and parse web pages
    concurrently.

    Args:
        urls: List of URLs to be scraped.
        n_sessions: Number of concurrent sessions to use
            for scraping. Defaults to 1.
        delay: Delay in seconds to wait before making each
            request. Defaults to 0.

    Returns:
        Tuples containing the URL and the corresponding BeautifulSoup object.
    """
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
    """Parse the HTML content of a given URL into a BeautifulSoup object.

    Args:
        url: The URL to scrape.
        session: A requests session object. If not provided, a new session
            will be created.
        delay: Delay in seconds before making the request.

    Returns:
        Parsed BeautifulSoup object containing the HTML content of the page.
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
    """Create a worker target function for processing tasks with retry functionality.

    Args:
        method: The function to be executed by the worker.
        max_exception_retries : Maximum number of retries for handling exceptions.

    Returns:
        A worker function that processes tasks from a queue and puts results in
            another queue.
    """

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
    """Callable to check if an element is present in a list of elements on a web page.

    Attributes:
        locators (Tuple[str, str]): Locators used to find elements on the page.
    """

    def __init__(self, *locators: Tuple[str, str]):
        """Initialize the element_present_in_list class.

        Args:
            locators: List of all locators used to find elements on the page.
        """
        self.locators = locators

    def __call__(self, driver: webdriver.Chrome) -> bool | List[WebElement]:
        """Check if any elements matching the locators are present on the page.

        Args:
            driver: The WebDriver instance used to interact with the web page.

        Returns:
            True if elements are found, otherwise False. If elements are found,
                returns the list of WebElements.
        """
        for locator in self.locators:
            elements = driver.find_elements(*locator)
            if elements:
                return elements
        return False


def clean_date_string(date_str: str) -> str:
    """
    Clean a date string to remove incorrect ordinal suffixes and make it
        suitable for parsing.

    Args:
        date_str (str): The date string to be cleaned.

    Returns:
        str: The cleaned date string.
    """
    # Replace incorrect ordinal suffixes
    date_str = re.sub(r"(\d)(nd|st|rd|th)", r"\1", date_str)
    return date_str


def parse_date(date_str: str) -> Optional[datetime.date]:
    """Parse a date string into a `datetime.date` object.

    Args:
        date_str (str): The date string to be parsed.

    Returns:
        Optional[datetime.date]: The parsed date object if successful,
            otherwise None.
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
