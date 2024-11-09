""" Base modules for ufc scraper

This module defines BaseFileHandler and BaseScraper classes,
meant to be inherited by specific scraper or file handler 
modules.
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import TYPE_CHECKING
from abc import ABC

import pandas as pd


if TYPE_CHECKING:  # pragma: no cover
    from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class BaseFileHandler(ABC):
    """Base class for file handlers associated with a CSV table.

    This class provides the basic functionality to manage data stored in a CSV
    file. It handles checking the existence of the file, initializing it with
    columns if it's missing, removing duplicates, and loading the data into a
    pandas DataFrame.

    Attributes:
        dtypes: A dictionary mapping column names to their data types.
        sort_fields: A list of column names used for sorting the data.
        data_folder: The folder where the CSV file is stored.
        filename: The name of the CSV file. This should be defined in subclasses.
        data: A pandas DataFrame that holds the data loaded from the CSV file.
    """

    dtypes: Dict[str, type | pd.core.arrays.integer.Int64Dtype]
    sort_fields: List[str]
    data_folder: Path
    filename: str

    data = pd.DataFrame([])

    def __init__(
        self,
        data_folder: Path | str,
    ):
        """Initializes the BaseFileHandler with the specified data folder.

        Args:
            data_folder (Path | str): The folder where the CSV file is stored
            or will be created.
        """
        self.data_folder = Path(data_folder)
        self.data_file: Path = Path(self.data_folder) / self.filename

        self.check_data_file()
        self.load_data()

    def check_data_file(self) -> None:
        """Checks if the CSV file exists in the specified data folder.

        If the file does not exist, it creates a new file with the specified columns.
        Logs the status of the file (whether new or existing) using the logger.
        """
        if not self.data_file.is_file():
            with open(self.data_file, "w", newline="", encoding="UTF8") as f:
                writer = csv.writer(f)
                writer.writerow(self.dtypes.keys())

            logger.info(f"Using new file:\n\t{self.data_file}")
        else:
            logger.info(f"Using existing file:\n\t{self.data_file}")

    def remove_duplicates_from_file(self) -> None:
        """Removes duplicate rows from the CSV file.

        This method reads the CSV file, removes any duplicate rows, and then
        saves the cleaned data back to the same file.
        """
        date_columns = [
            col for col, dtype in self.dtypes.items() if dtype == "datetime64[ns]"
        ]
        non_date_types = {
            col: dtype
            for col, dtype in self.dtypes.items()
            if dtype != "datetime64[ns]"
        }
        data = pd.read_csv(
            self.data_file, dtype=non_date_types, parse_dates=date_columns
        ).drop_duplicates()
        data = data.sort_values(by=self.sort_fields).reset_index(drop=True)
        data.to_csv(self.data_file, index=False)

    def load_data(self) -> None:
        """Loads the data from the CSV file into the `data` DataFrame.

        This method reads the CSV file, removes duplicates, and stores the data
        in the `data` attribute for further processing.
        """
        date_columns = [
            col for col, dtype in self.dtypes.items() if dtype == "datetime64[ns]"
        ]
        non_date_types = {
            col: dtype
            for col, dtype in self.dtypes.items()
            if dtype != "datetime64[ns]"
        }
        self.data = pd.read_csv(
            self.data_file, dtype=non_date_types, parse_dates=date_columns
        ).drop_duplicates()


class BaseScraper(BaseFileHandler):
    """Base class for web scrapers associated with a CSV file.

    This class provides basic functionality for scraping data from specific
    webs and storing it in a CSV file. It includes default settings for web
    scraping such as the base URL, the number of concurrent sessions, and
    the delay between requests.

    Attributes:
        web_url: The base URL for the website to scrape.
        n_sessions: Number of concurrent sessions for scraping.
        delay: Delay between requests to avoid being blocked.
    """

    web_url: str = "http://www.ufcstats.com"
    n_sessions: int = 1  # these are the defaults
    delay: float = 0.1

    def __init__(
        self,
        data_folder: Path | str,
        n_sessions: Optional[int] = None,
        delay: Optional[float] = None,
    ):
        """Initializes the BaseScraper with the specified parameters.

        Args:
            n_sessions: Number of concurrent sessions for scraping. If not
                provided, defaults to the class attribute.
            delay: Delay between requests to avoid being blocked. If not
                provided, defaults to the class attribute.
        """
        super().__init__(data_folder)
        self.n_sessions = n_sessions or self.n_sessions
        self.delay = delay or self.delay

    @staticmethod
    def id_from_url(url: str) -> str:
        """Extracts and returns the ID from a given URL.

        Args:
            url: The URL from which to extract the ID.

        Returns:
            The extracted ID as a string.
        """
        if url[-1] == "/":
            return BaseScraper.id_from_url(url[:-1])

        return url.split("/")[-1]
