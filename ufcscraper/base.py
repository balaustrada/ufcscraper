from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import TYPE_CHECKING
from abc import ABC, abstractmethod

import pandas as pd


if TYPE_CHECKING:  # pragma: no cover
    from typing import List, Optional

logger = logging.getLogger(__name__)


class BaseFileHandler(ABC):
    columns: List[str]
    data_folder: Path
    filename: str

    data = pd.DataFrame([])

    def __init__(
        self,
        data_folder: Path | str,
    ):
        self.data_folder = Path(data_folder)
        self.data_file: Path = Path(self.data_folder) / self.filename

        self.check_data_file()
        self.remove_duplicates_from_file()
        self.load_data()

    def check_data_file(self) -> None:
        if not self.data_file.is_file():
            with open(self.data_file, "w", newline="", encoding="UTF8") as f:
                writer = csv.writer(f)
                writer.writerow(self.columns)

            logger.info(f"Using new file:\n\t{self.data_file}")
        else:
            logger.info(f"Using existing file:\n\t{self.data_file}")

    def remove_duplicates_from_file(self) -> None:
        data = pd.read_csv(self.data_file).drop_duplicates()
        data.to_csv(self.data_file, index=False)

    def load_data(self) -> None:
        self.data = pd.read_csv(self.data_file).drop_duplicates()


class BaseScraper(BaseFileHandler):
    web_url: str = "http://www.ufcstats.com"
    n_sessions: int = 1  # These are defaults
    delay: float = 0.1

    def __init__(
        self,
        data_folder: Path | str,
        n_sessions: Optional[int] = None,
        delay: Optional[float] = None,
    ):
        """
        filename: name of the csv file
        n_sessions: number of concurrent sessions
        delay: delay between requests to avoid being blocked
        """
        super().__init__(data_folder)
        self.n_sessions = n_sessions or self.n_sessions
        self.delay = delay or self.delay

    @staticmethod
    def id_from_url(url: str) -> str:
        if url[-1] == "/":
            return BaseScraper.id_from_url(url[:-1])

        return url.split("/")[-1]
