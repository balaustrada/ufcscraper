from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd


if TYPE_CHECKING: # pragma: no cover
    from typing import List, Optional 

logger = logging.getLogger(__name__)


class BaseScraper:
    web_url: str = "http://www.ufcstats.com"
    columns: List[str]
    data_folder: Path
    n_sessions: int = 8  # These are defaults
    delay: float = 0.1
    filename: str
    data = pd.DataFrame([])

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
        self.data_folder = Path(data_folder)
        self.data_file: Path = Path(self.data_folder) / self.filename
        self.n_sessions = n_sessions or self.n_sessions
        self.delay = delay or self.delay

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

    @staticmethod
    def id_from_url(url: str) -> str:
        if url[-1] == "/":
            return BaseScraper.id_from_url(url[:-1])

        return url.split("/")[-1]
