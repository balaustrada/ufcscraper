from __future__ import annotations

import argparse
from typing import TYPE_CHECKING
from pathlib import Path

import logging
import sys

if TYPE_CHECKING:
    from typing import Optional

logger = logging.getLogger(__name__)

from ufcscraper.odds_scraper import Bet365OddsReader

def main(args: Optional[argparse.Namespace] = None) -> None:
    if args is None:
        args = get_args()

    logging.basicConfig(
        stream=sys.stdout,
        level=args.log_level,
        format="%(levelname)s:%(message)s",
    )

    reader = Bet365OddsReader(
        html_file=args.file,
        data_folder=args.data_folder,
    )

    reader.scrape_odds(locales=args.locales)

    
def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read Bet365 odds data from a HTML file and save it to a CSV file."
    )
    parser.add_argument(
        "file",
        type=Path,
        help="Path to the HTML file containing the Bet365 odds data.",
    )

    parser.add_argument(
        "--data-folder",
        type=Path,
        default=Path("data"),
        help="Folder where the CSV file will be saved.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL).",
    )

    parser.add_argument(
        "--locales",
        type=list,
        default=["en_US.utf8", "es_ES.utf8"],
        help="Locales to use for parsing numbers and dates.",
    )

    return parser.parse_args()