from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import bs4

if TYPE_CHECKING:
    from typing import Optional

logger = logging.getLogger(__name__)

from ufcscraper.odds_reader import (
    Bet365OddsReader,
    WilliamHillOddsReader,
    BetwayOddsReader,
    BwinOddsReader,
)
from ufcscraper.utils import extract_most_common_domain


def main(args: Optional[argparse.Namespace] = None) -> None:
    if args is None:
        args = get_args()

    logging.basicConfig(
        stream=sys.stdout,
        level=args.log_level,
        format="%(levelname)s:%(message)s",
    )

    # Load html file into soup and check most common url
    soup = bs4.BeautifulSoup(open(args.file, "r", encoding="utf-8"), "lxml")
    most_common_domain = extract_most_common_domain(soup)

    if "bet365" in most_common_domain:
        logger.info("Detected Bet365 odds data.")
        Reader = Bet365OddsReader
    elif "williamhill" in most_common_domain:
        logger.info("Detected William Hill odds data.")
        Reader = WilliamHillOddsReader
    elif "betway" in most_common_domain:
        logger.info("Detected Betway odds data.")
        Reader = BetwayOddsReader
    elif "bwin" in most_common_domain:
        logger.info("Detected Bwin odds data.")
        Reader = BwinOddsReader
    else:
        logger.error("Unknown odds data source. Please check the HTML file.")
        sys.exit(1)

    reader = Reader(
        html_file=args.file,
        data_folder=args.data_folder,
    )

    reader.scrape_odds(languages=args.languages)


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
        "--languages",
        type=list,
        default=["es",],
        help="Languages to use for parsing numbers and dates.",
    )

    return parser.parse_args()
