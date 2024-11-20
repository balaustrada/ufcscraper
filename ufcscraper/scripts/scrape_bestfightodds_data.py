"""
This script is designed to scrape BestFightOdds betting odds, organizing the data into structured CSV files.

Usage:
------

To run the script, use the following command:

.. code-block:: bash

    ufcscraper_scrape_bestfightodds_data --data-folder /path/to/data --log-level INFO --n-sessions 5 --delay 2 --min-date 2010-01-01

Arguments:
----------

- **log-level**: Set the logging level (e.g., INFO, DEBUG).
- **data-folder**: Specify the folder where scraped data will be stored.
- **n-sessions**: Number of concurrent scraping sessions (default: 1).
- **delay**: Delay in seconds between requests (default: 0).
- **min-date**: Minimum date for data scraping (default: 2007-08-01).
"""

from __future__ import annotations

import argparse
from typing import TYPE_CHECKING
from pathlib import Path
import datetime
import logging
import sys

if TYPE_CHECKING:
    from typing import Optional

logger = logging.getLogger(__name__)

from ufcscraper.odds_scraper import BestFightOddsScraper


def get_args() -> argparse.Namespace:
    """
    Parse command-line arguments and return them as an `argparse.Namespace` object.

    This function sets up the command-line argument parser and defines the arguments
    that can be passed to the script. It returns the parsed arguments as an `argparse.Namespace` object.

    Returns:
        argparse.Namespace: The parsed command-line arguments, with attributes for each argument.
            - `log_level` (str): The logging level (default: "INFO").
            - `data_folder` (Path): The folder where scraped data will be stored.
            - `n_sessions` (int): The number of concurrent sessions (default: 1).
            - `delay` (int): The delay between requests in seconds (default: 0).
            - `min_date` (str): The minimum date for data scraping in "YYYY-MM-DD" format (default: "2007-08-01").
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"],
    )

    parser.add_argument(
        "--data-folder", type=Path, help="Folder where scraped data will be stored."
    )

    parser.add_argument("--n-sessions", type=int, default=1, help="Number of sessions.")

    parser.add_argument("--delay", type=int, default=0, help="Delay between requests.")

    parser.add_argument("--min-date", type=str, default="2007-08-01")

    return parser.parse_args()


def main(args: Optional[argparse.Namespace] = None) -> None:
    """
    This function sets up logging, parses command-line arguments
    (if not provided), initializes a `BestFightOddsScraper` instance,
    performs scraping of odds, filling the fighter_names table if needed
    and removes duplicates from the CSV files.

    Args:
        args (Optional[argparse.Namespace]): An optional `argparse.Namespace` object containing command-line arguments.
            If not provided, `get_args()` is called to parse the arguments.
    """
    if args is None:
        args = get_args()

    logging.basicConfig(
        stream=sys.stdout,
        level=args.log_level,
        format="%(levelname)s:%(message)s",
    )

    min_date = datetime.datetime.strptime(args.min_date, "%Y-%m-%d").date()
    scraper = BestFightOddsScraper(
        data_folder=args.data_folder,
        n_sessions=args.n_sessions,
        delay=args.delay,
        min_date=min_date,
    )
    scraper.scrape_BFO_odds()


if __name__ == "__main__":
    main()
