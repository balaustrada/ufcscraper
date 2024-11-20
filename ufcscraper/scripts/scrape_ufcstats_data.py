"""
This script is designed to scrape UFC Stats fighter, events, fight and round data, organizing it into structured CSV files.

Usage:
------

To run the script, use the following command:

.. code-block:: bash

    ufcscraper_scrape_ufcstats_data --data-folder /path/to/data --log-level INFO --n-sessions 5 --delay 2

Arguments:
----------

- **log-level**: Set the logging level (e.g., INFO, DEBUG).
- **data-folder**: Specify the folder where scraped data will be stored.
- **n-sessions**: Number of concurrent scraping sessions (default: 1).
- **delay**: Delay in seconds between requests (default: 0).
"""

from __future__ import annotations

import argparse
from typing import TYPE_CHECKING
from pathlib import Path

import logging
import sys

if TYPE_CHECKING:
    from typing import Optional

logger = logging.getLogger(__name__)

from ufcscraper.ufc_scraper import UFCScraper


def main(args: Optional[argparse.Namespace] = None) -> None:
    """
    This function sets up logging, parses command-line arguments
    (if not provided), initializes a `UFCScraper` instance, performs
    scraping of fighters, events, and fights, and removes duplicates
    from the CSV files.

    Args:
        args: Command-line arguments. If None, arguments are parsed
            using `get_args`.
    """
    if args is None:
        args = get_args()

    logging.basicConfig(
        stream=sys.stdout,
        level=args.log_level,
        format="%(levelname)s:%(message)s",
    )

    scraper = UFCScraper(
        data_folder=args.data_folder,
        n_sessions=args.n_sessions,
        delay=args.delay,
    )

    logger.info("")
    logger.info("Scraping fighters...")
    scraper.fighter_scraper.scrape_fighters()

    logger.info("")
    logger.info("Scraping events...")
    scraper.event_scraper.scrape_events()

    logger.info("")
    logger.info(f"Scraping fights...")
    scraper.fight_scraper.scrape_fights()


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

    return parser.parse_args()


if __name__ == "__main__":
    main()
