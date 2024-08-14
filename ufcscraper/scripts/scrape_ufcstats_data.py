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
    Main entry point for the script.

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

    logger.info("")
    logger.info(f"Removing duplicates from CSV files...")
    scraper.remove_duplicates_from_file()


def get_args() -> argparse.Namespace:
    """
    Parse command-line arguments and return them as an `argparse.Namespace` object.

    Returns:
        argparse.Namespace: Parsed command-line arguments.
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
