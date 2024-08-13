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
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"],
    )

    parser.add_argument(
        "--data-folder", type=Path, help="Folder where scraped data will be stored."
    )

    parser.add_argument("--n-sessions", type=int, default=8, help="Number of sessions.")

    parser.add_argument("--delay", type=int, default=0, help="Delay between requests.")

    return parser.parse_args()


if __name__ == "__main__":
    main()
