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


def main(args: Optional[argparse.Namespace] = None) -> None:
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

    parser.add_argument("--n-sessions", type=int, default=1, help="Number of sessions.")

    parser.add_argument("--delay", type=int, default=0, help="Delay between requests.")

    parser.add_argument("--min-date", type=str, default="2007-08-01")

    return parser.parse_args()


if __name__ == "__main__":
    main()
