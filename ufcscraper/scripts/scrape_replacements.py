"""
Script to scrape late replacement fights from BetMMA.tips.

Usage:
    ufcscraper_scrape_replacements --data-folder /path/to/data --log-level INFO

This will instantiate the ReplacementScraper and run scrape_replacements().
"""

from __future__ import annotations

import argparse
from pathlib import Path
import logging
import sys

from ufcscraper.replacement_scraper import ReplacementScraper


def main(args: argparse.Namespace | None = None) -> None:
    if args is None:
        args = get_args()

    logging.basicConfig(stream=sys.stdout, level=args.log_level, format="%(levelname)s:%(message)s")

    scraper = ReplacementScraper(data_folder=args.data_folder)
    scraper.scrape_replacements()


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", default="INFO", choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"]) 
    parser.add_argument("--data-folder", type=Path, help="Folder where scraped data will be stored.")
    return parser.parse_args()


if __name__ == "__main__":
    main()
