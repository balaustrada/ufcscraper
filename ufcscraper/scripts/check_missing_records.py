"""
This is script is designed to check the different tables and require actions to be taken
to ensure the data is consistent and complete.

Usage:
------

To run the script, use the following command:

.. code-block:: bash
    ufcscraper_data_checks --data-folder /path/to/data --log-level INFO

Arguments:
----------
- **log-level**: Set the logging level (e.g., INFO, DEBUG).
- **data-folder**: Specify the folder where scraped data is stored.
"""
from __future__ import annotations

import argparse

from typing import TYPE_CHECKING
from pathlib import Path
import logging
import pandas as pd
import sys

from ufcscraper.odds_scraper import BestFightOddsScraper
from ufcscraper.ufc_scraper import UFCScraper

if TYPE_CHECKING:
    from typing import Optional

logger = logging.getLogger(__name__)

def get_args() -> argparse.Namespace:
    """
    Parse command-line arguments and return them as an `argparse.Namespace` object.

    This function sets up the command-line argument parser and defines the arguments
    that can be passed to the script. It returns the parsed arguments as an 
    `argparse.Namespace` object.

    Returns:
        argparse.Namespace: The parsed command-line arguments, with attributes for each argument.
            - `log_level` (str): The logging level (default: "INFO").
            - `data_folder` (Path): The folder where scraped data is stored.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--log-level",
        default="WARNING",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"],
    )
    parser.add_argument(
        "--data-folder",
        type=Path,
        required=True,
        help="Folder where scraped data is stored.",
    )

    return parser.parse_args()


def main(args: Optional[argparse.Namespace] = None) -> None:
    """
    Main function to run the data checks script.

    This function initializes logging, parses command-line arguments, and runs the data checks.
    If no arguments are provided, it uses default values.

    Args:
        args (Optional[argparse.Namespace]): Command-line arguments. If None, default arguments are used.
    """
    if args is None:
        args = get_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
    )

    # Checking records with missing odds.
    scraper = UFCScraper(data_folder=args.data_folder)
    bfo_scraper = BestFightOddsScraper(
        data_folder=args.data_folder,
        n_sessions=-1,
    )

    # Concat scraper.fight_scraper.data with itself, selecting fighter id first 
    # fighter_1 and then fighter_2.
    fight_data = scraper.fight_scraper.data.copy()
    fight_data = pd.concat(
        [
            fight_data[["fight_id", "fighter_1", "event_id"]].rename(
                columns={"fighter_1": "fighter_id"}),
            fight_data[["fight_id", "fighter_2", "event_id"]].rename(
                columns={"fighter_2": "fighter_id"}),
        ],
        ignore_index=True,
    )

    fight_data = fight_data.merge(
        bfo_scraper.data,
        left_on=["fight_id", "fighter_id"],
        right_on=["fight_id", "fighter_id"],
        how="left",
    )
    fight_data = fight_data.merge(
        scraper.event_scraper.data[["event_id", "event_date"]],
        on="event_id",
    )

    # Only report fights with missing odds after the first fight with odds.
    msk_missing_odds = (
        fight_data["opening"].isna() |
        fight_data["closing_range_min"].isna() |
        fight_data["closing_range_max"].isna()
    )

    first_odds_date = fight_data[~msk_missing_odds]["event_date"].min()
    msk_missing_odds_after = msk_missing_odds & (fight_data["event_date"] >= first_odds_date)
    
    # Report the values fulfilling the condition.
    missing_odds = fight_data[msk_missing_odds_after].copy()

    if len(missing_odds) > 0:
        logger.warning(
            "Found %d fights with missing odds after the first fight with odds (%s). "
            "Please check the following records:",
            len(missing_odds),
            first_odds_date,
        )
        for _, row in missing_odds.iterrows():
            logger.warning(
                "Fight ID: %s, Event ID: %s, Event Date: %s, Fighter ID: %s",
                row["fight_id"],
                row["event_id"],
                row["event_date"],
                row["fighter_id"],
            )


    # Checking missing catch weights
    valid_weights = [
        "Bantamweight",
        "Featherweight",
        "Flyweight",
        "Heavyweight",
        "Light Heavyweight",
        "Lightweight",
        "Middleweight",
        "Welterweight",
        "Women's Bantamweight",
        "Women's Featherweight",
        "Women's Flyweight",
        "Women's Strawweight",
    ]

    scraper = UFCScraper(data_folder=args.data_folder)
    fight_data = scraper.fight_scraper.data.merge(
        scraper.catch_weights.data,
        on="fight_id",
        how="left",
    )
    mask = fight_data["catch_weight"].notna()
    fight_data.loc[mask, "weight_class"] = fight_data.loc[mask, "catch_weight"]
    fight_data.drop(columns="catch_weight", inplace=True)
    fight_data = fight_data.merge(
        scraper.event_scraper.data[["event_id", "event_date"]],
        on="event_id",
    )

    fight_data = fight_data[fight_data["event_date"] >= first_odds_date]

    msk_valid = fight_data["weight_class"].isin(valid_weights)
    msk_valid |= pd.to_numeric(fight_data["weight_class"], errors="coerce").notna()

    invalid = fight_data[~msk_valid].copy()

    if len(invalid) > 0:
        logger.warning(
            "Found %d invalid catch weights. Please check the following records:",
            len(invalid),
        )
        for _, row in invalid.iterrows():
            logger.warning(
                "Fight ID: %s, Event ID: %s, Event Date: %s, Weight Class: %s",
                row["fight_id"],
                row["event_id"],
                row["event_date"],
                row["weight_class"],
            )



if __name__ == "__main__": # pragma: no cover
    args = get_args()
    main(args)