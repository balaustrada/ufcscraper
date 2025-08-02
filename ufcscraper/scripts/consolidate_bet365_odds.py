"""
This script will read raw Bet365 odds data and consolidate it into a structured format.
It will create a CSV file with the consolidated odds data, ensuring that it is ready 
for further analysis or integration into a larger dataset.

Usage
------
To run the script, use the following command:

.. code-block:: bash

    ufcscraper_consolidate_bet365_odds --data-folder /path/to/data 

Arguments:
-----------

- **data-folder**: Specify the folder where the raw Bet365 odds data is stored.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from ufcscraper.odds_scraper.bet365_odds_reader import Bet365Odds

if TYPE_CHECKING:
    from typing import Optional

def get_args() -> argparse.Namespace:
    """
    Parse command-line arguments and return them as an `argparse.Namespace` object.

    This function sets up the command-line argument parser and defines the arguments
    that can be passed to the script. It returns the parsed arguments as an `argparse.Namespace` object.
    """ 
    parser = argparse.ArgumentParser(description="Consolidate Bet365 odds data into a structured CSV file.")
    
    parser.add_argument(
        "--data-folder",
        type=Path,
        required=True,
        help="The folder where the raw Bet365 odds data is stored.",
    )

    parser.add_argument(
        "--max-date-diff-days",
        type=int,
        default=3,
        help="Maximum number of days difference between fight date and odds date to consider for consolidation.",
    )

    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"],
        help="Set the logging level.",
    )

    parser.add_argument(
        "--min-match-score",
        type=int,
        default=90,
        help="Minimum match score for fuzzy matching of fighter names.",
    )

    
    return parser.parse_args()

def main(args: Optional[argparse.Namespace] = None) -> None:
    """
    Main function to run the script.

    This function initializes the logging, parses command-line arguments,
    and calls the consolidation function to process the Bet365 odds data.

    Args:
        args (Optional[argparse.Namespace]): Parsed command-line arguments.
    """
    if args is None:
        args = get_args()

    logging.basicConfig(
        stream=sys.stdout,
        level=args.log_level,
        format="%(levelname)s:%(message)s",
    )

    from ufcscraper.odds_scraper.bet365_odds_reader import Bet365OddsReader

    bet365_odds_reader = Bet365Odds(args.data_folder)

    bet365_odds_reader.consolidate_odds(
        max_date_diff_days=args.max_date_diff_days,
        min_match_score=args.min_match_score,
    )

if __name__ == "__main__":
    main()