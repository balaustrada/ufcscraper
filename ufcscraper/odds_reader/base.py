from __future__ import annotations

from abc import ABC
import csv
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Tuple

from fuzzywuzzy import fuzz
from loguru import logger
import pandas as pd

from ufcscraper.base import BaseFileHandler, BaseHTMLReader
from ufcscraper.fighter_names import FighterNames
from ufcscraper.ufc_scraper import UFCScraper

if TYPE_CHECKING:
    from typing import Dict, List




class OddsReader(BaseHTMLReader):
    """
    Base class for reading odds data from HTML files.
    """

    dtypes: Dict[str, type | pd.core.arrays.integer.Int64Dtype] = {
        "html_datetime": "datetime64[ns]",
        "fight_date": "datetime64[ns]",
        "fighter_name": str,
        "opponent_name": str,
        "fighter_odds": float,
        "opponent_odds": float,
    }

    sort_fields = [
        "html_datetime",
        "fight_date",
        "fighter_name",
        "opponent_name",
        "fighter_odds",
        "opponent_odds",
    ]
    data = pd.DataFrame({col: pd.Series(dtype=dt) for col, dt in dtypes.items()})

    def __init__(self, html_file: Path | str, data_folder: Path | str):
        """
        Initializes the Bet365OddsReader with the specified data folder.

        Args:
            html_file (Path | str): The path to the HTML file containing the odds data.
            data_folder (Path | str): The folder where the CSV file is stored
            or will be created.
        """
        super().__init__(html_file=html_file, data_folder=data_folder)

    def read_odds(self) -> pd.DataFrame:
        """
        Reads odds data from the scraper and returns a DataFrame.
        """
        raise NotImplementedError("This method should be implemented by subclasses.")

    def write_odds(self, rows: List[Tuple[datetime, str, str, float, float]]) -> None:
        """
        Writes the odds data to a CSV file.

        Args:
            rows (List[Tuple[datetime, str, str, str, float, float]]): A list of tuples containing date, fighter name, opponent name, fighter odds, and opponent odds.
        """
        logger.info(f"Rows to be written: {len(rows)}")
        database_length = len(self.data)

        # Iterate over tuple and add prepending element self.html_datetime.strftime("%Y-%m-%d %H:%M:%S")

        final_rows = []
        for row in rows:
            final_rows.append(
                (
                    self.html_datetime.strftime("%Y-%m-%d %H:%M:%S"),
                    row[0].strftime("%Y-%m-%d"),
                )
                + row[1:]
            )

        with open(self.data_file, "a") as file:
            writer = csv.writer(file)
            for row in final_rows:
                writer.writerow(row)

        self.remove_duplicates_from_file()
        self.load_data()
        logger.info(f"Rows added to database: {len(self.data) - database_length}")


class BaseOdds(BaseFileHandler, ABC):
    """
    Base class for handling odds data associated with existing fights.
    """

    dtypes: Dict[str, type | pd.core.arrays.integer.Int64Dtype] = {
        "scrape_datetime": "datetime64[ns]",
        "betting_house": str,
        "fight_id": "datetime64[ns]",
        "fighter_id": str,
        "odds": float,
    }

    sort_fields = ["scrape_datetime", "betting_house", "fight_id", "fighter_id", "odds"]
    data = pd.DataFrame({col: pd.Series(dtype=dt) for col, dt in dtypes.items()})
    upcoming: bool

    def consolidate_odds(
        self, betting_house: str, max_date_diff_days: int = 3, min_match_score: int = 90
    ) -> None:
        """
        Read raw odds data and consolidate it into a structured format.

        Args:
            betting_house: The name of the betting house (e.g., "Bet365").
            max_date_diff_days: Maximum allowed difference in days between fight date and event date
            min_match_score: Minimum fuzzy match score to consider a name match valid
        """
        if betting_house.lower() not in [
            "bet365",
            "betway",
            "bwin",
            "casino888",
            "sportium",
            "williamhill",
        ]:
            raise ValueError(f"Unsupported betting house: {betting_house}")

        scraper = UFCScraper(self.data_folder)
        odds = pd.read_csv(
            self.data_folder / f"raw_odds/{betting_house.lower()}_odds_raw.csv"
        )

        fighter_data = scraper.fighter_scraper.data

        if self.upcoming:
            fight_data = scraper.upcoming_fight_scraper.data
            event_data = scraper.upcoming_event_scraper.data

        else:
            fight_data = scraper.fight_scraper.data
            fighter_data = scraper.fighter_scraper.data
            event_data = scraper.event_scraper.data

        fighter_data["fighter_full_name"] = (
            fighter_data["fighter_f_name"] + " " + fighter_data["fighter_l_name"]
        )

        fight_data = pd.concat(
            [
                fight_data[["fight_id", "event_id", "fighter_1"]].rename(
                    columns={"fighter_1": "fighter_id"}
                ),
                fight_data[["fight_id", "event_id", "fighter_2"]].rename(
                    columns={"fighter_2": "fighter_id"}
                ),
            ]
        )

        data = (
            fight_data.merge(
                fighter_data[["fighter_id", "fighter_full_name"]],
                on="fighter_id",
            ).merge(event_data[["event_id", "event_date"]], on="event_id")
        )[
            [
                "fight_id",
                "fighter_id",
                "fighter_full_name",
                "event_date",
            ]
        ]

        odds = pd.concat(
            [
                odds[["html_datetime", "fight_date", "fighter_name", "fighter_odds"]],
                odds[
                    ["html_datetime", "fight_date", "opponent_name", "opponent_odds"]
                ].rename(
                    columns={
                        "opponent_name": "fighter_name",
                        "opponent_odds": "fighter_odds",
                    }
                ),
            ]
        )

        odds["fight_date"] = pd.to_datetime(odds["fight_date"])

        # Map fight_dates to valid event_dates
        unique_event_date = data["event_date"].unique()
        date_mapping = {}
        unmatched_dates = set()
        for odd_date in pd.to_datetime(odds["fight_date"].unique()):
            closest = min(unique_event_date, key=lambda d: abs(odd_date - d))
            distance = abs(odd_date - closest).days

            if distance <= max_date_diff_days:
                date_mapping[odd_date] = closest
            else:
                unmatched_dates.add(odd_date)

        for unmatched_date in sorted(unmatched_dates):
            if (unmatched_date < datetime.now() and self.upcoming) or (
                unmatched_date > datetime.now() and not self.upcoming
            ):
                continue
            logger.warning(f"Unmatched fight at date {unmatched_date}")

        odds["fight_date"] = odds["fight_date"].map(date_mapping).astype("datetime64[ns]")
        odds = odds.rename(columns={"fight_date": "event_date"})

        # Create index to select best match
        odds = odds.reset_index().rename(columns={"index": "odds_row_id"})

        logger.info(f"Rows to be matched: {len(odds)}")

        # Merge odds with fight data
        merged = odds.merge(
            data,
            on="event_date",
        )
        # Compute fuzzy match score
        merged["match_score"] = merged.apply(
            lambda row: fuzz.token_set_ratio(
                row["fighter_name"], row["fighter_full_name"]
            ),
            axis=1,
        )
        best_matches = merged.loc[merged.groupby("odds_row_id")["match_score"].idxmax()]

        below_threshold = best_matches["match_score"] < min_match_score
        for _, row in best_matches[below_threshold].iterrows():
            logger.warning(
                f"Low match score ({row['match_score']}):"
                f"\tFighter in raw odds: '{row['fighter_name']}'"
                f"\tFighter in UFCStats: '{row['fighter_full_name']}'"
            )

        final_data = best_matches[
            ["html_datetime", "fight_id", "fighter_id", "fighter_odds"]
        ].rename(
            columns={
                "html_datetime": "scrape_datetime",
                "fighter_odds": "odds",
            }
        )

        final_data["scrape_datetime"] = pd.to_datetime(final_data["scrape_datetime"])

        final_data["betting_house"] = betting_house

        logger.info(f"Rows to be consolidated: {len(final_data)}")

        previous_size = len(self.data)

        final_data = pd.concat([final_data, self.data], ignore_index=True)

        final_data.to_csv(self.data_file, index=False)
        self.remove_duplicates_from_file()
        logger.info(f"Consolidated {betting_house} odds data saved to {self.data_file}")
        logger.info(
            f"Rows added to database: {len(self.data) - previous_size} "
            f"(total: {len(self.data)})"
        )
