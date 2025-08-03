"""
This module contains the `FighterNames` class, which is part of the UFC Scraper project.
It is responsible for managing and handling fighter names and their associated data.

Classes:
- `FighterNames`:
    This class is responsible for managing the records of UFC fighters, including
    their IDs, names, and associated database identifiers from different sources.
    It is filled at the same time as the odds data is scraped and facilitates
    the completeness of the scraping process. The class also handles the storage
    of this data.
"""

from __future__ import annotations

import csv
import logging
from typing import TYPE_CHECKING

import pandas as pd

from ufcscraper.base import BaseFileHandler
from ufcscraper.ufc_scraper import UFCScraper

if TYPE_CHECKING:  # pragma: no cover
    from typing import Dict, Optional

logger = logging.getLogger(__name__)


class FighterNames(BaseFileHandler):
    """A class to manage and handle fighter names and their associated data.

    This class is responsible for loading and checking records of fighters
    from various data sources. It manages the storage of fighter information,
    including IDs, names, and database identifiers.
    """

    dtypes: Dict[str, type | pd.core.arrays.integer.Int64Dtype] = {
        "fighter_id": str,
        "database": str,
        "name": str,
        "database_id": str,
    }
    sort_fields = ["database", "name", "fighter_id"]
    data = pd.DataFrame({col: pd.Series(dtype=dt) for col, dt in dtypes.items()})
    filename = "fighter_names.csv"

    def check_missing_records(self) -> None:
        """Check for missing records in the UFCStats data.

        Compares the current data with the UFCStats data to identify
        missing records. Appends new records to the CSV file if any are found.
        """
        logger.info("Checking missing records...")
        ufc_stats_data = self.get_ufcstats_data()

        # Remove existing records from dataframe
        existing_records = self.data[self.data["database"] == "UFCStats"][
            "fighter_id"
        ].tolist()

        missing_records = ufc_stats_data[
            ~ufc_stats_data["fighter_id"].isin(existing_records)
        ]

        if len(missing_records) > 0:
            logger.info("Missing UFCStats records for some fighters, recomputing...")

            with open(self.data_file, "a+") as f:
                writer = csv.writer(f)
                for fighter_id, name in ufc_stats_data[
                    ["fighter_id", "fighter_name"]
                ].values:
                    writer.writerow([fighter_id, "UFCStats", name, fighter_id])

            print()
            logger.info("Reloading data after adding missing records")
            self.load_data()

    def check_fighter_id(self, fighter_name: str, database: str) -> Optional[str]:
        """Check if a fighter ID exists in the database. And return its ID.

        Args:
            fighter_name: Name of the fighter.
            database: Name of the database.

        Returns:
            str: The fighter ID if found, otherwise None.
        """
        fighter_id = self.data[
            (self.data["name"] == fighter_name) & (self.data["database"] == database)
        ]["fighter_id"]

        if len(fighter_id) > 0:
            return fighter_id.iloc[0]
        else:
            return None

    def fighter_in_database(
        self, fighter_id: str, database: str, name: str, database_id: str
    ) -> bool:
        """Check if a fighter is present in the database.

        Args:
            fighter_id: The ID of the fighter.
            database: The name of the database.
            name: The name of the fighter.
            database_id: The ID of the fighter in the database.

        Returns:
            True if the fighter is in the database, False otherwise.
        """
        return bool(
            (
                (self.data["fighter_id"] == fighter_id)
                & (self.data["database"] == database)
                & (self.data["name"] == name)
                & (self.data["database_id"] == database_id)
            ).any()
        )

    def get_ufcstats_data(self) -> pd.DataFrame:
        """Retrieve and prepare UFCStats data.

        Loads and processes data from UFCStats, including fights, and
        fighter details. Merges the data to create a comprehensive DataFrame.

        This can be later compared with the data in the CSV file to identify
        missing records.

        Returns:
            DataFrame containing processed UFCStats data.
        """
        logger.info("Loading UFCStats data...")
        ufc_stats_data = UFCScraper(self.data_folder)

        fights = ufc_stats_data.fight_scraper.data

        fighters_object = ufc_stats_data.fighter_scraper
        fighters_object.add_name_column()
        fighters = fighters_object.data

        data = pd.concat(
            [
                fights.rename(
                    columns={"fighter_1": "opponent_id", "fighter_2": "fighter_id"}
                ),
                fights.rename(
                    columns={"fighter_2": "opponent_id", "fighter_1": "fighter_id"}
                ),
            ]
        )

        fighter_fields = ["fighter_id", "fighter_name", "fighter_nickname"]
        data = data.merge(
            fighters[fighter_fields],
            on="fighter_id",
            how="left",
        )
        return data
