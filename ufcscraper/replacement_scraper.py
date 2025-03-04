"""
This module defines classes for scraping fighter replacement data from the
BetMMA.tips website.
"""

from __future__ import annotations

import csv
import logging
import re
from datetime import datetime
from typing import TYPE_CHECKING

import pandas as pd
from fuzzywuzzy import fuzz

from ufcscraper.base import BaseScraper
from ufcscraper.ufc_scraper import UFCScraper
from ufcscraper.utils import link_to_soup

if TYPE_CHECKING:  # pragma: no cover
    from typing import Dict, Optional

logger = logging.getLogger(__name__)


class ReplacementScraper(BaseScraper): # pragma: no cover
    """Scrapes fighter replacement data from the BetMMA.tips website.

    This class inherits from `BaseScraper` and is responsible for scraping
    fighter replacement data from the BetMMA.tips website. It stores the
    scraped data in a CSV file for further analysis.
    """

    dtypes: Dict[str, type | pd.core.arrays.integer.Int64Dtype] = {
        "fight_id": str,
        "fighter_id": str,
        "notice_days": int,
    }

    sort_fields = ["fight_id", "fighter_id"]
    data = pd.DataFrame({col: pd.Series(dtype=dt) for col, dt in dtypes.items()})
    filename = "replacement_data.csv"
    web_url = "https://www.betmma.tips/ufc_late_replacement_fight_stats.php"

    def scrape_replacements(self) -> None:
        """Scrapes replacement details from BetMMA and saves data into a CSV file."""
        logger.info("Scraping replacements...")
        # Generate soup, then navigate to select the correct table
        soup = link_to_soup(self.web_url)
        table = soup.find_all("td", bgcolor="#F7F7F7")[1].find("table")

        # Retrieve data from the table
        table_data = []

        for row in table.find_all("tr"):
            # Get all columns in the current row
            columns = row.find_all("td")

            # Extract text from each column
            row_data = [col.get_text(strip=True) for col in columns]
            if row_data:
                table_data.append(row_data)

        table_data = [[row[indx] for indx in [2, 5, 7, 8]] for row in table_data]

        # Create dataframe with data.
        replacement_data = pd.DataFrame(
            table_data[1:],
            columns=table_data[0],
        )

        # Convert date into datetime
        replacement_data["Date"] = replacement_data["Date"].apply(
            lambda x: (
                datetime.strptime(re.sub(r"(\d+)(st|nd|rd|th)", r"\1", x), "%d %b %y")
            )
        )

        # Generate index to safely group single entries later.
        replacement_data["Index"] = replacement_data.index

        # We now merge with ufc stats data
        # This will allow us to get fighter_id and fight_id.
        replacement_data = replacement_data.merge(
            self.get_ufc_stats_data(),
            left_on="Date",
            right_on="event_date",
        )

        # We keep for each records in the original replacement data,
        # only keep the best match using the fighter and opponent names.
        replacement_data = replacement_data.groupby("Index").apply(
            self.best_name_match,
            include_groups=False,
        )

        # We rename the dataframe to have the desired column names.
        replacement_data = (
            replacement_data[["fight_id", "fighter_1", "Days"]].rename(
                columns={
                    "fighter_1": "fighter_id",
                    "Days": "notice_days",
                }
            )
        ).dropna()

        # Now we want to check which of these are new records that we need
        # to write into file.
        missing_rows = pd.merge(
            replacement_data,
            self.data,
            on=["fight_id", "fighter_id"],
            how="left",
            indicator=True,
        )
        missing_rows = (
            missing_rows[missing_rows["_merge"] == "left_only"]
            .drop(columns=["_merge", "notice_days_y"])
            .rename(columns={"notice_days_x": "notice_days"})
        )

        logger.info(f"Found {len(missing_rows)} new records.")

        # Now we write the missing records
        with open(self.data_file, "a+") as f:
            writer = csv.writer(f)

            for _, row in missing_rows.iterrows():
                writer.writerow(row)

        self.load_data()

    def get_ufc_stats_data(self) -> pd.DataFrame:
        """
        Read and prepare UFC stats data to be merged into scraped
        replacement information.

        Returns:
            A pandas dataframe containing the relevant UFCStats information.
        """
        ufc_scraper = UFCScraper(self.data_folder)

        ufc_scraper.fighter_scraper.add_name_column()

        # First we merge between tables to get the information we want
        ufc_data = (
            ufc_scraper.fight_scraper.data[
                ["fight_id", "event_id", "fighter_1", "fighter_2"]
            ]
            .merge(
                ufc_scraper.fighter_scraper.data[["fighter_id", "fighter_name"]],
                left_on="fighter_1",
                right_on="fighter_id",
            )
            .merge(
                ufc_scraper.fighter_scraper.data[["fighter_id", "fighter_name"]].rename(
                    columns={"fighter_name": "opponent_name"}
                ),
                left_on="fighter_2",
                right_on="fighter_id",
            )
            .merge(
                ufc_scraper.event_scraper.data[["event_id", "event_date"]],
                on="event_id",
            )[
                [
                    "fight_id",
                    "fighter_1",
                    "fighter_2",
                    "event_date",
                    "fighter_name",
                    "opponent_name",
                ]
            ]
            .sort_values(by="event_date", ascending=False)
        )

        # Then we duplicate to have two entries for the same fight.
        # This will make easier to join later with the replacement data,
        # to avoid having to look into two columns
        ufc_data = pd.concat(
            [
                ufc_data,
                ufc_data.rename(
                    columns={
                        "fighter_1": "fighter_2",
                        "fighter_2": "fighter_1",
                        "fighter_name": "opponent_name",
                        "opponent_name": "fighter_name",
                    }
                ),
            ],
            ignore_index=True,
        )

        return ufc_data

    # Function to apply on a pandas dataframe group.
    @staticmethod
    def best_name_match(group: pd.DataFrame) -> Optional[pd.Series]:
        """
        This function is designed to be applied on a grouped dataframe.

        It will check the match in the group where the field 'Late Replacement'
        agrees with 'fighter_name' and where 'Opponent' agrees with 'opponent_name'.

        It will require both fields to agree with a score > 80, if multiple
        records are compatible, the best match will be returned.

        Returns:
            A pandas series with the matching record (if existing).
        """
        replacement_name = group["Late Replacement"].values[0]
        opponent_name = group["Opponent"].values[0]

        group["name_score"] = group["fighter_name"].apply(
            lambda x: fuzz.WRatio(replacement_name, x)
        )
        group["opponent_score"] = group["opponent_name"].apply(
            lambda x: fuzz.WRatio(opponent_name, x)
        )

        valid_matches = group[
            (group["name_score"] >= 80) & (group["opponent_score"] >= 80)
        ]

        if not valid_matches.empty:
            best_match_idx = valid_matches["name_score"].idxmax()
            return valid_matches.loc[best_match_idx]
        else:
            return None
