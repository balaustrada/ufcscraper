"""
This module defines classes for scraping placed bets information.
Currently it only works with Bet365 (and needs the html to be
provided as text).

Classes:
- `Bet365BetHandler`: A handler for Bet365 placed bets.
"""

from __future__ import annotations

import csv
from fuzzywuzzy import process, fuzz
from pathlib import Path
import logging
from bs4 import BeautifulSoup
from pandas import pd
from typing import TYPE_CHECKING

from ufcscraper.base import BaseFileHandler
from ufcscraper.fighter_names import FighterNames

if TYPE_CHECKING:
    from typing import Dict


logging = logging.getLogger(__name__)


class Bet365BetHandler(BaseFileHandler):
    """
    A handler for Bet365 placed bets.
    """

    dtypes: Dict[str, type | pd.core.arrays.integer.Int64Dtype] = {
        "datetime": "datetime64[ns]",
        "fight_id": str,
        "stake": float,
        "acca_boost": float,
        "bonus": float,
        "return": float,
        "odds": float,
        "bet_type": str,
    }

    sort_fields = ["datetime", "fight_id"]
    data = pd.DataFrame({col: pd.Series(dtype=dt) for col, dt in dtypes.items()})
    filename = "bet365_bets.csv"

    def __init__(self, data_folder: Path | str):
        """
        Initializes the Bet365BetHandler with the specified data folder.

        Args:
            data_folder (Path | str): The folder where the CSV file is stored
            or will be created.
        """
        super().__init__(data_folder)
        self.fighter_names = FighterNames(data_folder)
        self._load_fighter_names_data()

    def _load_fighter_names_data(self) -> None:
        """
        Loads fighter names from the fighter_names handler.
        """
        self.fighter_names.load_data()
        self.bet365_names = self.fighter_names.data[
            self.fighter_names.data["database"] == "bet365"
        ]
        self.ufcstats_names = self.fighter_names.data[
            self.fighter_names.data["database"] == "UFCStats"
        ]

    def _get_fighter_id(self, name: str) -> str:
        """
        Retrieves the fighter ID for a given fighter name.

        Args:
            name (str): The name of the fighter.

        Returns:
            str: The fighter ID, or an empty string if not found.
        """
        fighter_id = self.fighter_names.check_fighter_id(name, "bet365")

        if not fighter_id:
            # If fighter not in database, try to find a close match in UFCStats
            best_name, score = process.extractOne(
                name,
                self.ufcstats_names["name"].tolist(),
                scorer=fuzz.token_sort_ratio,
            )
            if score > 90:
                row = self.ufcstats_names[
                    self.ufcstats_names["name"] == best_name
                ].iloc[0]
                return row["fighter_id"]
            else:
                raise ValueError(f"Fighter ID not found for name: {name}")
        else:
            return fighter_id

    def get_bets(self, html: str) -> None:
        """
        Parses the provided HTML to extract placed bets and stores them in the data DataFrame.

        Args:
            html (str): The HTML content containing the placed bets.
        """
        soup = BeautifulSoup(html, "lxml")

        for bet in soup.find_all("div", class_="h-BetSummary"):
            # 1. Datetime
            datetime_tag = bet.find("div", class_="h-BetSummary_DateAndTime")
            datetime_text = datetime_tag.text.strip() if datetime_tag else None

            # 2. Acca Boost
            acca_boost_tag = bet.find("div", class_="hob-OfferBadgeSettled_BonusText")
            acca_boost = acca_boost_tag.text.strip() if acca_boost_tag else None

            # 3. Stake
            stake_tag = bet.select_one(".h-StakeReturnSection_StakeContainer > div")
            stake_text = (
                stake_tag.text.strip().replace("Stake ", "") if stake_tag else None
            )

            # 4. Return
            return_tag = bet.find("div", class_="h-StakeReturnSection_ReturnText")
            return_text = (
                return_tag.text.strip().replace("Return ", "") if return_tag else None
            )

            # 5. Bonus
            bonus_tag = bet.find("div", class_="h-StakeReturnSection_BonusText")
            bonus_text = bonus_tag.text.strip() if bonus_tag else None

            bonus = ""
            if bonus_text:
                bonus = (
                    bonus_text.replace("bonus", "")
                    .strip()
                    .replace("+", "")
                    .replace("€", "")
                    .strip()
                )

            # 6. Fighter Names and Odds
            fighter_names = []
            odds_list = []
            bet_types = []

            for sel in bet.find_all("div", class_="h-BetSelection"):
                name_tag = sel.find("div", class_="h-BetSelection_Name")
                odds_tag = sel.find("div", class_="h-BetSelection_Odds")

                name = name_tag.text.strip() if name_tag else None
                odds_str = (
                    odds_tag.span.text.strip() if odds_tag and odds_tag.span else None
                )

                if name and odds_str:
                    fighter_names.append(name)
                    try:
                        odds_list.append(odds_str)
                    except ValueError:
                        odds_list.append("")

                    if " por " in name:
                        bet_types.append("Method")
                    else:
                        bet_types.append("Winner")

                fighter_id = self._get_fighter_id(name)
