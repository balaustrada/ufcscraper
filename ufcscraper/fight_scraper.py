"""
This module defines classes for scraping fight and round data from the UFCStats 
website.

Classes:
    FightScraper: Inherits from `BaseScraper` and is responsible for scraping 
    detailed fight statistics, such as fighter information, results, referees, 
    and more. The data is stored in a CSV file named `fight_data.csv`. It also 
    interacts with the `RoundsHandler` to scrape and store round-specific 
    statistics.
    
    RoundsHandler: Inherits from `BaseFileHandler` and manages the collection 
    and storage of round-specific fight data. The data is saved in a CSV file 
    named `round_data.csv`. It handles statistics like strikes, takedowns, 
    control time, and more.
"""

from __future__ import annotations

import csv
import logging
import re
from typing import TYPE_CHECKING

import pandas as pd

from ufcscraper.base import BaseScraper, BaseFileHandler
from ufcscraper.event_scraper import EventScraper
from ufcscraper.fighter_scraper import FighterScraper
from ufcscraper.utils import links_to_soups

if TYPE_CHECKING:  # pragma: no cover
    import bs4
    from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)


class FightScraper(BaseScraper):
    """Scrapes fight data from the UFCStats website.

    This class inherits from `BaseScraper` and handles scraping detailed
    fight statistics including fighters, referees, results, and more. It
    saves the scraped data into two CSV files: one for fights and one for
    rounds (through the companion class `RoundsHandler`).
    """

    dtypes: Dict[str, type | pd.core.arrays.integer.Int64Dtype] = {
        "fight_id": str,
        "event_id": str,
        "referee": str,
        "fighter_1": str,
        "fighter_2": str,
        "winner": str,
        "num_rounds": pd.Int64Dtype(),
        "title_fight": str,
        "weight_class": str,
        "gender": str,
        "result": str,
        "result_details": str,
        "finish_round": pd.Int64Dtype(),
        "finish_time": str,
        "time_format": str,
        "scores_1": pd.Int64Dtype(),
        "scores_2": pd.Int64Dtype(),
    }
    sort_fields = ["event_id", "fight_id"]
    data = pd.DataFrame({col: pd.Series(dtype=dt) for col, dt in dtypes.items()})
    filename = "fight_data.csv"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initializes the FightScraper and the companion RoundsHandler.

        Args:
            *args: Additional positional arguments passed to the base class.
            **kwargs: Additional keyword arguments passed to the base class.
        """
        super().__init__(*args, **kwargs)

        self.rounds_handler = RoundsHandler(self.data_folder)

    @classmethod
    def url_from_id(cls, id_: str) -> str:
        """Constructs the fight URL using the fight ID.

        Args:
            id_: The unique identifier for the fight.

        Returns:
            The full URL to the fight's details page on UFCStats.
        """
        return f"{cls.web_url}/fight-details/{id_}"

    def scrape_fights(self, get_all_events: bool = False) -> None:
        """Scrapes fight data and saves it to CSV files.

        This method scrapes fight details and round statistics. It saves the
        fight details and round statistics to separate CSV files.

        Args:
            get_all_events: If False, only scrapes fights from events not
                already scraped.
        """
        existing_urls = set(map(self.url_from_id, self.data["fight_id"]))
        ufcstats_fight_urls = self.get_fight_urls(get_all_events)
        urls_to_scrape = set(ufcstats_fight_urls) - existing_urls

        logger.info(f"Opening round information to scrape stats")
        rounds_handler = RoundsHandler(self.data_folder)

        logger.info(f"Scraping {len(urls_to_scrape)} fights...")

        with (
            open(self.data_file, "a") as f_fights,
            open(rounds_handler.data_file, "a+") as f_rounds,
        ):
            writer_fights = csv.writer(f_fights)
            writer_rounds = csv.writer(f_rounds)

            for i, (url, soup) in enumerate(
                links_to_soups(list(urls_to_scrape), self.n_sessions, self.delay)
            ):
                try:
                    overview = soup.select("i.b-fight-details__text-item")
                    select_result = soup.select("i.b-fight-details__text-item_first")
                    select_result_details = soup.select("p.b-fight-details__text")
                    fight_details = soup.select("p.b-fight-details__table-text")
                    fight_type = soup.select("i.b-fight-details__fight-title")
                    win_lose = soup.select("i.b-fight-details__person-status")

                    if soup.h2 is not None:
                        event_id = EventScraper.id_from_url(
                            str(soup.h2.select("a.b-link")[0]["href"])
                        )
                    else:
                        raise TypeError("Couldn't find header in the soup.")

                    referee = self.get_referee(overview)
                    fighter_1, fighter_2 = self.get_fighters(fight_details, soup)
                    num_rounds = overview[2].text.split(":")[1].strip()[0].strip()
                    num_rounds = str(int(num_rounds)) if num_rounds != "N" else ""
                    title_fight = self.get_title_fight(fight_type)
                    weight_class = self.get_weight_class(fight_type)
                    gender = self.get_gender(fight_type)
                    result, result_details = self.get_result(
                        select_result, select_result_details
                    )
                    finish_round = int(overview[0].text.split(":")[1].strip())
                    finish_time = re.findall(r"\d:\d\d", overview[1].text)[0]
                    winner = self.get_winner(fighter_1, fighter_2, win_lose)
                    time_format = overview[2].text.split(":")[1].strip()
                    fight_id = self.id_from_url(url)
                    scores_1, scores_2 = self.get_scores(
                        overview, select_result, select_result_details
                    )

                    # Correctly assign winner, in UFCStats winner is the scores_2
                    # always...
                    # I also need to flip in case of tie (right score for the higher
                    # ranked)
                    if winner != fighter_2:
                        scores_1, scores_2 = scores_2, scores_1

                    # I am saving first the rounds and then the fights
                    # in case of error the fight doesn't count as scraped
                    fight_stats_select = soup.select("p.b-fight-details__table-text")
                    for j, fighter_id in enumerate((fighter_1, fighter_2)):
                        for round_ in range(1, finish_round + 1):
                            stats = rounds_handler.get_stats(
                                fight_stats_select,
                                fighter=j,
                                round_=round_,
                                finish_round=finish_round,
                            )

                            writer_rounds.writerow(
                                (fight_id, fighter_id, round_) + stats
                            )

                    writer_fights.writerow(
                        [
                            fight_id,
                            event_id,
                            referee.strip(),
                            fighter_1,
                            fighter_2,
                            winner.strip(),
                            num_rounds,
                            title_fight,
                            weight_class,
                            gender,
                            result.strip(),
                            result_details.strip(),
                            finish_round,
                            finish_time.strip(),
                            time_format.strip(),
                            scores_1,
                            scores_2,
                        ]
                    )

                    logger.info(f"Scraped {i+1}/{len(urls_to_scrape)} fights...")
                except Exception as e:
                    logger.error(f"Error saving data from url: {url}\nError: {e}")

        self.remove_duplicates_from_file()
        self.rounds_handler.remove_duplicates_from_file()

    def get_fight_urls(self, get_all_events: bool = False) -> List[str]:
        """Retrieves URLs of all fights from UFCStats.

        Args:
            get_all_events: If False, only gets URLs for fights from events
                not already scraped.

        Returns:
            A list of URLs for fights.
        """
        logger.info("Scraping fight links...")

        logger.info("Opening event information to extract event urls...")
        event_scraper = EventScraper(self.data_folder, self.n_sessions, self.delay)
        event_ids = event_scraper.data["event_id"].unique().tolist()

        # Remove events for which information is extracted
        if not get_all_events:
            event_ids = [
                id_
                for id_ in event_ids
                if id_ not in self.data["event_id"].unique().tolist()
            ]

        event_urls: List[str] = list(map(EventScraper.url_from_id, event_ids))

        fight_urls = set()
        i = 0
        for _, soup in links_to_soups(event_urls, self.n_sessions):
            for item in soup.find_all("a", class_="b-flag b-flag_style_green"):
                fight_urls.add(item.get("href"))
            for item in soup.find_all("a", class_="b-flag b-flag_style_bordered"):
                fight_urls.add(item.get("href"))
            print(f"Scraped {i}/{len(event_urls)} events...", end="\r")
            i += 1

        logger.info(f"Got {len(fight_urls)} fight links...")
        return list(fight_urls)

    @staticmethod
    def get_referee(overview: bs4.element.ResultSet) -> str:
        """Extracts the referee's name from the fight overview.

        Args:
            overview: A ResultSet containing fight overview information.

        Returns:
            The referee's name, or '' if not found.
        """
        try:
            return overview[3].text.split(":")[1]
        except:
            return ""

    @staticmethod
    def get_fighters(
        fight_details: bs4.element.ResultSet, fight_soup: bs4.BeautifulSoup
    ) -> Tuple[str, str]:
        """Extracts fighter IDs from the fight details.

        Args:
            fight_details: A ResultSet containing fight detail information.
            fight_soup: The BeautifulSoup object containing the fight page.

        Returns:
            A tuple containing the IDs of the two fighters.
        """
        # Scrape both fighter names
        try:
            fighters = (
                fight_details[0].select("a.b-link.b-link_style_black")[0]["href"],
                fight_details[1].select("a.b-link.b-link_style_black")[0]["href"],
            )
        except:  # pragma: no cover
            fighters = (
                fight_soup.select("a.b-fight-details__person-link")[0]["href"],
                fight_soup.select("a.b-fight-details__person-link")[1]["href"],
            )

        fighter_1, fighter_2 = map(
            FighterScraper.id_from_url,
            fighters,
        )

        return fighter_1, fighter_2

    # Scrape name of winner
    @staticmethod
    def get_winner(
        fighter_1: str, fighter_2: str, win_lose: bs4.element.ResultSet
    ) -> str:
        """Determines the winner of the fight based on the win/lose status.

        Args:
            fighter_1: The ID of the first fighter.
            fighter_2: The ID of the second fighter.
            win_lose: A ResultSet containing win/lose status for the fighters.

        Returns:
            The ID of the winner, or 'Draw' if it's a draw, or 'NC if no contest
            or '' if not determined.
        """
        fighter_1_result = win_lose[0].text.strip()
        fighter_2_result = win_lose[1].text.strip()

        if fighter_1_result == "D" and fighter_2_result == "D":
            return "Draw"
        elif fighter_1_result == "NC" and fighter_2_result == "NC":
            return "NC"
        elif fighter_1_result == "W":
            return fighter_1
        elif fighter_2_result == "W":
            return fighter_2
        else:
            return ""

    # Checks if fight is title fight
    @staticmethod
    def get_title_fight(fight_type: bs4.element.ResultSet) -> str:
        """Determines if the fight is a title fight.

        Args:
            fight_type: A ResultSet containing fight type information.

        Returns:
            'T' if it's a title fight, 'F' otherwise.
        """
        if "Title" in fight_type[0].text:
            return "T"
        else:
            return "F"

    # Scrapes weight class of fight
    @staticmethod
    def get_weight_class(fight_type: bs4.element.ResultSet) -> str:
        """Extracts the weight class of the fight.

        Args:
            fight_type: A ResultSet containing fight type information.

        Returns:
            The weight class of the fight, or '' if not found.
        """
        if "Light Heavyweight" in fight_type[0].text.strip():
            return "Light Heavyweight"

        elif "Women" in fight_type[0].text.strip():
            return "Women's " + re.findall(r"\w*weight", fight_type[0].text.strip())[0]

        elif "Catch Weight" in fight_type[0].text.strip():
            return "Catch Weight"

        elif "Open Weight" in fight_type[0].text.strip():
            return "Open Weight"

        else:
            try:
                return re.findall(r"\w*weight", fight_type[0].text.strip())[0]
            except:
                return ""

    # Checks gender of fight
    @staticmethod
    def get_gender(fight_type: bs4.element.ResultSet) -> str:
        """Determines the gender of the fight.

        Args:
            fight_type: A ResultSet containing fight type information.

        Returns:
            'F' if it's a women's fight, 'M' otherwise.
        """
        if "Women" in fight_type[0].text:
            return "F"
        else:
            return "M"

    # Scrapes the way the fight ended (e.g. KO, decision, etc.)
    @staticmethod
    def get_result(
        select_result: bs4.element.ResultSet,
        select_result_details: bs4.element.ResultSet,
    ) -> Tuple[str, str]:
        """
        Extracts the result and details of the fight.

        Args:
            select_result: A ResultSet containing the fight result.
            select_result_details: A ResultSet containing additional result details.

        Returns:
            A tuple with the result type and result details.
        """
        if "Decision" in select_result[0].text.split(":")[1]:
            return (
                select_result[0].text.split(":")[1].split()[0],
                select_result[0].text.split(":")[1].split()[-1],
            )
        else:
            result = select_result[0].text.split(":")[1]
            result_details = select_result_details[1].text.split(":")[-1]

            if result_details.count("-") >= 3:
                # This is the case of an overturned decision where the
                # - appearing at least three times is the score '29 - 28'
                # for the three judges (+ maybe an extra term in the
                # description)
                return (
                    result,
                    " ".join(result_details.split(".")[-4].split("-")[-2].split()[:-3]),
                )

            return result, result_details

    @staticmethod
    def get_scores(
        overview: bs4.element.ResultSet,
        select_result: bs4.element.ResultSet,
        select_result_details: bs4.element.ResultSet,
    ) -> Tuple[str, str]:
        """
        Extracts the scores of the fight if they the fight went the distance.

        Args:
            overview: A ResultSet containing the fight overview.
            select_result: A ResultSet containing the fight result.

        Returns:
            A tuple with the scores of the fight. As str to be
            written to the CSV file.

        """
        result_details = select_result_details[1].text.split(":")[-1]

        if ("Decision" in select_result[0].text.split(":")[1]) or (
            result_details.count("-") >= 3
        ):
            # Initialize a list to hold the extracted scores
            scores = []

            # Define the regex pattern for capturing the scores (e.g., 27 - 30, 28 - 29, etc.)
            score_pattern = re.compile(r"(\d{1,2})\s*-\s*(\d{1,2})\.")

            # Iterate over the selected elements and check for score patterns
            for detail in overview:
                text = detail.get_text(strip=True)
                matches = score_pattern.findall(text)  # Find all matches in the text
                for match in matches:
                    scores.append(match)  # Append each found score to the list

            if len(scores) > 0:
                scores1 = 0
                scores2 = 0
                for s1, s2 in scores:
                    scores1 += int(s1)
                    scores2 += int(s2)

                return str(scores1), str(scores2)

        return "", ""


class RoundsHandler(BaseFileHandler):
    """Handles the manipulation and storage of round statistics.

    This class inherits from `BaseFileHandler` and manages round-specific
    statistics, including strikes, takedowns, and control time. It formats
    and saves the data to a CSV file.
    """

    dtypes: Dict[str, type | pd.core.arrays.integer.Int64Dtype] = {
        "fight_id": str,
        "fighter_id": str,
        "round": pd.Int64Dtype(),
        "knockdowns": pd.Int64Dtype(),
        "strikes_att": pd.Int64Dtype(),  # If not stated otherwise they are significant
        "strikes_succ": pd.Int64Dtype(),
        "head_strikes_att": pd.Int64Dtype(),
        "head_strikes_succ": pd.Int64Dtype(),
        "body_strikes_att": pd.Int64Dtype(),
        "body_strikes_succ": pd.Int64Dtype(),
        "leg_strikes_att": pd.Int64Dtype(),
        "leg_strikes_succ": pd.Int64Dtype(),
        "distance_strikes_att": pd.Int64Dtype(),
        "distance_strikes_succ": pd.Int64Dtype(),
        "ground_strikes_att": pd.Int64Dtype(),
        "ground_strikes_succ": pd.Int64Dtype(),
        "clinch_strikes_att": pd.Int64Dtype(),
        "clinch_strikes_succ": pd.Int64Dtype(),
        "total_strikes_att": pd.Int64Dtype(),  # significant and not significant
        "total_strikes_succ": pd.Int64Dtype(),
        "takedown_att": pd.Int64Dtype(),
        "takedown_succ": pd.Int64Dtype(),
        "submission_att": pd.Int64Dtype(),
        "reversals": pd.Int64Dtype(),
        "ctrl_time": str,
    }
    sort_fields = ["fight_id", "fighter_id", "round"]
    data = pd.DataFrame({col: pd.Series(dtype=dt) for col, dt in dtypes.items()})
    filename = "round_data.csv"

    @staticmethod
    def get_stats(
        fight_stats: bs4.element.ResultSet, fighter: int, round_: int, finish_round: int
    ) -> Tuple[str, ...]:
        """
        Extracts round statistics for a specific fighter in a given fight.

        Args:
            fight_stats: A ResultSet containing fight statistics.
            fighter: The index of the fighter (0 or 1).
            round_: The round number.
            finish_round: The total number of rounds.

        Returns:
            A tuple of statistics for the specified fighter in the given round.
            Returns "" for all fields if an error occurs.

        Raises:
            ValueError: If `fighter` is not 0 or 1.
        """
        if fighter not in (0, 1):
            raise ValueError(f"fighter must be 0 or 1, not {fighter}")

        shift_general = 20 * round_
        shift_striking = 20 * (finish_round + 1) + 18 * (round_)

        if fighter == 1:
            shift_general += 1
            shift_striking += 1
        try:
            data = (
                fight_stats[2 + shift_general].text,  # knockdowns
                fight_stats[2 + shift_striking].text.split(" of ")[
                    1
                ],  # Significant strikes
                fight_stats[2 + shift_striking].text.split(" of ")[0],
                fight_stats[6 + shift_striking].text.split(" of ")[1],  # Head
                fight_stats[6 + shift_striking].text.split(" of ")[0],
                fight_stats[8 + shift_striking].text.split(" of ")[1],  # Body
                fight_stats[8 + shift_striking].text.split(" of ")[0],
                fight_stats[10 + shift_striking].text.split(" of ")[1],  # Leg
                fight_stats[10 + shift_striking].text.split(" of ")[0],
                fight_stats[12 + shift_striking].text.split(" of ")[1],  # Distance
                fight_stats[12 + shift_striking].text.split(" of ")[0],
                fight_stats[16 + shift_striking].text.split(" of ")[1],  # Ground
                fight_stats[16 + shift_striking].text.split(" of ")[0],
                fight_stats[14 + shift_striking].text.split(" of ")[1],  # Clinch
                fight_stats[14 + shift_striking].text.split(" of ")[0],
                fight_stats[8 + shift_general].text.split(" of ")[1],  # Total strikes
                fight_stats[8 + shift_general].text.split(" of ")[0],
                fight_stats[10 + shift_general].text.split(" of ")[1],  # Takedown
                fight_stats[10 + shift_general].text.split(" of ")[0],
                fight_stats[14 + shift_general].text,  # Submission attempts
                fight_stats[16 + shift_general].text,  # Reversals
                fight_stats[18 + shift_general].text,  # Control time
            )

            return tuple(datum.strip() for datum in data)
        except:
            return ("",) * 22
