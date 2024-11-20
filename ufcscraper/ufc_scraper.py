from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from ufcscraper.base import BaseScraper
from ufcscraper.event_scraper import EventScraper
from ufcscraper.fight_scraper import FightScraper
from ufcscraper.fighter_scraper import FighterScraper

if TYPE_CHECKING:
    from typing import Optional


logger = logging.getLogger(__name__)


class UFCScraper(BaseScraper):
    """A class to handle scraping of UFC-related data.

    This class is responsible for initializing and managing the individual
    scrapers for UFC events, fighters, and fights. It provides methods to
    check data files, load data, remove duplicates, and scrape different
    types of UFC data.

    Attributes:
        data_folder: Path to the folder where data will be stored.
        n_sessions: Number of concurrent sessions to use for scraping.
        delay: Delay between requests in seconds.
        event_scraper: Scraper instance for UFC events.
        fighter_scraper: Scraper instance for UFC fighters.
        fight_scraper: Scraper instance for UFC fights.
    """

    def __init__(
        self,
        data_folder: Path | str,
        n_sessions: Optional[int] = 1,
        delay: Optional[float] = 0,
    ) -> None:
        """Initialize the UFCScraper with given parameters.

        This class collects all the other scraper classes and
        gives a consistent interface for scraping data.

        Args:
            data_folder: Path to the folder where data will be stored.
            n_sessions: Number of concurrent sessions to use for scraping.
            delay: Delay between requests in seconds.
        """
        self.data_folder = Path(data_folder)
        self.n_sessions = n_sessions or self.n_sessions
        self.delay = delay or self.delay

        self.event_scraper = EventScraper(self.data_folder, n_sessions, delay)
        self.fighter_scraper = FighterScraper(self.data_folder, n_sessions, delay)
        self.fight_scraper = FightScraper(self.data_folder, n_sessions, delay)

    def check_data_file(self) -> None:
        """Check the integrity of data files for all scrapers.

        This method iterates over all scrapers and verifies their data files.
        """
        for scraper in [
            self.event_scraper,
            self.fighter_scraper,
            self.fight_scraper,
            self.fight_scraper.rounds_handler,
        ]:
            scraper.check_data_file()

    def load_data(self) -> None:
        """Load data for all scrapers.

        This method iterates over all scrapers and loads their data.
        """
        for scraper in [
            self.event_scraper,
            self.fighter_scraper,
            self.fight_scraper,
            self.fight_scraper.rounds_handler,
        ]:
            scraper.load_data()

    def remove_duplicates_from_file(self) -> None:
        """Remove duplicate entries from data files for all scrapers.

        This method iterates over all scrapers and removes duplicates from their data files.
        """
        for scraper in [
            self.event_scraper,
            self.fighter_scraper,
            self.fight_scraper,
            self.fight_scraper.rounds_handler,
        ]:
            scraper.remove_duplicates_from_file()

    def scrape_fighters(self) -> None:
        """Scrape fighter data.

        Calls the fighter scraper to collect fighter information.
        """
        self.fighter_scraper.scrape_fighters()

    def scrape_events(self) -> None:
        """Scrape event data.

        Calls the event scraper to collect event information.
        """
        self.event_scraper.scrape_events()

    def scrape_fights(self, get_all_events: bool = False) -> None:
        """Scrape fight data.

        Calls the fight scraper to collect fight information based on the
            specified parameter.

        Args:
            get_all_events: If False, only scrapes fights from events not
                already scraped.
        """
        self.fight_scraper.scrape_fights(get_all_events=get_all_events)
