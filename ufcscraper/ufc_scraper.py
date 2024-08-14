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
    def __init__(
        self,
        data_folder: Path | str,
        n_sessions: Optional[int] = 1,
        delay: Optional[float] = 0,
    ) -> None:
        self.data_folder = Path(data_folder)
        self.n_sessions = n_sessions or self.n_sessions
        self.delay = delay or self.delay

        self.event_scraper = EventScraper(self.data_folder, n_sessions, delay)
        self.fighter_scraper = FighterScraper(self.data_folder, n_sessions, delay)
        self.fight_scraper = FightScraper(self.data_folder, n_sessions, delay)

    def check_data_file(self) -> None:
        for scraper in [
            self.event_scraper,
            self.fighter_scraper,
            self.fight_scraper,
            self.fight_scraper.rounds_handler,
        ]:
            scraper.check_data_file()

    def load_data(self) -> None:
        for scraper in [
            self.event_scraper,
            self.fighter_scraper,
            self.fight_scraper,
            self.fight_scraper.rounds_handler,
        ]:
            scraper.load_data()

    def remove_duplicates_from_file(self) -> None:
        for scraper in [
            self.event_scraper,
            self.fighter_scraper,
            self.fight_scraper,
            self.fight_scraper.rounds_handler,
        ]:
            scraper.remove_duplicates_from_file()

    def scrape_fighters(self) -> None:
        self.fighter_scraper.scrape_fighters()

    def scrape_events(self) -> None:
        self.event_scraper.scrape_events()

    def scrape_fights(self, get_all_events: bool = False) -> None:
        self.fight_scraper.scrape_fights(get_all_events=get_all_events)