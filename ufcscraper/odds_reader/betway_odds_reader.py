"""
This module provides a reader for betway odds data provided
as a HTML file.
    
Classes:
- `BetwayOddsReader`: A reader for Betway odds data.
"""

from __future__ import annotations

import ast
import csv
import dateparser
from datetime import datetime
import json
from locale import setlocale, LC_TIME
import logging
import re
from datetime import time
from typing import TYPE_CHECKING

from bs4 import BeautifulSoup

from ufcscraper.odds_reader.base import OddsReader

if TYPE_CHECKING:
    from typing import Dict, List


logger = logging.getLogger(__name__)


class BetwayOddsReader(OddsReader):
    """
    A reader for BetWay odds data provided as a HTML file.
    """

    filename = "raw_odds/betway_odds_raw.csv"

    def scrape_odds(self, languages: list[str] = ["es", "en"]) -> None:
        """
        Scrapes the odds data from the HTML file and saves it to a CSV file.
        """
        html = self.read_html()
        soup = BeautifulSoup(html, "lxml")
        sections = soup.find_all("section", {"data-testid": "event-table-section"})

        rows_to_add = []

        if sections:
            for section in sections:
                date = section.find("span", {"data-testid": "table-header-title"})
                
                for language in languages:
                    date = dateparser.parse(
                        date.text,
                        languages=[language,],
                        locales=[language,],
                        settings = {
                            "PREFER_DATES_FROM": "future",
                            "RELATIVE_BASE": self.html_datetime,
                        }
                    )
                    if date is not None:
                        break
                else:
                    raise ValueError(f"Could not parse date from: {date.text}")

                for fight_section in section.find_all("div", {"data-testid": "table-section"}):
                    fight_records = fight_section.find_all("span")

                    rows_to_add.append((
                        date,
                        fight_records[0].text.strip(),
                        fight_records[3].text.strip(),
                        float(fight_records[5].text.strip().replace(',', '.')),
                        float(fight_records[7].text.strip().replace(',', '.')),   
                    ))
        else:
            payload = self._extract_next_payload(html)
            match_emos = self._extract_match_emos(payload)

            for event in match_emos["events"].values():
                date = datetime.fromisoformat(event["startsAt"].replace("Z", "+00:00"))
                market = next(
                    (
                        market
                        for market in match_emos["markets"].values()
                        if market["eventId"] == event["id"]
                        and market.get("marketCName") == "fight-winner"
                    ),
                    None,
                )
                if market is None or len(market.get("outcomes", [])) < 2:
                    continue

                outcomes = [
                    match_emos["outcomes"][str(outcome_id)]
                    for outcome_id in market["outcomes"]
                    if str(outcome_id) in match_emos["outcomes"]
                ]
                outcomes = sorted(outcomes, key=lambda outcome: outcome.get("teamCName", ""))
                if len(outcomes) < 2:
                    continue

                rows_to_add.append((
                    date,
                    outcomes[0]["name"]["default"],
                    outcomes[1]["name"]["default"],
                    float(outcomes[0]["oddsDecimal"]),
                    float(outcomes[1]["oddsDecimal"]),
                ))

        if not rows_to_add:
            raise ValueError(
                "Betway HTML does not contain rendered odds sections or embedded odds payload. "
                "The page was likely saved before client-side data finished loading."
            )


        self.write_odds(rows_to_add)

    @staticmethod
    def _extract_next_payload(html: str) -> str:
        parts = []
        for match in re.finditer(r'self\.__next_f\.push\(\[1,"((?:[^"\\]|\\.)*)"\]\)', html):
            parts.append(ast.literal_eval('"' + match.group(1) + '"'))

        if not parts:
            raise ValueError("Could not find Betway Next.js payload in HTML")

        return "".join(parts)

    @staticmethod
    def _extract_match_emos(payload: str) -> dict:
        anchor = 'matchEmos":['
        start = payload.find(anchor)
        if start == -1:
            raise ValueError("Could not find Betway match data in payload")

        match_emos, _ = json.JSONDecoder().raw_decode(payload[start + len(anchor):])
        return match_emos
