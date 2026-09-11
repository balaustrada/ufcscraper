from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ufcscraper.odds_reader.bet365_odds_reader import Bet365OddsReader
from ufcscraper.odds_reader.betway_odds_reader import BetwayOddsReader


class TestOddsReaders(unittest.TestCase):
    def test_betway_skips_extra_markets_section(self) -> None:
        html = """
            <section data-testid="event-table-section">
              <span data-testid="table-header-title">September 20, 2026</span>
              <div data-testid="table-section">
                <span>Fighter One</span><span>Fighter Two</span>
                <span data-testid="outcome-price-value">1.50</span>
                <span data-testid="outcome-price-value">2.50</span>
                <span data-testid="event-table-market-count">Más apuestas</span>
              </div>
            </section>
            <section data-testid="event-table-section">
              <span data-testid="table-header-title">Más apuestas</span>
            </section>
        """

        with tempfile.TemporaryDirectory() as folder:
            data_folder = Path(folder)
            (data_folder / "raw_odds").mkdir()
            html_file = data_folder / "betway.html"
            html_file.write_text(html)
            reader = BetwayOddsReader(html_file, data_folder)

            reader.scrape_odds()

            self.assertEqual(len(reader.data), 1)
            self.assertEqual(reader.data.iloc[0]["fighter_name"], "Fighter One")

    def test_bet365_skips_html_without_rendered_market(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            data_folder = Path(folder)
            (data_folder / "raw_odds").mkdir()
            html_file = data_folder / "bet365.html"
            html_file.write_text("<html><body>Loading...</body></html>")
            reader = Bet365OddsReader(html_file, data_folder)

            reader.scrape_odds()

            self.assertTrue(reader.data.empty)
