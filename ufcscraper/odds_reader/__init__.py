from .bet365_odds_reader import Bet365OddsReader
from .williamhill_odds_reader import WilliamHillOddsReader
from .base import BaseOdds

class Odds(BaseOdds):
    filename = "odds.csv"
    upcoming = False

class UpcomingOdds(BaseOdds):
    filename = "upcoming/odds.csv"
    upcoming = True