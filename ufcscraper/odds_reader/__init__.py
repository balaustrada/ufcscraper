from .bet365_odds_reader import Bet365OddsReader
from .betway_odds_reader import BetwayOddsReader
from .bwin_odds_reader import BwinOddsReader
from .casino_888_odds_reader import Casino888OddsReader
from .williamhill_odds_reader import WilliamHillOddsReader
from .sportium_odds_reader import SportiumOddsReader

from .base import available_betting_houses, BaseOdds

class Odds(BaseOdds):
    filename = "odds.csv"
    upcoming = False

class UpcomingOdds(BaseOdds):
    filename = "upcoming/odds.csv"
    upcoming = True

