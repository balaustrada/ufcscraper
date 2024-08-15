======================
BestFightOdds tables
======================

The information extracted from BestFightOdds is contained in the BestFightOdds_odds table, while the fighters_names table contains lookup information to connect the two parts of the code.

Fighter Name
--------------

This table is used for tracking the names of fighters across different databases, necessary to speed up the odds collecting process. It contains the columns:

- **fighter_id:** Unique identifier for the fighter.
- **database:** The name of the database where the fighter's name is listed.
- **name:** The fighter's name as listed in the database.
- **database_id:** The identifier used by the database for the fighter.

BestFightOdds odds
---------------------

This table contains the betting odds for UFC fights, including both opening odds and the range of closing odds.

- **fight_id:** Unique identifier for the fight.
- **fighter_id:** Unique identifier for the fighter.
- **opening:** The opening betting odds for the fighter.
- **closing_range_min:** The minimum value in the range of closing betting odds.
- **closing_range_max:** The maximum value in the range of closing betting odds.