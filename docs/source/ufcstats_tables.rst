=================
UFC stats tables
=================

The UFC Scraper generates several data tables that store information about events, fights, fighters, and rounds.

Event Data
------------

This table stores information about UFC events, including their names, dates, and locations:

- **event_id:** Unique identifier for the event.
- **event_name:** The name of the UFC event.
- **event_date:** The date the event took place.
- **event_city:** The city where the event was held.
- **event_state:** The state or region where the event was held.
- **event_country:** The country where the event was held.

Fight Data
------------------

This table contains detailed data about individual UFC fights, including participants, outcomes, and fight characteristics:

- **fight_id:** Unique identifier for the fight.
- **event_id:** Identifier linking the fight to a specific event.
- **referee:** The referee who officiated the fight.
- **fighter_1:** Unique identifier of the first fighter.
- **fighter_2:** Unique identifier ofr the second fighter.
- **winner:** Identifier of the winning fighter.
- **num_rounds:** The number of rounds the fight was set to.
- **title_fight:** Indicates if the fight was for a title (True/False).
- **weight_class:** The weight class in which the fight occurred.
- **gender:** The gender category of the fighters.
- **result:** The outcome of the fight (e.g., KO, Submission).
- **result_details:** Additional information about the fight result.
- **finish_round:** The round in which the fight ended.
- **finish_time:** The time in the round when the fight ended.
- **time_format:** The format of the time used (e.g., 5-5-5 for 3 rounds of 5 minutos).

Fighter Data
--------------------

This table contains detailed information about UFC fighters, including physical attributes and fight records.

- **fighter_id:** Unique identifier for the fighter.
- **fighter_f_name:** The first name of the fighter.
- **fighter_l_name:** The last name of the fighter.
- **fighter_nickname:** The nickname of the fighter.
- **fighter_height_cm:** The height of the fighter in centimeters.
- **fighter_weight_lbs:** The weight of the fighter in pounds.
- **fighter_reach_cm:** The reach of the fighter in centimeters.
- **fighter_stance:** The fighting stance of the fighter (e.g., Orthodox, Southpaw).
- **fighter_dob:** The date of birth of the fighter.
- **fighter_w:** The number of wins in the fighter's career.
- **fighter_l:** The number of losses in the fighter's career.
- **fighter_d:** The number of draws in the fighter's career.
- **fighter_nc_dq:** The number of no contests or disqualifications in the fighter's career.

Round Data
------------------

This table holds detailed statistics for each round of a UFC fight, including strikes, takedowns, and control time. Here all strikes are significant except the ``total_strikes`` fields.

- **fight_id:** Unique identifier for the fight.
- **fighter_id:** Unique identifier for the fighter.
- **round:** The round number.
- **knockdowns:** The number of knockdowns achieved by the fighter in the round.
- **strikes_att:** The number of strikes attempted by the fighter in the round.
- **strikes_succ:** The number of strikes successfully landed by the fighter in the round.
- **head_strikes_att:** The number of head strikes attempted by the fighter.
- **head_strikes_succ:** The number of head strikes successfully landed by the fighter.
- **body_strikes_att:** The number of body strikes attempted by the fighter.
- **body_strikes_succ:** The number of body strikes successfully landed by the fighter.
- **leg_strikes_att:** The number of leg strikes attempted by the fighter.
- **leg_strikes_succ:** The number of leg strikes successfully landed by the fighter.
- **distance_strikes_att:** The number of strikes attempted from a distance.
- **distance_strikes_succ:** The number of strikes landed from a distance.
- **ground_strikes_att:** The number of ground strikes attempted.
- **ground_strikes_succ:** The number of ground strikes successfully landed.
- **clinch_strikes_att:** The number of clinch strikes attempted.
- **clinch_strikes_succ:** The number of clinch strikes successfully landed.
- **total_strikes_att:** The total number of strikes attempted in the round (including non-significant ones).
- **total_strikes_succ:** The total number of strikes successfully landed in the round (including non-significant ones).
- **takedown_att:** The number of takedown attempts.
- **takedown_succ:** The number of successful takedowns.
- **submission_att:** The number of submission attempts.
- **reversals:** The number of reversals achieved by the fighter.
- **ctrl_time:** The amount of control time achieved by the fighter.