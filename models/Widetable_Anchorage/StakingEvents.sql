-- WIDETABLE: StakingEvents
-- Struct-based join of StakingEvents semantic base and all its calcs.
-- No explicit columns. Add a micro-calc to StakingEventsCalc/ and re-run
-- generate_calc_views.py — this widetable picks up the new field automatically.
-- Consumer: StakingRewardsByPeriod business view.

SELECT
  StakingEvents
  , StakingEventsCalc
FROM {{ source('anchorage_data_platform', 'StakingEvents') }} AS StakingEvents
JOIN {{ source('anchorage_calc', 'StakingEventsCalc') }} AS StakingEventsCalc
  ON StakingEventsCalc.ID = StakingEvents.ID
