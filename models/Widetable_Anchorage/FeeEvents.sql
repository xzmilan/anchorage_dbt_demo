-- WIDETABLE: FeeEvents
-- Struct-based join of FeeEvents semantic base and all its calcs.
-- No explicit columns. Add a micro-calc to FeeEventsCalc/ and re-run
-- generate_calc_views.py — this widetable picks up the new field automatically.
-- Consumer: FeeAccrualsByPeriod business view.

SELECT
  FeeEvents
  , FeeEventsCalc
FROM {{ source('anchorage_data_platform', 'FeeEvents') }} AS FeeEvents
JOIN {{ ref('FeeEventsCalc') }} AS FeeEventsCalc
  ON FeeEventsCalc.ID = FeeEvents.ID
