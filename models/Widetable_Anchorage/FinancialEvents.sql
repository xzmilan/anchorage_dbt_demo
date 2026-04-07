-- WIDETABLE: FinancialEvents
-- Struct-based join of FinancialEvents semantic base and all its calcs.
-- No explicit columns. Add a micro-calc to FinancialEventsCalc/ and re-run
-- generate_calc_views.py — this widetable picks up the new field automatically.
-- Consumer: ReconciliationSummary business view.

SELECT
  FinancialEvents
  , FinancialEventsCalc
FROM {{ source('anchorage_data_platform', 'FinancialEvents') }} AS FinancialEvents
JOIN {{ ref('FinancialEventsCalc') }} AS FinancialEventsCalc
  ON FinancialEventsCalc.ID = FinancialEvents.ID
