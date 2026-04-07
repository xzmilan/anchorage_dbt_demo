-- CALC: EventQuarter
-- The calendar quarter the financial event's period date falls in.
-- Used for quarterly P&L aggregation and quarter-end reconciliation reporting.

SELECT
  FinancialEvents.ID AS ID
  , DATE_TRUNC(FinancialEvents.PeriodDate, QUARTER) AS EventQuarter
FROM {{ source('anchorage_data_platform', 'FinancialEvents') }} AS FinancialEvents
