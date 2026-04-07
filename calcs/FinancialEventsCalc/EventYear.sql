-- CALC: EventYear
-- The calendar year the financial event's period date falls in.
-- Used for annual financial reporting and year-over-year activity trend analysis.

SELECT
  FinancialEvents.ID AS ID
  , EXTRACT(YEAR FROM FinancialEvents.PeriodDate) AS EventYear
FROM {{ source('anchorage_data_platform', 'FinancialEvents') }} AS FinancialEvents
