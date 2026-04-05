-- CALC: EventPeriod
-- The calendar month a financial event's period date falls in.
-- Used to aggregate the canonical event log to monthly period totals
-- for reconciliation against reported position and fee figures.

SELECT
  FinancialEvents.ID AS ID
  , DATE_TRUNC(FinancialEvents.PeriodDate, MONTH) AS EventPeriod
FROM {{ source('anchorage_data_platform', 'FinancialEvents') }} AS FinancialEvents
