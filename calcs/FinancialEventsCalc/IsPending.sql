-- CALC: IsPending
-- TRUE when a financial event has not yet settled.
-- Used in ReconciliationSummary to surface periods with open items
-- before month-end close. Target state: zero pending events per period.

SELECT
  FinancialEvents.ID AS ID
  , UPPER(FinancialEvents.Status) = 'PENDING' AS IsPending
FROM {{ source('anchorage_data_platform', 'FinancialEvents') }} AS FinancialEvents
