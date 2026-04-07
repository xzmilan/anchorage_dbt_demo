-- CALC: IsSettled
-- TRUE when the financial event has reached final settlement.
-- An event is only eligible for period-close inclusion once IsSettled = TRUE.
-- IsPending and IsSettled are mutually exclusive; both FALSE = cancelled/voided.

SELECT
  FinancialEvents.ID AS ID
  , UPPER(FinancialEvents.Status) = 'SETTLED' AS IsSettled
FROM {{ source('anchorage_data_platform', 'FinancialEvents') }} AS FinancialEvents
