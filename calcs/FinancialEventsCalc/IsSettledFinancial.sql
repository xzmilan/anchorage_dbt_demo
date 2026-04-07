-- CALC: IsSettledFinancial
-- TRUE when the financial event has reached final settlement.
-- An event is only eligible for period-close inclusion once IsSettledFinancial = TRUE.
-- IsPending and IsSettledFinancial are mutually exclusive; both FALSE = cancelled/voided.
-- Named IsSettledFinancial (not IsSettled) to avoid collision with IsSettledStaking.

SELECT
  FinancialEvents.ID AS ID
  , UPPER(FinancialEvents.Status) = 'SETTLED' AS IsSettledFinancial
FROM {{ source('anchorage_data_platform', 'FinancialEvents') }} AS FinancialEvents
