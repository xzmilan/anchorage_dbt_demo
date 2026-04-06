-- CALC: IsWaived
-- TRUE when the fee has been explicitly waived (credited back) for the client.
-- Waived fees must still appear on statements as a zero-amount line item with
-- WAIVED status for regulatory disclosure — they cannot be silently omitted.

SELECT
  FeeEvents.ID AS ID
  , UPPER(FeeEvents.BillingStatus) = 'WAIVED' AS IsWaived
FROM {{ source('anchorage_data_platform', 'FeeEvents') }} AS FeeEvents
