-- CALC: FeeAmountUSDRounded
-- FeeAmountUSD rounded to the nearest cent (2 decimal places).
-- The raw value preserves full NUMERIC precision for internal reconciliation.
-- This rounded value is used for all client-facing statement line items.

SELECT
  FeeEvents.ID AS ID
  , ROUND(FeeEvents.FeeAmountUSD, 2) AS FeeAmountUSDRounded
FROM {{ source('anchorage_data_platform', 'FeeEvents') }} AS FeeEvents
