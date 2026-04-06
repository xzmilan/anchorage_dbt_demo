-- CALC: EffectiveRateBps
-- Actual effective fee rate in basis points, derived from the charged fee amount
-- and the AUM at the time of billing. Differs from contracted BasisPoints when
-- proration or waivers are applied. Used for fee audit and rate compliance checks.

SELECT
  FeeEvents.ID AS ID
  , SAFE_DIVIDE(FeeEvents.FeeAmountUSD, FeeEvents.AumAtBilling) * 10000 AS EffectiveRateBps
FROM {{ source('anchorage_data_platform', 'FeeEvents') }} AS FeeEvents
