-- CALC: NetAmountRounded
-- NetAmount rounded to 8 decimal places (satoshi-level precision standard).
-- The raw value preserves BIGNUMERIC precision for reconciliation arithmetic.
-- This rounded value is used for client-facing statement reward line items.

SELECT
  StakingEvents.ID AS ID
  , ROUND(StakingEvents.NetAmount, 8) AS NetAmountRounded
FROM {{ source('anchorage_data_platform', 'StakingEvents') }} AS StakingEvents
