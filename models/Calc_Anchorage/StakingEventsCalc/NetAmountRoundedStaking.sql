-- CALC: NetAmountRoundedStaking
-- NetAmount rounded to 8 decimal places (satoshi-level precision standard).
-- The raw value preserves BIGNUMERIC precision for reconciliation arithmetic.
-- This rounded value is used for client-facing statement reward line items.
-- Named NetAmountRoundedStaking (not NetAmountRounded) to avoid collision with NetAmountRoundedFinancial.

SELECT
  StakingEvents.ID AS ID
  , ROUND(StakingEvents.NetAmount, 8) AS NetAmountRoundedStaking
FROM {{ source('anchorage_data_platform', 'StakingEvents') }} AS StakingEvents
