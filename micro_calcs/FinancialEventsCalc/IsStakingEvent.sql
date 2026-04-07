-- CALC: IsStakingEvent
-- TRUE when the event originates from the staking product line.
-- Staking events require a separate reconciliation path against the
-- StakingEvents table — they cannot be reconciled via custody positions alone.

SELECT
  FinancialEvents.ID AS ID
  , UPPER(FinancialEvents.ProductLine) = 'STAKING' AS IsStakingEvent
FROM {{ source('anchorage_data_platform', 'FinancialEvents') }} AS FinancialEvents
