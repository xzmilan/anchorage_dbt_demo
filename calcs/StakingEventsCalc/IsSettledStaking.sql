-- CALC: IsSettledStaking
-- TRUE when a staking reward has been settled and is liquid/distributable to the client.
-- FALSE (pending) = unbonding — appears as NetPen on client statements.
-- Drives the settled vs pending split in StakingRewardsByPeriod.
-- Named IsSettledStaking (not IsSettled) to avoid collision with IsSettledFinancial.

SELECT
  StakingEvents.ID AS ID
  , UPPER(StakingEvents.RewardState) = 'SETTLED' AS IsSettledStaking
FROM {{ source('anchorage_data_platform', 'StakingEvents') }} AS StakingEvents
