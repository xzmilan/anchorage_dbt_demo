-- CALC: IsSettled
-- TRUE when a staking reward has been settled and is liquid/distributable to the client.
-- FALSE (pending) = unbonding — appears as NetPen on client statements.
-- Drives the settled vs pending split in StakingRewardsByPeriod.

SELECT
  StakingEvents.ID AS ID
  , UPPER(StakingEvents.RewardState) = 'SETTLED' AS IsSettled
FROM {{ source('anchorage_data_platform', 'StakingEvents') }} AS StakingEvents
