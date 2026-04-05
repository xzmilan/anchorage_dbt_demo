-- BUSINESS VIEW: StakingRewardsByPeriod
-- Monthly staking reward totals per customer and asset.
-- Settled rewards are liquid and included in client statements.
-- Pending rewards (NetPen) are accrued but not yet distributable — unbonding.
-- FeeTakeRatePct is the validator's share of gross rewards.
-- Used in ClientPeriodPositions for total economic exposure calculation.

WITH StakingEvents AS (
  SELECT
    WtStakingEvents.StakingEvents.CustomerId AS CustomerId
    , WtStakingEvents.StakingEvents.AssetId AS AssetId
    , WtStakingEvents.StakingEvents.ID AS ID
    , WtStakingEvents.StakingEvents.NetAmount AS NetAmount
    , WtStakingEvents.StakingEvents.GrossAmount AS GrossAmount
    , WtStakingEvents.StakingEvents.FeeAmount AS FeeAmount
    , WtStakingEvents.StakingEventsCalc.EarnPeriod AS EarnPeriod
    , WtStakingEvents.StakingEventsCalc.IsSettled AS IsSettled
    , WtStakingEvents.StakingEventsCalc.UnbondingCategory AS UnbondingCategory
  FROM {{ ref('StakingEvents') }} AS WtStakingEvents
)

SELECT
  StakingEvents.CustomerId AS CustomerId
  , StakingEvents.AssetId AS AssetId
  , StakingEvents.EarnPeriod AS RewardPeriod
  , StakingEvents.UnbondingCategory AS UnbondingCategory
  , SUM(
      CASE
        WHEN StakingEvents.IsSettled THEN StakingEvents.NetAmount
        ELSE 0
      END
    ) AS SettledNetRewards
  , SUM(
      CASE
        WHEN StakingEvents.IsSettled THEN StakingEvents.GrossAmount
        ELSE 0
      END
    ) AS SettledGrossRewards
  , SUM(
      CASE
        WHEN StakingEvents.IsSettled THEN StakingEvents.FeeAmount
        ELSE 0
      END
    ) AS SettledFees
  , SUM(
      CASE
        WHEN NOT StakingEvents.IsSettled THEN StakingEvents.NetAmount
        ELSE 0
      END
    ) AS NetPendingRewards
  , SUM(
      CASE
        WHEN NOT StakingEvents.IsSettled THEN StakingEvents.GrossAmount
        ELSE 0
      END
    ) AS GrossPendingRewards
  , SUM(StakingEvents.NetAmount) AS TotalNetRewardsAllStates
  , SUM(StakingEvents.GrossAmount) AS TotalGrossRewardsAllStates
  , SUM(StakingEvents.FeeAmount) AS TotalFeesAllStates
  , COUNT(StakingEvents.ID) AS TotalRewardEvents
  , SAFE_DIVIDE(
      SUM(StakingEvents.FeeAmount)
      , NULLIF(SUM(StakingEvents.GrossAmount), 0)
    ) AS FeeTakeRatePct
FROM StakingEvents
GROUP BY
  StakingEvents.CustomerId
  , StakingEvents.AssetId
  , StakingEvents.EarnPeriod
  , StakingEvents.UnbondingCategory
