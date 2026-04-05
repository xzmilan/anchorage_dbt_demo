-- BUSINESS VIEW: ClientPeriodPositions
-- One row per customer / asset / period — the fully denormalized reporting spine.
-- All three teams' data unified: Platform (custody), Staking, Fees.
-- Consumer teams query this table directly — no joins required.
--
-- TotalPositionValueWithNetpenUSD is the key institutional metric:
--   PeriodEndValuationUSD + (NetPendingRewards * PeriodEndPriceUSD)
--   = liquid position + unrealized pending rewards = true economic exposure.
--
-- ReconFlagFeeWithoutBalance surfaces records that need investigation
-- before month-end close. All flags resolved = safe to deliver statements.

WITH FeesCollapsedByPeriod AS (
  -- Collapse FeeType dimension so fees join 1:1 on customer/asset/period
  SELECT
    FeeAccrualsByPeriod.CustomerId AS CustomerId
    , FeeAccrualsByPeriod.AssetId AS AssetId
    , FeeAccrualsByPeriod.FeePeriod AS FeePeriod
    , SUM(FeeAccrualsByPeriod.TotalFeeAmountUSD) AS TotalFeesUSD
    , SUM(FeeAccrualsByPeriod.BilledFeeAmountUSD) AS BilledFeesUSD
    , SUM(FeeAccrualsByPeriod.PendingFeeAmountUSD) AS PendingFeesUSD
    , LOGICAL_AND(FeeAccrualsByPeriod.AllFeesBilled) AS AllFeesBilled
    , STRING_AGG(FeeAccrualsByPeriod.InvoiceReferences, ' | ') AS AllInvoiceReferences
  FROM {{ ref('FeeAccrualsByPeriod') }} AS FeeAccrualsByPeriod
  GROUP BY
    FeeAccrualsByPeriod.CustomerId
    , FeeAccrualsByPeriod.AssetId
    , FeeAccrualsByPeriod.FeePeriod
)

SELECT
  -- Identity
  CustodyBalancesByPeriod.CustomerId AS CustomerId
  , CustodyBalancesByPeriod.AssetId AS AssetId
  , CustodyBalancesByPeriod.PositionPeriod AS PositionPeriod

  -- Customer attributes (Platform team — no join needed downstream)
  , Customers.CustomerName AS CustomerName
  , Customers.CustomerType AS CustomerType
  , Customers.Jurisdiction AS Jurisdiction
  , Customers.RiskTier AS RiskTier
  , Customers.KycStatus AS KycStatus

  -- Asset attributes (Platform team — no join needed downstream)
  , ReferenceAssets.Symbol AS Symbol
  , ReferenceAssets.AssetName AS AssetName
  , ReferenceAssets.AssetType AS AssetType
  , ReferenceAssets.Protocol AS Protocol
  , ReferenceAssets.IsStakeable AS IsStakeable
  , ReferenceAssets.UnbondingDays AS UnbondingDays

  -- Custody position metrics
  , CustodyBalancesByPeriod.PeriodEndDate AS PeriodEndDate
  , CustodyBalancesByPeriod.PeriodEndQuantity AS PeriodEndQuantity
  , CustodyBalancesByPeriod.PeriodEndPriceUSD AS PeriodEndPriceUSD
  , CustodyBalancesByPeriod.PeriodEndValuationUSD AS PeriodEndValuationUSD
  , CustodyBalancesByPeriod.CustodianLedgerRef AS CustodianLedgerRef

  -- Staking reward metrics (NULL for non-staking assets)
  , StakingRewardsByPeriod.SettledNetRewards AS SettledNetRewards
  , StakingRewardsByPeriod.SettledGrossRewards AS SettledGrossRewards
  , StakingRewardsByPeriod.SettledFees AS StakingFeesSettled
  , StakingRewardsByPeriod.NetPendingRewards AS NetPendingRewards
  , StakingRewardsByPeriod.GrossPendingRewards AS GrossPendingRewards
  , StakingRewardsByPeriod.TotalNetRewardsAllStates AS TotalNetRewardsAllStates
  , StakingRewardsByPeriod.TotalGrossRewardsAllStates AS TotalGrossRewardsAllStates
  , StakingRewardsByPeriod.TotalFeesAllStates AS TotalStakingFees
  , StakingRewardsByPeriod.TotalRewardEvents AS TotalRewardEvents
  , StakingRewardsByPeriod.FeeTakeRatePct AS StakingFeeTakeRatePct
  , StakingRewardsByPeriod.UnbondingCategory AS UnbondingCategory

  -- Fee metrics
  , FeesCollapsedByPeriod.TotalFeesUSD AS TotalFeesUSD
  , FeesCollapsedByPeriod.BilledFeesUSD AS BilledFeesUSD
  , FeesCollapsedByPeriod.PendingFeesUSD AS PendingFeesUSD
  , FeesCollapsedByPeriod.AllFeesBilled AS AllFeesBilled
  , FeesCollapsedByPeriod.AllInvoiceReferences AS AllInvoiceReferences

  -- Composite: true economic exposure = liquid position + accrued NetPen
  , CustodyBalancesByPeriod.PeriodEndValuationUSD
    + COALESCE(StakingRewardsByPeriod.NetPendingRewards, 0)
    * CustodyBalancesByPeriod.PeriodEndPriceUSD
    AS TotalPositionValueWithNetpenUSD

  -- Reconciliation flag: fee charged with no matching position
  , COALESCE(
      FeesCollapsedByPeriod.TotalFeesUSD IS NOT NULL
      AND CustodyBalancesByPeriod.PeriodEndValuationUSD IS NULL
      , FALSE
    ) AS ReconFlagFeeWithoutBalance

  , CURRENT_TIMESTAMP AS _BuiltAt

FROM {{ ref('CustodyBalancesByPeriod') }} AS CustodyBalancesByPeriod
LEFT JOIN {{ source('anchorage_data_platform', 'Customers') }} AS Customers
  ON Customers.CustomerId = CustodyBalancesByPeriod.CustomerId
LEFT JOIN {{ source('anchorage_data_platform', 'ReferenceAssets') }} AS ReferenceAssets
  ON ReferenceAssets.AssetId = CustodyBalancesByPeriod.AssetId
LEFT JOIN {{ ref('StakingRewardsByPeriod') }} AS StakingRewardsByPeriod
  ON StakingRewardsByPeriod.CustomerId = CustodyBalancesByPeriod.CustomerId
    AND StakingRewardsByPeriod.AssetId = CustodyBalancesByPeriod.AssetId
    AND StakingRewardsByPeriod.RewardPeriod = CustodyBalancesByPeriod.PositionPeriod
LEFT JOIN FeesCollapsedByPeriod
  ON FeesCollapsedByPeriod.CustomerId = CustodyBalancesByPeriod.CustomerId
    AND FeesCollapsedByPeriod.AssetId = CustodyBalancesByPeriod.AssetId
    AND FeesCollapsedByPeriod.FeePeriod = CustodyBalancesByPeriod.PositionPeriod
