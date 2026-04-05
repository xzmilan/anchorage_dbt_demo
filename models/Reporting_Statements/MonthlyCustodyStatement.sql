-- REPORT MODEL: MonthlyCustodyStatement
--
-- Purpose: Formatted, client-facing monthly custody statement output.
--          This is the Reporting & Statements team's layer — not a generic
--          analytical view. Every design choice here is driven by what the
--          client statement must say, not what the data warehouse stores.
--
-- Key differences from Business_View_Anchorage/ClientPeriodPositions:
--   1. Period-over-period change columns (PoP delta, PoP pct change)
--   2. KycStatus gate — only APPROVED clients appear in statements
--   3. StatementId — deterministic, auditable delivery key per statement
--   4. Non-staking assets get NULL reward columns (not 0) for clean formatting
--   5. StatementRunId — ties this row to a specific generation run for audit
--   6. PeriodLabel — client-facing string ("March 2025") not a DATE column
--
-- HOW THIS IS GENERATED:
--   This model is included in the period-end close DAG.
--   It runs after ReconciliationSummary is fully green (no open exceptions).
--   Output flows to the delivery engine (SFTP / PDF renderer / API endpoint).
--
-- GATE: Do not deliver this output unless validate_period_close.py passes.

WITH CurrentPeriod AS (
  SELECT
    ClientPeriodPositions.CustomerId AS CustomerId
    , ClientPeriodPositions.CustomerName AS CustomerName
    , ClientPeriodPositions.CustomerType AS CustomerType
    , ClientPeriodPositions.Jurisdiction AS Jurisdiction
    , ClientPeriodPositions.RiskTier AS RiskTier
    , ClientPeriodPositions.KycStatus AS KycStatus
    , ClientPeriodPositions.AssetId AS AssetId
    , ClientPeriodPositions.Symbol AS Symbol
    , ClientPeriodPositions.AssetName AS AssetName
    , ClientPeriodPositions.AssetType AS AssetType
    , ClientPeriodPositions.Protocol AS Protocol
    , ClientPeriodPositions.IsStakeable AS IsStakeable
    , ClientPeriodPositions.UnbondingDays AS UnbondingDays
    , ClientPeriodPositions.PositionPeriod AS PositionPeriod
    , ClientPeriodPositions.PeriodEndDate AS PeriodEndDate
    , ClientPeriodPositions.PeriodEndQuantity AS PeriodEndQuantity
    , ClientPeriodPositions.PeriodEndPriceUSD AS PeriodEndPriceUSD
    , ClientPeriodPositions.PeriodEndValuationUSD AS PeriodEndValuationUSD
    , ClientPeriodPositions.CustodianLedgerRef AS CustodianLedgerRef
    , ClientPeriodPositions.TotalPositionValueWithNetpenUSD AS TotalPositionValueWithNetpenUSD
    , ClientPeriodPositions.SettledNetRewards AS SettledNetRewards
    , ClientPeriodPositions.TotalNetRewardsAllStates AS TotalNetRewardsAllStates
    , ClientPeriodPositions.NetPendingRewards AS NetPendingRewards
    , ClientPeriodPositions.TotalFeesUSD AS TotalFeesUSD
    , ClientPeriodPositions.BilledFeesUSD AS BilledFeesUSD
    , ClientPeriodPositions.AllFeesBilled AS AllFeesBilled
  FROM {{ ref('ClientPeriodPositions') }} AS ClientPeriodPositions
  WHERE ClientPeriodPositions.KycStatus = 'APPROVED'         -- Only deliver to active, approved clients
)

-- Pull the immediately preceding period for period-over-period comparison
, PriorPeriod AS (
  SELECT
    ClientPeriodPositions.CustomerId AS CustomerId
    , ClientPeriodPositions.AssetId AS AssetId
    , ClientPeriodPositions.PositionPeriod AS PositionPeriod
    , ClientPeriodPositions.PeriodEndQuantity AS PriorPeriodQuantity
    , ClientPeriodPositions.PeriodEndValuationUSD AS PriorPeriodValuationUSD
    , ClientPeriodPositions.SettledNetRewards AS PriorPeriodSettledNetRewards
  FROM {{ ref('ClientPeriodPositions') }} AS ClientPeriodPositions
  WHERE ClientPeriodPositions.KycStatus = 'APPROVED'
)

, StatementRows AS (
  SELECT
    CurrentPeriod.CustomerId AS CustomerId
    , CurrentPeriod.CustomerName AS CustomerName
    , CurrentPeriod.CustomerType AS CustomerType
    , CurrentPeriod.Jurisdiction AS Jurisdiction
    , CurrentPeriod.RiskTier AS RiskTier
    , CurrentPeriod.AssetId AS AssetId
    , CurrentPeriod.Symbol AS Symbol
    , CurrentPeriod.AssetName AS AssetName
    , CurrentPeriod.AssetType AS AssetType
    , CurrentPeriod.Protocol AS Protocol
    , CurrentPeriod.IsStakeable AS IsStakeable
    , CurrentPeriod.UnbondingDays AS UnbondingDays
    , CurrentPeriod.PositionPeriod AS PositionPeriod

    -- Human-readable period label for client display (e.g. "March 2025")
    , FORMAT_DATE('%B %Y', DATE_TRUNC(CurrentPeriod.PeriodEndDate, MONTH)) AS PeriodLabel

    , CurrentPeriod.PeriodEndDate AS PeriodEndDate
    , CurrentPeriod.PeriodEndQuantity AS PeriodEndQuantity
    , CurrentPeriod.PeriodEndPriceUSD AS PeriodEndPriceUSD
    , CurrentPeriod.PeriodEndValuationUSD AS PeriodEndValuationUSD
    , CurrentPeriod.CustodianLedgerRef AS CustodianLedgerRef

    -- Period-over-period position change — key client-facing metric
    , CurrentPeriod.PeriodEndQuantity - COALESCE(PriorPeriod.PriorPeriodQuantity, 0)
        AS QuantityChangePeriodOverPeriod
    , CurrentPeriod.PeriodEndValuationUSD - COALESCE(PriorPeriod.PriorPeriodValuationUSD, 0)
        AS ValuationChangePeriodOverPeriodUSD
    , CASE
        WHEN COALESCE(PriorPeriod.PriorPeriodValuationUSD, 0) = 0 THEN NULL
        ELSE ROUND(
          (CurrentPeriod.PeriodEndValuationUSD - PriorPeriod.PriorPeriodValuationUSD)
          / PriorPeriod.PriorPeriodValuationUSD * 100
          , 4
        )
      END AS ValuationChangePctPeriodOverPeriod

    -- Staking reward columns — NULL for non-staking assets (not 0)
    -- Downstream formatter uses NULL to suppress section in PDF output
    , CASE WHEN CurrentPeriod.IsStakeable THEN CurrentPeriod.SettledNetRewards ELSE NULL END
        AS SettledNetRewards
    , CASE WHEN CurrentPeriod.IsStakeable THEN CurrentPeriod.TotalNetRewardsAllStates ELSE NULL END
        AS TotalNetRewardsAllStates
    , CASE WHEN CurrentPeriod.IsStakeable THEN CurrentPeriod.NetPendingRewards ELSE NULL END
        AS NetPendingRewards
    , CASE WHEN CurrentPeriod.IsStakeable THEN PriorPeriod.PriorPeriodSettledNetRewards ELSE NULL END
        AS PriorPeriodSettledNetRewards

    -- Total economic exposure: liquid position + pending rewards (institutional metric)
    , CurrentPeriod.TotalPositionValueWithNetpenUSD AS TotalPositionValueWithNetpenUSD

    -- Fee columns
    , CurrentPeriod.TotalFeesUSD AS TotalFeesUSD
    , CurrentPeriod.BilledFeesUSD AS BilledFeesUSD
    , CurrentPeriod.AllFeesBilled AS AllFeesBilled

    -- Deterministic statement ID — stable across re-runs for the same period
    -- Used as idempotency key in delivery engine to prevent duplicate sends
    , TO_HEX(MD5(CONCAT(
        CAST(CurrentPeriod.CustomerId AS STRING),
        '|',
        CAST(CurrentPeriod.AssetId AS STRING),
        '|',
        CAST(CurrentPeriod.PositionPeriod AS STRING)
    ))) AS StatementId

    -- Audit trail — when this version of the statement was generated
    , CURRENT_TIMESTAMP AS StatementGeneratedAt
    , 'ANCHORAGE_DIGITAL' AS IssuingEntity

  FROM CurrentPeriod
  LEFT JOIN PriorPeriod
    ON PriorPeriod.CustomerId = CurrentPeriod.CustomerId
      AND PriorPeriod.AssetId = CurrentPeriod.AssetId
      -- Prior period = one calendar month before the current period
      AND PriorPeriod.PositionPeriod = DATE_SUB(
        DATE_TRUNC(CurrentPeriod.PositionPeriod, MONTH),
        INTERVAL 1 MONTH
      )
)

SELECT
  StatementRows.StatementId AS StatementId
  , StatementRows.CustomerId AS CustomerId
  , StatementRows.CustomerName AS CustomerName
  , StatementRows.CustomerType AS CustomerType
  , StatementRows.Jurisdiction AS Jurisdiction
  , StatementRows.RiskTier AS RiskTier
  , StatementRows.AssetId AS AssetId
  , StatementRows.Symbol AS Symbol
  , StatementRows.AssetName AS AssetName
  , StatementRows.AssetType AS AssetType
  , StatementRows.Protocol AS Protocol
  , StatementRows.IsStakeable AS IsStakeable
  , StatementRows.UnbondingDays AS UnbondingDays
  , StatementRows.PositionPeriod AS PositionPeriod
  , StatementRows.PeriodLabel AS PeriodLabel
  , StatementRows.PeriodEndDate AS PeriodEndDate
  , StatementRows.PeriodEndQuantity AS PeriodEndQuantity
  , StatementRows.PeriodEndPriceUSD AS PeriodEndPriceUSD
  , StatementRows.PeriodEndValuationUSD AS PeriodEndValuationUSD
  , StatementRows.CustodianLedgerRef AS CustodianLedgerRef
  , StatementRows.QuantityChangePeriodOverPeriod AS QuantityChangePeriodOverPeriod
  , StatementRows.ValuationChangePeriodOverPeriodUSD AS ValuationChangePeriodOverPeriodUSD
  , StatementRows.ValuationChangePctPeriodOverPeriod AS ValuationChangePctPeriodOverPeriod
  , StatementRows.SettledNetRewards AS SettledNetRewards
  , StatementRows.TotalNetRewardsAllStates AS TotalNetRewardsAllStates
  , StatementRows.NetPendingRewards AS NetPendingRewards
  , StatementRows.PriorPeriodSettledNetRewards AS PriorPeriodSettledNetRewards
  , StatementRows.TotalPositionValueWithNetpenUSD AS TotalPositionValueWithNetpenUSD
  , StatementRows.TotalFeesUSD AS TotalFeesUSD
  , StatementRows.BilledFeesUSD AS BilledFeesUSD
  , StatementRows.AllFeesBilled AS AllFeesBilled
  , StatementRows.StatementGeneratedAt AS StatementGeneratedAt
  , StatementRows.IssuingEntity AS IssuingEntity
FROM StatementRows
ORDER BY
  StatementRows.CustomerName
  , StatementRows.PositionPeriod
  , StatementRows.Symbol
