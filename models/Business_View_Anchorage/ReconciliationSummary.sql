-- BUSINESS VIEW: ReconciliationSummary
-- Internal compliance view — reconciliation status per customer/asset/period.
-- Joins ClientPeriodPositions (reported figures) against FinancialEvents
-- (canonical event log) as the independent source of truth.
-- Target state before month-end close: ALL records = Reconciled.
--
-- Status priority order (first failing check wins):
--   1. MissingPosition     → no custody balance for a period with activity
--   2. FeeWithoutBalance   → fee charged but no corresponding position
--   3. HasPendingEvents    → unsettled financial events remain open
--   4. UnbilledFees        → fees accrued but not yet invoiced
--   5. Reconciled          → all checks passed — safe to deliver statements

WITH FinancialEvents AS (
  SELECT
    WtFinancialEvents.FinancialEvents.CustomerId AS CustomerId
    , WtFinancialEvents.FinancialEvents.AssetId AS AssetId
    , WtFinancialEvents.FinancialEvents.ID AS ID
    , WtFinancialEvents.FinancialEvents.GrossAmount AS GrossAmount
    , WtFinancialEvents.FinancialEvents.NetAmount AS NetAmount
    , WtFinancialEvents.FinancialEventsCalc.EventPeriod AS EventPeriod
    , WtFinancialEvents.FinancialEventsCalc.IsPending AS IsPending
  FROM {{ ref('FinancialEvents') }} AS WtFinancialEvents
)

, FinancialEventTotals AS (
  SELECT
    FinancialEvents.CustomerId AS CustomerId
    , FinancialEvents.AssetId AS AssetId
    , FinancialEvents.EventPeriod AS EventPeriod
    , COUNT(FinancialEvents.ID) AS RawEventCount
    , SUM(FinancialEvents.GrossAmount) AS SumGrossFromEvents
    , SUM(FinancialEvents.NetAmount) AS SumNetFromEvents
    , COUNT(
        CASE
          WHEN FinancialEvents.IsPending THEN FinancialEvents.ID
        END
      ) AS PendingEventCount
  FROM FinancialEvents
  GROUP BY
    FinancialEvents.CustomerId
    , FinancialEvents.AssetId
    , FinancialEvents.EventPeriod
)

, ReconciliationRows AS (
  SELECT
    ClientPeriodPositions.CustomerId AS CustomerId
    , ClientPeriodPositions.CustomerName AS CustomerName
    , ClientPeriodPositions.AssetId AS AssetId
    , ClientPeriodPositions.Symbol AS Symbol
    , ClientPeriodPositions.PositionPeriod AS PositionPeriod
    , ClientPeriodPositions.RiskTier AS RiskTier
    , ClientPeriodPositions.PeriodEndValuationUSD AS ReportedPositionUSD
    , ClientPeriodPositions.CustodianLedgerRef AS CustodianSourceRef
    , ClientPeriodPositions.TotalGrossRewardsAllStates AS ReportedGrossRewards
    , ClientPeriodPositions.TotalStakingFees AS ReportedStakingFees
    , ClientPeriodPositions.TotalNetRewardsAllStates AS ReportedNetRewards
    , ClientPeriodPositions.NetPendingRewards AS NetPendingRewards
    , ClientPeriodPositions.TotalFeesUSD AS ReportedFeesChargedUSD
    , ClientPeriodPositions.BilledFeesUSD AS BilledFeesUSD
    , ClientPeriodPositions.AllFeesBilled AS AllFeesBilled
    , ClientPeriodPositions.AllInvoiceReferences AS AllInvoiceReferences
    , FinancialEventTotals.RawEventCount AS RawEventCount
    , FinancialEventTotals.SumGrossFromEvents AS SumGrossFromEvents
    , FinancialEventTotals.SumNetFromEvents AS SumNetFromEvents
    , FinancialEventTotals.PendingEventCount AS PendingEventCount
    , 'ANCHORAGE_DIGITAL' AS Entity
    , CASE
        WHEN
          ClientPeriodPositions.PeriodEndValuationUSD IS NULL
          AND FinancialEventTotals.RawEventCount > 0 THEN 'MissingPosition'
        WHEN ClientPeriodPositions.ReconFlagFeeWithoutBalance THEN 'FeeWithoutBalance'
        WHEN COALESCE(FinancialEventTotals.PendingEventCount, 0) > 0 THEN 'HasPendingEvents'
        WHEN
          ClientPeriodPositions.AllFeesBilled = FALSE
          OR ClientPeriodPositions.AllFeesBilled IS NULL THEN 'UnbilledFees'
        ELSE 'Reconciled'
      END AS ReconciliationStatus
    , CURRENT_TIMESTAMP AS ReconciliationRunAt
  FROM {{ ref('ClientPeriodPositions') }} AS ClientPeriodPositions
  LEFT JOIN FinancialEventTotals
    ON FinancialEventTotals.CustomerId = ClientPeriodPositions.CustomerId
      AND FinancialEventTotals.AssetId = ClientPeriodPositions.AssetId
      AND FinancialEventTotals.EventPeriod = ClientPeriodPositions.PositionPeriod
)

SELECT
  ReconciliationRows.CustomerId AS CustomerId
  , ReconciliationRows.CustomerName AS CustomerName
  , ReconciliationRows.AssetId AS AssetId
  , ReconciliationRows.Symbol AS Symbol
  , ReconciliationRows.PositionPeriod AS PositionPeriod
  , ReconciliationRows.RiskTier AS RiskTier
  , ReconciliationRows.ReportedPositionUSD AS ReportedPositionUSD
  , ReconciliationRows.CustodianSourceRef AS CustodianSourceRef
  , ReconciliationRows.ReportedGrossRewards AS ReportedGrossRewards
  , ReconciliationRows.ReportedStakingFees AS ReportedStakingFees
  , ReconciliationRows.ReportedNetRewards AS ReportedNetRewards
  , ReconciliationRows.NetPendingRewards AS NetPendingRewards
  , ReconciliationRows.ReportedFeesChargedUSD AS ReportedFeesChargedUSD
  , ReconciliationRows.BilledFeesUSD AS BilledFeesUSD
  , ReconciliationRows.AllFeesBilled AS AllFeesBilled
  , ReconciliationRows.AllInvoiceReferences AS AllInvoiceReferences
  , ReconciliationRows.RawEventCount AS RawEventCount
  , ReconciliationRows.SumGrossFromEvents AS SumGrossFromEvents
  , ReconciliationRows.SumNetFromEvents AS SumNetFromEvents
  , ReconciliationRows.PendingEventCount AS PendingEventCount
  , ReconciliationRows.Entity AS Entity
  , ReconciliationRows.ReconciliationStatus AS ReconciliationStatus
  , ReconciliationRows.ReconciliationRunAt AS ReconciliationRunAt
FROM ReconciliationRows
ORDER BY
  CASE ReconciliationStatus
    WHEN 'MissingPosition' THEN 1
    WHEN 'FeeWithoutBalance' THEN 2
    WHEN 'HasPendingEvents' THEN 3
    WHEN 'UnbilledFees' THEN 4
    ELSE 5
  END
  , CustomerId
  , PositionPeriod
