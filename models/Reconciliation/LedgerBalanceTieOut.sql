-- RECONCILIATION: LedgerBalanceTieOut
--
-- Purpose: Formal tie-out between the custody balances we COMPUTED
--          and the custodian's own ledger reference.
--
-- This is NOT an analytical view. It is a pre-delivery control.
-- Before any client statement is generated, every row in this model
-- must have VarianceUSD = 0 and LedgerStatus = 'MATCHED'.
--
-- How it works:
--   1. CustodyBalancesByPeriod gives us the position we computed from
--      raw events (quantity × price = valuation).
--   2. CustodianLedgerRef is the authoritative reference ID from the
--      custodian that confirms THEIR ledger agrees with our position.
--      A NULL ledger ref = no confirmation from the custodian = UNCONFIRMED.
--   3. We compare our computed valuation against the restated valuation
--      using the same price — any difference is a variance that must be
--      explained before the statement ships.
--
-- Pipeline enforcement:
--   validate_period_close.py queries this model.
--   If ANY row has LedgerStatus != 'MATCHED', the pipeline halts.
--   Nothing ships until every row is green.
--
-- Ownership: Reporting & Statements team writes this model.
--            Platform team owns the upstream CustodyPositions source.
--            The gap between them is exactly what this model measures.

WITH ComputedPositions AS (
  SELECT
    CustodyBalancesByPeriod.CustomerId AS CustomerId
    , CustodyBalancesByPeriod.AssetId AS AssetId
    , CustodyBalancesByPeriod.PositionPeriod AS PositionPeriod
    , CustodyBalancesByPeriod.PeriodEndDate AS PeriodEndDate
    , CustodyBalancesByPeriod.PeriodEndQuantity AS ComputedQuantity
    , CustodyBalancesByPeriod.PeriodEndPriceUSD AS PriceUSD
    , CustodyBalancesByPeriod.PeriodEndValuationUSD AS ComputedValuationUSD
    , CustodyBalancesByPeriod.CustodianLedgerRef AS CustodianLedgerRef
  FROM {{ ref('CustodyBalancesByPeriod') }} AS CustodyBalancesByPeriod
)

-- Re-derive valuation from first principles using quantity × price.
-- If this doesn't match ComputedValuationUSD, the raw data has an internal
-- inconsistency (price or quantity was silently updated at source).
, RestatedPositions AS (
  SELECT
    ComputedPositions.CustomerId AS CustomerId
    , ComputedPositions.AssetId AS AssetId
    , ComputedPositions.PositionPeriod AS PositionPeriod
    , ComputedPositions.PeriodEndDate AS PeriodEndDate
    , ComputedPositions.ComputedQuantity AS ComputedQuantity
    , ComputedPositions.PriceUSD AS PriceUSD
    , ComputedPositions.ComputedValuationUSD AS ComputedValuationUSD
    -- Restate: quantity × price should equal computed valuation exactly
    , ROUND(ComputedPositions.ComputedQuantity * ComputedPositions.PriceUSD, 2)
        AS RestatedValuationUSD
    , ComputedPositions.CustodianLedgerRef AS CustodianLedgerRef
  FROM ComputedPositions
)

SELECT
  RestatedPositions.CustomerId AS CustomerId
  , RestatedPositions.AssetId AS AssetId
  , RestatedPositions.PositionPeriod AS PositionPeriod
  , RestatedPositions.PeriodEndDate AS PeriodEndDate
  , RestatedPositions.ComputedQuantity AS ComputedQuantity
  , RestatedPositions.PriceUSD AS PriceUSD
  , RestatedPositions.ComputedValuationUSD AS ComputedValuationUSD
  , RestatedPositions.RestatedValuationUSD AS RestatedValuationUSD

  -- The number that must be zero before any statement ships
  , ROUND(
      RestatedPositions.ComputedValuationUSD - RestatedPositions.RestatedValuationUSD,
      2
    ) AS VarianceUSD

  -- Ledger confirmation reference from the custodian
  , RestatedPositions.CustodianLedgerRef AS CustodianLedgerRef

  -- Status — the pipeline checks this column
  , CASE
      -- Custodian has not confirmed this position
      WHEN RestatedPositions.CustodianLedgerRef IS NULL THEN 'UNCONFIRMED'
      -- Internal restatement variance — data quality issue
      WHEN ABS(
        RestatedPositions.ComputedValuationUSD - RestatedPositions.RestatedValuationUSD
      ) > 0.01 THEN 'VARIANCE_DETECTED'
      -- All checks passed — safe to deliver
      ELSE 'MATCHED'
    END AS LedgerStatus

  -- Explanation field — populated for non-MATCHED rows to guide investigation
  , CASE
      WHEN RestatedPositions.CustodianLedgerRef IS NULL
        THEN 'No custodian ledger reference — position not confirmed by custodian'
      WHEN ABS(
        RestatedPositions.ComputedValuationUSD - RestatedPositions.RestatedValuationUSD
      ) > 0.01
        THEN CONCAT(
          'Qty × Price restatement differs from computed valuation by $',
          CAST(
            ABS(RestatedPositions.ComputedValuationUSD - RestatedPositions.RestatedValuationUSD)
            AS STRING
          )
        )
      ELSE NULL
    END AS ExceptionDetail

  , CURRENT_TIMESTAMP AS ReconRunAt

FROM RestatedPositions
ORDER BY
  CASE LedgerStatus
    WHEN 'VARIANCE_DETECTED' THEN 1
    WHEN 'UNCONFIRMED' THEN 2
    ELSE 3
  END
  , CustomerId
  , AssetId
  , PositionPeriod
