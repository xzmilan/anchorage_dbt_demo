-- BUSINESS VIEW: CustodyBalancesByPeriod
-- Period-end custody balances per customer and asset.
-- Takes the latest position snapshot within each calendar month as the
-- authoritative period-end value. Multiple intra-month snapshots exist
-- in the widetable — only the final one per period is surfaced here.
-- Used as the position spine for ClientPeriodPositions.

WITH CustodyPositions AS (
  SELECT
    WtCustodyPositions.CustodyPositions.CustomerId AS CustomerId
    , WtCustodyPositions.CustodyPositions.AssetId AS AssetId
    , WtCustodyPositions.CustodyPositions.PositionDate AS PositionDate
    , WtCustodyPositions.CustodyPositions.Quantity AS Quantity
    , WtCustodyPositions.CustodyPositions.PriceUSD AS PriceUSD
    , WtCustodyPositions.CustodyPositions.ValuationUSD AS ValuationUSD
    , WtCustodyPositions.CustodyPositions.CustodianLedgerRef AS CustodianLedgerRef
    , WtCustodyPositions.CustodyPositionsCalc.PositionPeriod AS PositionPeriod
  FROM {{ ref('CustodyPositions') }} AS WtCustodyPositions
)

, LatestSnapshotByPeriod AS (
  SELECT
    CustodyPositions.CustomerId AS CustomerId
    , CustodyPositions.AssetId AS AssetId
    , CustodyPositions.PositionPeriod AS PositionPeriod
    , MAX(CustodyPositions.PositionDate) AS PeriodEndDate
  FROM CustodyPositions
  GROUP BY
    CustodyPositions.CustomerId
    , CustodyPositions.AssetId
    , CustodyPositions.PositionPeriod
)

SELECT
  CustodyPositions.CustomerId AS CustomerId
  , CustodyPositions.AssetId AS AssetId
  , CustodyPositions.PositionPeriod AS PositionPeriod
  , CustodyPositions.PositionDate AS PeriodEndDate
  , CustodyPositions.Quantity AS PeriodEndQuantity
  , CustodyPositions.PriceUSD AS PeriodEndPriceUSD
  , CustodyPositions.ValuationUSD AS PeriodEndValuationUSD
  , CustodyPositions.CustodianLedgerRef AS CustodianLedgerRef
FROM CustodyPositions
JOIN LatestSnapshotByPeriod
  ON LatestSnapshotByPeriod.CustomerId = CustodyPositions.CustomerId
    AND LatestSnapshotByPeriod.AssetId = CustodyPositions.AssetId
    AND LatestSnapshotByPeriod.PositionPeriod = CustodyPositions.PositionPeriod
    AND LatestSnapshotByPeriod.PeriodEndDate = CustodyPositions.PositionDate
