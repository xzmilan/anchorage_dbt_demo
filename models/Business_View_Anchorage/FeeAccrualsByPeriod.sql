-- BUSINESS VIEW: FeeAccrualsByPeriod
-- Monthly fee accruals per customer, asset, and fee type.
-- BilledFeeAmountUSD = collected. PendingFeeAmountUSD = accrued, not yet invoiced.
-- AllFeesBilled is the compliance gate — must be TRUE for a period to close.
-- InvoiceReferences provides audit trail back to billing system.

WITH FeeEvents AS (
  SELECT
    WtFeeEvents.FeeEvents.CustomerId AS CustomerId
    , WtFeeEvents.FeeEvents.AssetId AS AssetId
    , WtFeeEvents.FeeEvents.ID AS ID
    , WtFeeEvents.FeeEvents.FeeType AS FeeType
    , WtFeeEvents.FeeEvents.FeeAmountUSD AS FeeAmountUSD
    , WtFeeEvents.FeeEvents.AumAtBilling AS AumAtBilling
    , WtFeeEvents.FeeEvents.BasisPoints AS BasisPoints
    , WtFeeEvents.FeeEvents.InvoiceReference AS InvoiceReference
    , WtFeeEvents.FeeEventsCalc.FeePeriod AS FeePeriod
    , WtFeeEvents.FeeEventsCalc.IsFullyBilled AS IsFullyBilled
  FROM {{ ref('FeeEvents') }} AS WtFeeEvents
)

SELECT
  FeeEvents.CustomerId AS CustomerId
  , FeeEvents.AssetId AS AssetId
  , FeeEvents.FeePeriod AS FeePeriod
  , FeeEvents.FeeType AS FeeType
  , SUM(FeeEvents.FeeAmountUSD) AS TotalFeeAmountUSD
  , SUM(FeeEvents.AumAtBilling) AS TotalAumBilled
  , AVG(FeeEvents.BasisPoints) AS AvgBasisPoints
  , COUNT(FeeEvents.ID) AS FeeEventCount
  , SUM(
      CASE
        WHEN FeeEvents.IsFullyBilled THEN FeeEvents.FeeAmountUSD
        ELSE 0
      END
    ) AS BilledFeeAmountUSD
  , SUM(
      CASE
        WHEN NOT FeeEvents.IsFullyBilled THEN FeeEvents.FeeAmountUSD
        ELSE 0
      END
    ) AS PendingFeeAmountUSD
  , LOGICAL_AND(FeeEvents.IsFullyBilled) AS AllFeesBilled
  , STRING_AGG(FeeEvents.InvoiceReference, ', ') AS InvoiceReferences
FROM FeeEvents
GROUP BY
  FeeEvents.CustomerId
  , FeeEvents.AssetId
  , FeeEvents.FeePeriod
  , FeeEvents.FeeType
