-- CALC: ValuationTier
-- Client-segment tier based on the USD valuation of the position.
-- Used for fee tier assignment and statement routing.
--   Institutional  ≥ $10M
--   Prime          ≥ $1M
--   Standard       < $1M

SELECT
  CustodyPositions.ID AS ID
  , CASE
      WHEN CustodyPositions.ValuationUSD >= 10000000 THEN 'Institutional'
      WHEN CustodyPositions.ValuationUSD >= 1000000  THEN 'Prime'
      ELSE 'Standard'
    END AS ValuationTier
FROM {{ source('anchorage_data_platform', 'CustodyPositions') }} AS CustodyPositions
