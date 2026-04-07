-- CALC: ValuationUSDRounded
-- ValuationUSD rounded to the nearest cent (2 decimal places).
-- Raw ValuationUSD preserves full BIGNUMERIC precision for reconciliation.
-- This rounded version is used for display on client-facing statements.

SELECT
  CustodyPositions.ID AS ID
  , ROUND(CustodyPositions.ValuationUSD, 2) AS ValuationUSDRounded
FROM {{ source('anchorage_data_platform', 'CustodyPositions') }} AS CustodyPositions
