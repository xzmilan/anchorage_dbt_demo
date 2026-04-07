-- CALC: IsStablecoin
-- TRUE when the position asset is a USD-pegged stablecoin (USDC or USDT).
-- Stablecoin positions carry near-zero market risk and are reported separately
-- from volatile crypto holdings on client statements.

SELECT
  CustodyPositions.ID AS ID
  , CustodyPositions.AssetId IN ('ASSET_USDC', 'ASSET_USDT') AS IsStablecoin
FROM {{ source('anchorage_data_platform', 'CustodyPositions') }} AS CustodyPositions
