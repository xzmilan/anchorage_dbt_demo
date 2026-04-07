-- CALC: AssetClass
-- Broad asset classification used for portfolio risk bucketing and statement layout.
--   Stablecoin  → USDC, USDT (USD-pegged, near-zero market risk)
--   Layer1      → BTC, ETH, SOL (L1 settlement layer assets)
--   AltCoin     → ADA, MATIC, ATOM, DOT (higher volatility, protocol-specific risk)

SELECT
  CustodyPositions.ID AS ID
  , CASE
      WHEN CustodyPositions.AssetId IN ('ASSET_USDC', 'ASSET_USDT') THEN 'Stablecoin'
      WHEN CustodyPositions.AssetId IN ('ASSET_BTC', 'ASSET_ETH', 'ASSET_SOL') THEN 'Layer1'
      ELSE 'AltCoin'
    END AS AssetClass
FROM {{ source('anchorage_data_platform', 'CustodyPositions') }} AS CustodyPositions
