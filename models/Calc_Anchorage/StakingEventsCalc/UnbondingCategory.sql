-- CALC: UnbondingCategory
-- Classifies a staking event's unbonding risk tier based on the asset's
-- unbonding period. Joins ReferenceAssets to get UnbondingDays.
-- Used for NetPen exposure risk bucketing on client statements.
--   Immediate  (0 days)  → ETH post-merge, ADA
--   ShortTail  (1-3)     → SOL, MATIC
--   MediumTail (4-21)    → ATOM
--   LongTail   (>21)     → DOT — highest period-end timing risk

SELECT
  StakingEvents.ID AS ID
  , CASE
      WHEN ReferenceAssets.UnbondingDays = 0 THEN 'Immediate'
      WHEN ReferenceAssets.UnbondingDays <= 3 THEN 'ShortTail'
      WHEN ReferenceAssets.UnbondingDays <= 21 THEN 'MediumTail'
      ELSE 'LongTail'
    END AS UnbondingCategory
FROM {{ source('anchorage_data_platform', 'StakingEvents') }} AS StakingEvents
JOIN {{ source('anchorage_data_platform', 'ReferenceAssets') }} AS ReferenceAssets
  ON ReferenceAssets.AssetId = StakingEvents.AssetId
