-- CALC: IsBelowMinDistribution
-- TRUE when the net reward amount is below the minimum distribution threshold (0.0001).
-- Sub-threshold rewards are pooled and held until they cross the threshold rather than
-- generating individual distribution events — reduces on-chain transaction noise.
-- These rows appear on statements as "accumulated pending" rather than distributed.

SELECT
  StakingEvents.ID AS ID
  , StakingEvents.NetAmount < 0.0001 AS IsBelowMinDistribution
FROM {{ source('anchorage_data_platform', 'StakingEvents') }} AS StakingEvents
