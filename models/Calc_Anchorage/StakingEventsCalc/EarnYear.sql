-- CALC: EarnYear
-- The calendar year in which a staking reward was earned.
-- Used for annual yield reporting and YoY staking revenue trend analysis.

SELECT
  StakingEvents.ID AS ID
  , EXTRACT(YEAR FROM StakingEvents.EarnDate) AS EarnYear
FROM {{ source('anchorage_data_platform', 'StakingEvents') }} AS StakingEvents
