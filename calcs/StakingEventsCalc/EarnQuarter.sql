-- CALC: EarnQuarter
-- The calendar quarter in which a staking reward was earned.
-- Used for quarterly staking yield reporting and quarter-over-quarter reward comparisons.

SELECT
  StakingEvents.ID AS ID
  , DATE_TRUNC(StakingEvents.EarnDate, QUARTER) AS EarnQuarter
FROM {{ source('anchorage_data_platform', 'StakingEvents') }} AS StakingEvents
