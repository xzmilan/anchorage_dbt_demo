-- CALC: EarnPeriod
-- The calendar month in which a staking reward was earned.
-- Used to group events into monthly reward periods
-- in the StakingRewardsByPeriod business view.

SELECT
  StakingEvents.ID AS ID
  , DATE_TRUNC(StakingEvents.EarnDate, MONTH) AS EarnPeriod
FROM {{ source('anchorage_data_platform', 'StakingEvents') }} AS StakingEvents
