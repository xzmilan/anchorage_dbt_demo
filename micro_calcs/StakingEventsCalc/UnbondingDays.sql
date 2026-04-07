-- CALC: UnbondingDays
-- Actual number of days between earn date and settle date for this event.
-- NULL while the reward is still in the unbonding queue (SettleDate not yet set).
-- Used to validate that observed unbonding durations match protocol expectations
-- (e.g., SOL ≈ 2-3 days, DOT ≈ 28 days) and flag oracle or pipeline delays.

SELECT
  StakingEvents.ID AS ID
  , DATE_DIFF(StakingEvents.SettleDate, StakingEvents.EarnDate, DAY) AS UnbondingDays
FROM {{ source('anchorage_data_platform', 'StakingEvents') }} AS StakingEvents
