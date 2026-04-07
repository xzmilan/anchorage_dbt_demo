-- CALC: SettlePeriod
-- The calendar month in which a staking reward settled (became liquid/distributable).
-- NULL for rewards still in unbonding (SettleDate IS NULL / RewardState = 'pending').
-- Drives the settled vs pending period split for NetPen exposure reporting.

SELECT
  StakingEvents.ID AS ID
  , DATE_TRUNC(StakingEvents.SettleDate, MONTH) AS SettlePeriod
FROM {{ source('anchorage_data_platform', 'StakingEvents') }} AS StakingEvents
