-- CALC: FeePeriod
-- The calendar month a fee event's billing period starts in.
-- Used to group individual fee events into monthly accrual buckets
-- in the FeeAccrualsByPeriod business view.

SELECT
  FeeEvents.ID AS ID
  , DATE_TRUNC(FeeEvents.FeePeriodStart, MONTH) AS FeePeriod
FROM {{ source('anchorage_data_platform', 'FeeEvents') }} AS FeeEvents
