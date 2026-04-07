-- CALC: FeePeriodDays
-- Number of calendar days in the fee billing period (end - start).
-- Standard custody billing period = 28-31 days. Values outside this range
-- flag irregular billing windows that require ops review before statement delivery.

SELECT
  FeeEvents.ID AS ID
  , DATE_DIFF(FeeEvents.FeePeriodEnd, FeeEvents.FeePeriodStart, DAY) AS FeePeriodDays
FROM {{ source('anchorage_data_platform', 'FeeEvents') }} AS FeeEvents
