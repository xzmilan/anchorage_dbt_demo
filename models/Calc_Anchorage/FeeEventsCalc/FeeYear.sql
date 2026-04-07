-- CALC: FeeYear
-- The calendar year the fee billing period starts in.
-- Used for annual revenue reporting and YoY fee trend analysis.

SELECT
  FeeEvents.ID AS ID
  , EXTRACT(YEAR FROM FeeEvents.FeePeriodStart) AS FeeYear
FROM {{ source('anchorage_data_platform', 'FeeEvents') }} AS FeeEvents
