-- CALC: FeeQuarter
-- The calendar quarter the fee billing period starts in.
-- Used for quarterly fee revenue roll-ups and quarter-over-quarter accrual comparisons.

SELECT
  FeeEvents.ID AS ID
  , DATE_TRUNC(FeeEvents.FeePeriodStart, QUARTER) AS FeeQuarter
FROM {{ source('anchorage_data_platform', 'FeeEvents') }} AS FeeEvents
