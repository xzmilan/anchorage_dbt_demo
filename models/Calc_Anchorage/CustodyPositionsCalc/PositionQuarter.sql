-- CALC: PositionQuarter
-- The calendar quarter a custody position snapshot belongs to.
-- Used for quarterly roll-up reporting and quarter-over-quarter balance comparisons.

SELECT
  CustodyPositions.ID AS ID
  , DATE_TRUNC(CustodyPositions.PositionDate, QUARTER) AS PositionQuarter
FROM {{ source('anchorage_data_platform', 'CustodyPositions') }} AS CustodyPositions
