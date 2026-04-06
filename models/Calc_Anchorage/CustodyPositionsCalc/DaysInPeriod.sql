-- CALC: DaysInPeriod
-- Number of calendar days in the month the position date falls in.
-- Used to prorate daily AUM for custody fee calculations:
-- DailyAUM = ValuationUSD / DaysInPeriod × (days held in period).

SELECT
  CustodyPositions.ID AS ID
  , DATE_DIFF(
      LAST_DAY(CustodyPositions.PositionDate, MONTH),
      DATE_TRUNC(CustodyPositions.PositionDate, MONTH),
      DAY
    ) + 1 AS DaysInPeriod
FROM {{ source('anchorage_data_platform', 'CustodyPositions') }} AS CustodyPositions
