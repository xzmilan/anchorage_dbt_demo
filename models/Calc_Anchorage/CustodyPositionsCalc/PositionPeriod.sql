-- CALC: PositionPeriod
-- The calendar month a custody position snapshot belongs to.
-- Used to group daily snapshots into monthly period-end balances
-- in the CustodyBalancesByPeriod business view.

SELECT
  CustodyPositions.ID AS ID
  , DATE_TRUNC(CustodyPositions.PositionDate, MONTH) AS PositionPeriod
FROM {{ source('anchorage_data_platform', 'CustodyPositions') }} AS CustodyPositions
