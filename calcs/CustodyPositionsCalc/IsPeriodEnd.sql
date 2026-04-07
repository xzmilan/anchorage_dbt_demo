-- CALC: IsPeriodEnd
-- TRUE when the position snapshot date is the last day of its calendar month.
-- Period-end rows are the authoritative balance for monthly client statements.
-- Non-period-end rows are intra-month snapshots used for daily repricing only.

SELECT
  CustodyPositions.ID AS ID
  , CustodyPositions.PositionDate = LAST_DAY(CustodyPositions.PositionDate, MONTH) AS IsPeriodEnd
FROM {{ source('anchorage_data_platform', 'CustodyPositions') }} AS CustodyPositions
