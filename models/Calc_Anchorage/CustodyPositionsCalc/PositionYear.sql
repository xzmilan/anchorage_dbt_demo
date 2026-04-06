-- CALC: PositionYear
-- The calendar year a custody position snapshot belongs to.
-- Used for year-end reporting and annual AUM trend analysis.

SELECT
  CustodyPositions.ID AS ID
  , EXTRACT(YEAR FROM CustodyPositions.PositionDate) AS PositionYear
FROM {{ source('anchorage_data_platform', 'CustodyPositions') }} AS CustodyPositions
