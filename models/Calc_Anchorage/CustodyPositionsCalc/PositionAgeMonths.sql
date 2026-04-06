-- CALC: PositionAgeMonths
-- Months elapsed between the position record's creation date and its snapshot date.
-- Zero indicates the position was created in the same month it was first snapshotted.
-- Used for position vintage analysis and long-standing balance trend reporting.

SELECT
  CustodyPositions.ID AS ID
  , DATE_DIFF(
      CustodyPositions.PositionDate,
      DATE(CustodyPositions.CreatedAt),
      MONTH
    ) AS PositionAgeMonths
FROM {{ source('anchorage_data_platform', 'CustodyPositions') }} AS CustodyPositions
