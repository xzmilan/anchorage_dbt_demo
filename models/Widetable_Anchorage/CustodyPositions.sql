-- WIDETABLE: CustodyPositions
-- Struct-based join of CustodyPositions semantic base and all its calcs.
-- No explicit columns. Add a micro-calc to CustodyPositionsCalc/ and re-run
-- generate_calc_views.py — this widetable picks up the new field automatically.
-- Consumer: CustodyBalancesByPeriod business view.

SELECT
  CustodyPositions
  , CustodyPositionsCalc
FROM {{ source('anchorage_data_platform', 'CustodyPositions') }} AS CustodyPositions
JOIN {{ source('Calc_Anchorage', 'CustodyPositionsCalc') }} AS CustodyPositionsCalc
  ON CustodyPositionsCalc.ID = CustodyPositions.ID