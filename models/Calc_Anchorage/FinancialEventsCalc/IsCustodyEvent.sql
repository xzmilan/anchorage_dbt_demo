-- CALC: IsCustodyEvent
-- TRUE when the event originates from the custody product line.
-- Used to partition the unified financial event log for custody-specific
-- reconciliation against the CustodyPositions balance sheet.

SELECT
  FinancialEvents.ID AS ID
  , UPPER(FinancialEvents.ProductLine) = 'CUSTODY' AS IsCustodyEvent
FROM {{ source('anchorage_data_platform', 'FinancialEvents') }} AS FinancialEvents
