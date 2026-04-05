-- CALC: IsFullyBilled
-- TRUE when a fee event has been invoiced and collected from the client.
-- FALSE = accrued but not yet billed. Compliance gate: all fees must be
-- TRUE before a period can be marked RECONCILED.

SELECT
  FeeEvents.ID AS ID
  , UPPER(FeeEvents.BillingStatus) = 'BILLED' AS IsFullyBilled
FROM {{ source('anchorage_data_platform', 'FeeEvents') }} AS FeeEvents
