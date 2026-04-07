-- CALC: IsAccruedNotBilled
-- TRUE when a fee has been recognized (accrued) but not yet invoiced to the client.
-- ACCRUED fees appear on internal P&L but must NOT appear on client-facing statements
-- until BillingStatus transitions to BILLED. Compliance gate for statement delivery.

SELECT
  FeeEvents.ID AS ID
  , UPPER(FeeEvents.BillingStatus) = 'ACCRUED' AS IsAccruedNotBilled
FROM {{ source('anchorage_data_platform', 'FeeEvents') }} AS FeeEvents
