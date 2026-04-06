-- CALC: NetAmountRounded
-- NetAmount rounded to the nearest cent (2 decimal places).
-- The raw NetAmount preserves BIGNUMERIC precision for reconciliation arithmetic.
-- This rounded value is used for all client-facing statement line items.

SELECT
  FinancialEvents.ID AS ID
  , ROUND(FinancialEvents.NetAmount, 2) AS NetAmountRounded
FROM {{ source('anchorage_data_platform', 'FinancialEvents') }} AS FinancialEvents
