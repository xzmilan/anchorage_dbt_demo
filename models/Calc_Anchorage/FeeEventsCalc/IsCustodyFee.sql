-- CALC: IsCustodyFee
-- TRUE when the fee type is a direct custody service charge.
-- Custody fees are the primary revenue line. Non-custody types include
-- staking service fees, wire fees, and one-time setup charges.

SELECT
  FeeEvents.ID AS ID
  , UPPER(FeeEvents.FeeType) = 'CUSTODY' AS IsCustodyFee
FROM {{ source('anchorage_data_platform', 'FeeEvents') }} AS FeeEvents
