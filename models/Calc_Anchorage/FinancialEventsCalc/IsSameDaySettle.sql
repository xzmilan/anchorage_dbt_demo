-- CALC: IsSameDaySettle
-- TRUE when the event settled on the same calendar day it was timestamped.
-- Crypto-native events should typically be same-day. Non-same-day crypto events
-- may indicate protocol delays or manual holds — surfaced for ops review.
-- NULL when SettlementDate is NULL (event not yet settled).

SELECT
  FinancialEvents.ID AS ID
  , DATE(FinancialEvents.EventTimestamp) = FinancialEvents.SettlementDate AS IsSameDaySettle
FROM {{ source('anchorage_data_platform', 'FinancialEvents') }} AS FinancialEvents
