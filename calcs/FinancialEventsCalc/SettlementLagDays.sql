-- CALC: SettlementLagDays
-- Days elapsed from event timestamp (trade date) to settlement date.
-- Standard expected lags: T+0 for crypto, T+1 for stablecoin, T+2 for OTC.
-- Values > 5 are flagged for ops investigation before period close.
-- NULL when SettlementDate is NULL (event not yet settled).

SELECT
  FinancialEvents.ID AS ID
  , DATE_DIFF(
      FinancialEvents.SettlementDate,
      DATE(FinancialEvents.EventTimestamp),
      DAY
    ) AS SettlementLagDays
FROM {{ source('anchorage_data_platform', 'FinancialEvents') }} AS FinancialEvents
