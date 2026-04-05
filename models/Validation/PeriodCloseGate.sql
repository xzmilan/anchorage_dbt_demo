-- VALIDATION: PeriodCloseGate
--
-- Purpose: Single-table pass/fail gate for all period-end close checks.
--          This model is the LAST thing the validate_period_close.py script
--          queries before the delivery engine is allowed to run.
--
-- Design: Each check is a UNION ALL row. Zero failing counts = green.
--         The Python gate script reads this model and fails (exit 1)
--         if ANY row has Status = 'FAIL'. Nothing ships until this is all green.
--
-- Checks in this model:
--   1. reconciliation_clean      — No open recon exceptions in ReconciliationSummary
--   2. ledger_tieout_clean       — No VARIANCE or UNCONFIRMED rows in LedgerBalanceTieOut
--   3. no_pending_events         — All financial events settled (no IsPending = TRUE)
--   4. all_fees_billed           — All fee accruals have corresponding invoice references
--   5. no_missing_positions      — No client/asset pairs with events but no position
--   6. kyc_approved_only         — No PENDING/SUSPENDED clients in statement output
--
-- Ownership: Reporting & Statements team writes and maintains this model.
--            Platform team owns the upstream tables being checked against.
--            This model is the contract boundary — if it's green, we ship.

-- Check 1: No open reconciliation exceptions
SELECT
  'reconciliation_clean' AS CheckName
  , 'All ReconciliationSummary rows must be Reconciled before statements ship' AS Description
  , COUNTIF(ReconciliationSummary.ReconciliationStatus != 'Reconciled') AS FailingCount
  , CASE
      WHEN COUNTIF(ReconciliationSummary.ReconciliationStatus != 'Reconciled') = 0
        THEN 'PASS'
      ELSE 'FAIL'
    END AS Status
  , IFNULL(
      STRING_AGG(
        CASE
          WHEN ReconciliationSummary.ReconciliationStatus != 'Reconciled'
            THEN CONCAT(
              ReconciliationSummary.CustomerName, ' / ',
              ReconciliationSummary.Symbol, ' / ',
              CAST(ReconciliationSummary.PositionPeriod AS STRING),
              ': ', ReconciliationSummary.ReconciliationStatus
            )
        END,
        ' | '
        LIMIT 5
      ),
      'None'
    ) AS SampleExceptions
FROM {{ ref('ReconciliationSummary') }} AS ReconciliationSummary

UNION ALL

-- Check 2: Ledger tie-out is clean — no variances, no unconfirmed positions
SELECT
  'ledger_tieout_clean' AS CheckName
  , 'All LedgerBalanceTieOut rows must be MATCHED before statements ship' AS Description
  , COUNTIF(LedgerBalanceTieOut.LedgerStatus != 'MATCHED') AS FailingCount
  , CASE
      WHEN COUNTIF(LedgerBalanceTieOut.LedgerStatus != 'MATCHED') = 0 THEN 'PASS'
      ELSE 'FAIL'
    END AS Status
  , IFNULL(
      STRING_AGG(
        CASE
          WHEN LedgerBalanceTieOut.LedgerStatus != 'MATCHED'
            THEN CONCAT(
              LedgerBalanceTieOut.CustomerId, ' / ',
              LedgerBalanceTieOut.AssetId, ' / ',
              CAST(LedgerBalanceTieOut.PositionPeriod AS STRING),
              ': ', LedgerBalanceTieOut.LedgerStatus,
              COALESCE(CONCAT(' — ', LedgerBalanceTieOut.ExceptionDetail), '')
            )
        END,
        ' | '
        LIMIT 5
      ),
      'None'
    ) AS SampleExceptions
FROM {{ ref('LedgerBalanceTieOut') }} AS LedgerBalanceTieOut

UNION ALL

-- Check 3: No unsettled financial events remain open
SELECT
  'no_pending_events' AS CheckName
  , 'All FinancialEvents must be settled (IsPending = FALSE) before month-end close' AS Description
  , COUNTIF(FinancialEventsCalc.IsPending) AS FailingCount
  , CASE
      WHEN COUNTIF(FinancialEventsCalc.IsPending) = 0 THEN 'PASS'
      ELSE 'FAIL'
    END AS Status
  , IFNULL(
      STRING_AGG(
        CASE
          WHEN FinancialEventsCalc.IsPending
            THEN CONCAT(
              CAST(FinancialEvents.CustomerId AS STRING), ' / ',
              FinancialEvents.AssetId, ' / ',
              CAST(FinancialEventsCalc.EventPeriod AS STRING)
            )
        END,
        ' | '
        LIMIT 5
      ),
      'None'
    ) AS SampleExceptions
FROM {{ source('anchorage_data_platform', 'FinancialEvents') }} AS FinancialEvents
JOIN {{ source('anchorage_calc', 'FinancialEventsCalc') }} AS FinancialEventsCalc
  ON FinancialEventsCalc.ID = FinancialEvents.ID

UNION ALL

-- Check 4: All fee accruals have been billed (no stranded unbilled fees)
SELECT
  'all_fees_billed' AS CheckName
  , 'All FeeEvents must be fully billed before month-end close' AS Description
  , COUNTIF(NOT FeeEventsCalc.IsFullyBilled) AS FailingCount
  , CASE
      WHEN COUNTIF(NOT FeeEventsCalc.IsFullyBilled) = 0 THEN 'PASS'
      ELSE 'FAIL'
    END AS Status
  , IFNULL(
      STRING_AGG(
        CASE
          WHEN NOT FeeEventsCalc.IsFullyBilled
            THEN CONCAT(
              CAST(FeeEvents.CustomerId AS STRING), ' / ',
              FeeEvents.AssetId, ' / ',
              FeeEvents.FeeType, ' / ',
              CAST(FeeEventsCalc.FeePeriod AS STRING)
            )
        END,
        ' | '
        LIMIT 5
      ),
      'None'
    ) AS SampleExceptions
FROM {{ source('anchorage_data_platform', 'FeeEvents') }} AS FeeEvents
JOIN {{ source('anchorage_calc', 'FeeEventsCalc') }} AS FeeEventsCalc
  ON FeeEventsCalc.ID = FeeEvents.ID

UNION ALL

-- Check 5: No clients with active events but no position record
-- (would result in a statement row missing position data)
SELECT
  'no_missing_positions' AS CheckName
  , 'Every client/asset with activity must have a corresponding position snapshot' AS Description
  , COUNTIF(ReconciliationSummary.ReconciliationStatus = 'MissingPosition') AS FailingCount
  , CASE
      WHEN COUNTIF(ReconciliationSummary.ReconciliationStatus = 'MissingPosition') = 0
        THEN 'PASS'
      ELSE 'FAIL'
    END AS Status
  , IFNULL(
      STRING_AGG(
        CASE
          WHEN ReconciliationSummary.ReconciliationStatus = 'MissingPosition'
            THEN CONCAT(
              ReconciliationSummary.CustomerName, ' / ',
              ReconciliationSummary.Symbol, ' / ',
              CAST(ReconciliationSummary.PositionPeriod AS STRING)
            )
        END,
        ' | '
        LIMIT 5
      ),
      'None'
    ) AS SampleExceptions
FROM {{ ref('ReconciliationSummary') }} AS ReconciliationSummary

UNION ALL

-- Check 6: No non-APPROVED clients appear in statement output
SELECT
  'kyc_approved_only' AS CheckName
  , 'Only KYC-approved clients (KycStatus = APPROVED) may receive statements' AS Description
  , COUNTIF(Customers.KycStatus != 'APPROVED') AS FailingCount
  , CASE
      WHEN COUNTIF(Customers.KycStatus != 'APPROVED') = 0 THEN 'PASS'
      ELSE 'FAIL'
    END AS Status
  , IFNULL(
      STRING_AGG(
        CASE
          WHEN Customers.KycStatus != 'APPROVED'
            THEN CONCAT(
              Customers.CustomerName,
              ' (KycStatus=', Customers.KycStatus, ')'
            )
        END,
        ' | '
        LIMIT 5
      ),
      'None'
    ) AS SampleExceptions
FROM {{ source('anchorage_data_platform', 'Customers') }} AS Customers
-- Only flag clients who actually have positions this period
WHERE Customers.IsActive = TRUE
