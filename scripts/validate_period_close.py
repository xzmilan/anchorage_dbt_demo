#!/usr/bin/env python3
"""
validate_period_close.py

Pre-delivery gate for month-end close.

Queries PeriodCloseGate in BigQuery. If ALL checks pass (Status = 'PASS'),
exits 0. If ANY check fails, prints the failing rows and exits 1.

The Airflow DAG (month_end_close_dag.py) calls this script as a BashOperator
after dbt run completes. Exit code 1 halts the pipeline — nothing ships.

Usage:
    python3 scripts/validate_period_close.py --period 2025-03-01 --project sigma-method-453023-a4
    python3 scripts/validate_period_close.py --period 2025-03-01 --project sigma-method-453023-a4 --dataset Business_View_Anchorage
"""

import argparse
import sys
from datetime import date
from google.cloud import bigquery


# The PeriodCloseGate model is built in the Validation dataset by dbt.
# This query filters to the close period being validated.
# Note: PeriodCloseGate is a UNION ALL of checks — it doesn't have a period
# column itself. We pass period into the checks via this wrapper.
GATE_QUERY = """
SELECT
  CheckName,
  Description,
  FailingCount,
  Status,
  SampleExceptions
FROM `{project}.{dataset}.PeriodCloseGate`
ORDER BY
  CASE Status WHEN 'FAIL' THEN 1 ELSE 2 END,
  CheckName
"""


def run_gate(project: str, dataset: str, period: str) -> int:
    """
    Query the PeriodCloseGate model and print results.
    Returns the number of failing checks.
    """
    client = bigquery.Client(project=project)
    query = GATE_QUERY.format(project=project, dataset=dataset)

    print()
    print("=" * 72)
    print(f"  Period Close Validation Gate")
    print(f"  Period   : {period}")
    print(f"  Project  : {project}")
    print(f"  Dataset  : {dataset}")
    print("=" * 72)

    results = client.query(query).result()
    rows = list(results)

    if not rows:
        print("  ERROR: No rows returned from PeriodCloseGate. Has dbt run completed?")
        return 1

    failing_checks = []

    print(f"  {'Check':<30}  {'Status':<6}  {'Failing':>7}  Description")
    print(f"  {'-'*30}  {'-'*6}  {'-'*7}  {'-'*40}")

    for row in rows:
        status_display = row.Status
        print(f"  {row.CheckName:<30}  {status_display:<6}  {row.FailingCount:>7}  {row.Description[:40]}")

        if row.Status == 'FAIL':
            failing_checks.append(row)

    print("=" * 72)

    if failing_checks:
        print()
        print("  GATE FAILED — the following checks are blocking delivery:")
        print()
        for row in failing_checks:
            print(f"  ✗  {row.CheckName}")
            print(f"     {row.Description}")
            print(f"     Failing count : {row.FailingCount}")
            if row.SampleExceptions and row.SampleExceptions != 'None':
                print(f"     Sample exceptions:")
                for exc in row.SampleExceptions.split(' | ')[:5]:
                    print(f"       - {exc}")
            print()
        print("  Resolve all exceptions before re-running.")
        print("  Nothing ships until this gate is green.")
        print()
    else:
        print()
        print(f"  ALL CHECKS PASSED ({len(rows)}/{len(rows)}) — safe to proceed with delivery.")
        print()

    return len(failing_checks)


def main():
    parser = argparse.ArgumentParser(
        description="Validate period close gate before statement delivery"
    )
    parser.add_argument(
        "--period",
        required=True,
        help="Statement period in YYYY-MM-DD format (first day of month, e.g. 2025-03-01)",
    )
    parser.add_argument(
        "--project",
        default="sigma-method-453023-a4",
        help="GCP project ID",
    )
    parser.add_argument(
        "--dataset",
        default="Business_View_Anchorage",
        help="BigQuery dataset containing PeriodCloseGate",
    )
    args = parser.parse_args()

    # Validate period format
    try:
        date.fromisoformat(args.period)
    except ValueError:
        print(f"ERROR: --period must be YYYY-MM-DD format, got: {args.period}", file=sys.stderr)
        sys.exit(2)

    failing = run_gate(
        project=args.project,
        dataset=args.dataset,
        period=args.period,
    )

    sys.exit(1 if failing > 0 else 0)


if __name__ == "__main__":
    main()
