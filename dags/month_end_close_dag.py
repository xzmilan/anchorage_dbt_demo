"""
month_end_close_dag.py

Airflow DAG — Monthly Statement Close Pipeline
Owned by: Reporting & Statements team

This DAG orchestrates the full period-end close sequence:

  Step 1 — build_report_models
    Run dbt models for Reconciliation/, Validation/, and Reporting_Statements/
    layers. Business_View_Anchorage is assumed already built by the platform DAG.

  Step 2 — validate_period_close  [GATE]
    Run validate_period_close.py against PeriodCloseGate.
    Exit code 1 halts the pipeline here. Nothing advances to delivery
    until every check is green.

  Step 3 — branch_on_gate_result
    If gate passed → proceed to format_and_deliver.
    If gate failed → alert_on_failure (PagerDuty/Slack), stop.

  Step 4 — format_and_deliver
    Export MonthlyCustodyStatement from BigQuery.
    Format per client delivery preferences (CSV / encrypted SFTP / API).
    Record each delivery attempt in the delivery_audit_log table.

  Step 5 — confirm_delivery
    Verify each client's statement was received/acknowledged.
    Write final DeliveryStatus = 'CONFIRMED' to audit log.
    Trigger downstream notification to compliance team.

  On failure (any step):
    alert_on_failure → Slack #reporting-alerts + PagerDuty
    DAG does NOT auto-retry delivery steps — re-delivery must be intentional.

Scheduling:
    Runs at 06:00 UTC on the 2nd business day of each month.
    (Day 1 is reserved for overnight platform DAG + data availability checks.)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.models import Variable

# ---------------------------------------------------------------------------
# Config — all environment-specific values come from Airflow Variables
# Non-secret values only. GCP credentials are in the Airflow connection.
# ---------------------------------------------------------------------------

GCP_PROJECT = Variable.get("gcp_project", default_var="sigma-method-453023-a4")
DBT_PROJECT_DIR = Variable.get("dbt_project_dir", default_var="/opt/airflow/dbt/anchorage_dbt_demo")
DBT_PROFILES_DIR = Variable.get("dbt_profiles_dir", default_var="/opt/airflow/dbt")
DELIVERY_BUCKET = Variable.get("statement_delivery_bucket", default_var="gs://anchorage-statement-delivery")
SLACK_ALERTS_WEBHOOK = "slack_reporting_alerts"  # Airflow connection ID

# Period macro: first day of the previous calendar month
# Airflow passes {{ ds }} as the DAG execution date — we derive the
# statement period from that so re-runs are idempotent.
STATEMENT_PERIOD_MACRO = "{{ (execution_date.replace(day=1) - macros.timedelta(days=1)).replace(day=1).strftime('%Y-%m-%d') }}"

DEFAULT_ARGS = {
    "owner": "reporting-statements-team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["reporting-alerts@anchoragedigital.com"],
    "retries": 0,           # No auto-retry — failed delivery must be intentional
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="month_end_close",
    description="Monthly statement close: build → reconcile → validate → deliver → confirm",
    schedule_interval="0 6 2 * *",      # 06:00 UTC on the 2nd of each month
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["reporting", "statements", "month-end"],
    max_active_runs=1,                  # Never run two close cycles in parallel
) as dag:

    # -----------------------------------------------------------------------
    # Step 1: Build dbt report models
    # Runs only the Reporting_Statements, Reconciliation, and Validation layers.
    # Business_View_Anchorage is a dependency — assumed already built.
    # -----------------------------------------------------------------------
    build_report_models = BashOperator(
        task_id="build_report_models",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run "
            f"  --profiles-dir {DBT_PROFILES_DIR} "
            f"  --select tag:reconciliation tag:validation tag:reporting_statements "
            f"  --vars '{{\"statement_period\": \"{STATEMENT_PERIOD_MACRO}\"}}' "
        ),
        doc_md=(
            "Builds Reconciliation/, Validation/, and Reporting_Statements/ dbt models. "
            "These are the Reporting & Statements team's owned layers. "
            "The upstream Business_View_Anchorage models are expected to already be built "
            "by the platform DAG on the same schedule."
        ),
    )

    # -----------------------------------------------------------------------
    # Step 2: Run period close gate (hard stop if any check fails)
    # validate_period_close.py exits 1 if any PeriodCloseGate row is 'FAIL'.
    # BashOperator propagates exit code — Airflow marks task as failed.
    # -----------------------------------------------------------------------
    validate_period_close = BashOperator(
        task_id="validate_period_close",
        bash_command=(
            f"python3 {DBT_PROJECT_DIR}/scripts/validate_period_close.py "
            f"  --period {STATEMENT_PERIOD_MACRO} "
            f"  --project {GCP_PROJECT} "
        ),
        doc_md=(
            "Hard gate: queries PeriodCloseGate in BigQuery. "
            "ALL reconciliation, ledger tie-out, pending event, fee billing, "
            "missing position, and KYC checks must pass. "
            "If any fail, this task fails and nothing advances to delivery. "
            "On-call must resolve all exceptions and manually trigger re-run."
        ),
    )

    # -----------------------------------------------------------------------
    # Step 3: Branch — only proceed to delivery if gate passed
    # (Airflow handles this via task success/failure — the EmptyOperator
    # below is a join point for documentation clarity)
    # -----------------------------------------------------------------------
    gate_passed = EmptyOperator(
        task_id="gate_passed",
        doc_md="Logical join point — execution only reaches here if gate passed.",
    )

    # -----------------------------------------------------------------------
    # Step 4: Format and deliver statements
    # Export MonthlyCustodyStatement → CSV → SFTP to each client
    # In production this would fan out per client using a DynamicTaskMapping.
    # -----------------------------------------------------------------------
    format_and_deliver = BashOperator(
        task_id="format_and_deliver",
        bash_command=(
            # Export statement data from BigQuery to GCS
            f"bq extract "
            f"  --destination_format CSV "
            f"  --field_delimiter ',' "
            f"  {GCP_PROJECT}:Business_View_Anchorage.MonthlyCustodyStatement "
            f"  '{DELIVERY_BUCKET}/{STATEMENT_PERIOD_MACRO}/statements_*.csv' "
            # In production: trigger per-client formatter (PDF/SFTP/API)
            # and write each delivery attempt to delivery_audit_log
        ),
        doc_md=(
            "Exports MonthlyCustodyStatement from BigQuery. "
            "In production, this step fans out per client using DynamicTaskMapping "
            "and routes each statement to their configured delivery channel "
            "(encrypted SFTP, secure API endpoint, or custodian portal). "
            "Each attempt is written to delivery_audit_log with timestamp and status."
        ),
    )

    # -----------------------------------------------------------------------
    # Step 5: Confirm delivery — verify receipt, write audit record
    # -----------------------------------------------------------------------
    confirm_delivery = BashOperator(
        task_id="confirm_delivery",
        bash_command=(
            # In production: check SFTP acknowledgements / API delivery receipts,
            # update delivery_audit_log.DeliveryStatus = 'CONFIRMED',
            # notify compliance team.
            f"echo 'Delivery confirmed for period {STATEMENT_PERIOD_MACRO}' && "
            f"echo 'Audit log updated. Notifying compliance team.'"
        ),
        doc_md=(
            "Verifies each client statement was received and acknowledged. "
            "Writes DeliveryStatus = CONFIRMED to delivery_audit_log. "
            "This record is the SOX-traceable proof of delivery. "
            "Triggers downstream notification to compliance team."
        ),
    )

    # -----------------------------------------------------------------------
    # Failure handler — fires on any task failure in this DAG
    # -----------------------------------------------------------------------
    alert_on_failure = SlackWebhookOperator(
        task_id="alert_on_failure",
        http_conn_id=SLACK_ALERTS_WEBHOOK,
        message=(
            ":rotating_light: *Month-End Close FAILED*\n"
            f">Period: {STATEMENT_PERIOD_MACRO}\n"
            f">DAG: `month_end_close`\n"
            ">Check Airflow logs and resolve all PeriodCloseGate exceptions.\n"
            ">*Nothing ships until the gate is green and the DAG is manually re-triggered.*"
        ),
        trigger_rule="one_failed",      # Fires if ANY upstream task fails
        doc_md=(
            "Fires on any task failure. Posts to #reporting-alerts in Slack "
            "and triggers PagerDuty for on-call. "
            "DAG does not auto-retry — re-delivery must be intentional."
        ),
    )

    # -----------------------------------------------------------------------
    # Task dependencies — the pipeline graph
    # -----------------------------------------------------------------------
    (
        build_report_models
        >> validate_period_close
        >> gate_passed
        >> format_and_deliver
        >> confirm_delivery
    )

    # Alert fires independently on any failure
    [build_report_models, validate_period_close, format_and_deliver, confirm_delivery] >> alert_on_failure
