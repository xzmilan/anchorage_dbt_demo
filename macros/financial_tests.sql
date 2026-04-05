-- Financial compliance macros
-- Reusable test patterns for institutional reporting.

-- ── Generic test: no financial amount should be in scientific notation ──────
-- Scientific notation in a financial field (e.g. 1.5e-08) indicates either:
--   - A precision loss during type conversion
--   - An extremely small amount that should be treated as zero
-- Either case is a data quality failure on a client statement.
{% test no_scientific_notation(model, column_name) %}

select
    {{ column_name }},
    'SCIENTIFIC_NOTATION_IN_AMOUNT' as failure_reason
from {{ model }}
where cast({{ column_name }} as varchar) ilike '%e%'
  and {{ column_name }} is not null

{% endtest %}


-- ── Generic test: fee_take_rate should be within expected band ───────────────
-- Anchorage contracts specify fee rates per client tier.
-- fee_take_rate_pct outside [min_pct, max_pct] indicates a billing anomaly.
{% test fee_take_rate_in_bounds(model, column_name, min_pct=5, max_pct=15) %}

select
    customer_id,
    asset_id,
    reward_period,
    {{ column_name }} as fee_take_rate_pct,
    'FEE_RATE_OUT_OF_BOUNDS' as failure_reason
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < {{ min_pct }}
    or {{ column_name }} > {{ max_pct }})

{% endtest %}


-- ── Generic test: period-end reconciliation clean ────────────────────────────
-- All records in rpt_reconciliation_summary must be RECONCILED.
-- Use this as the period-close gate before any delivery runs.
{% test all_records_reconciled(model, column_name='reconciliation_status') %}

select
    customer_id,
    symbol,
    period_date,
    {{ column_name }},
    'NOT_RECONCILED_AT_PERIOD_CLOSE' as failure_reason
from {{ model }}
where {{ column_name }} != 'RECONCILED'

{% endtest %}
