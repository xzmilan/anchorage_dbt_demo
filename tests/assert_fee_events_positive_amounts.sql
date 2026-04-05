-- TEST: assert_fee_events_positive_amounts
-- Billing compliance: all fee amounts, basis points, and AUM must be positive.
-- A negative or zero fee could indicate:
--   - An unmodeled fee credit/waiver (should be its own event type)
--   - A billing system error
--   - A sign convention problem in the source
-- Under SOX, fee events must be verifiable, non-negative, and traceable.
-- Zero rows = PASS

select
    fee_id,
    customer_id,
    asset_id,
    fee_type,
    basis_points,
    fee_amount_usd,
    aum_at_billing,
    fee_period_start,
    case
        when fee_amount_usd <= 0 then 'ZERO_OR_NEGATIVE_FEE'
        when basis_points   <= 0 then 'INVALID_BASIS_POINTS'
        when aum_at_billing <= 0 then 'ZERO_OR_NEGATIVE_AUM'
    end as failure_reason
from {{ ref('stg_fee_events') }}
where fee_amount_usd <= 0
   or basis_points   <= 0
   or aum_at_billing <= 0
