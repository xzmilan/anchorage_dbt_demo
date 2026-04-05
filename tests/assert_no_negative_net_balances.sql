-- TEST: assert_no_negative_net_balances
-- Financial compliance: no position can have a negative quantity or valuation.
-- A negative result indicates either a sign error in the source system
-- or an unmodeled short position — both require investigation before delivery.
-- Zero rows = PASS (no negative balances found)

select
    position_id,
    customer_id,
    asset_id,
    quantity,
    price_usd,
    valuation_usd,
    position_date,
    'NEGATIVE_BALANCE' as failure_reason
from {{ ref('stg_custody_positions') }}
where valuation_usd < 0
   or quantity    < 0
   or price_usd   < 0
