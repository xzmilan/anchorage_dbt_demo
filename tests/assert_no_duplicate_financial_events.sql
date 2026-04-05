-- TEST: assert_no_duplicate_financial_events
-- SOX compliance: every financial event must be unique.
-- A duplicate event_id means the same transaction was recorded twice —
-- this would double-count amounts in every downstream report and statement.
-- Zero rows = PASS (all event_ids unique)

select
    event_id,
    count(*) as duplicate_count,
    'DUPLICATE_EVENT_ID' as failure_reason
from {{ ref('stg_financial_events') }}
group by event_id
having count(*) > 1
