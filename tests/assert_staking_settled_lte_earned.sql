-- TEST: assert_staking_settled_lte_earned
-- Critical integrity check: settled rewards can never exceed total earned rewards.
-- settled_net_rewards > total_net_rewards_all_states means we distributed more
-- than was earned — a critical data integrity and regulatory failure.
-- net_pending_rewards < 0 means pending rewards went negative — impossible.
-- Zero rows = PASS

select
    customer_id,
    asset_id,
    reward_period,
    settled_net_rewards,
    total_net_rewards_all_states,
    net_pending_rewards,
    case
        when settled_net_rewards > total_net_rewards_all_states
        then 'SETTLED_EXCEEDS_EARNED'
        when net_pending_rewards < 0
        then 'NEGATIVE_PENDING_REWARDS'
    end as failure_reason
from {{ ref('calc_staking_rewards_by_period') }}
where settled_net_rewards    > total_net_rewards_all_states
   or net_pending_rewards     < 0
