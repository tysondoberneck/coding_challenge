
with monthly_earned as (
    select
        policy_number,
        month_start,
        earned_premium
    from {{ ref('int_policy_monthly_earned_premium') }}
)

select
    policy_number,
    to_char(month_start, 'YYYYMM') as earn_month,
    earned_premium
from monthly_earned
