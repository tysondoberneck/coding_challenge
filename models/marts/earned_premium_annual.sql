
with monthly_earned as (
    select
        policy_number,
        month_start,
        earned_premium
    from {{ ref('int_policy_monthly_earned_premium') }}
),

annual_earned as (
    select
        policy_number,
        to_char(month_start, 'YYYY') as earn_year,
        sum(earned_premium) as total_earned_premium
    from monthly_earned
    group by
        policy_number,
        to_char(month_start, 'YYYY')
)

select
    policy_number,
    earn_year,
    round(total_earned_premium, 2) as earned_premium
from annual_earned
