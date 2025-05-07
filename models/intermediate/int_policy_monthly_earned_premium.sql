
-- Calculate the length of each policy period to use later
with base_policy as (
    select
        p.*,
        datediff(day, p.effective_date, p.expiration_date) as policy_days
    from {{ ref('base_policy') }} p
),

months as (
    select month_start, month_end
    from {{ ref('months') }}
),

-- Break down each policy into months indicating the coverage within each month
policy_month_segments as (
    select
        f.policy_number,
        f.written_premium,
        f.policy_days,

        m.month_start,
        m.month_end,

        greatest(f.effective_date, m.month_start) as in_force_start, -- Start date of policy within month
        least(f.expiration_date, m.month_end) as in_force_end, -- End date of policy within month
        datediff(
            day,
            greatest(f.effective_date, m.month_start),
            least(f.expiration_date, m.month_end)
        ) + 1 as earned_days -- Days policy is active within the month. Add +1 to fix datediff excluding start/end

    from base_policy f
    inner join months m
      on m.month_start <= f.expiration_date --The start of the month is on or before the end of the policy
     and m.month_end >= f.effective_date --The end of the month is on or after the start of the policy
      where year(month_start) = 2020 -- limit to 2020 based on instruction
),

-- running total sum of days each policy is active by month
policy_cumulative_days as (
    select
        *,
        sum(earned_days)
            over (partition by policy_number
                  order by month_start) as cumulative_days_lapsed -- use partition logic to get running total of days lapsed since policy began. For PN0000 we see 17, 47, 78, 109 cont...
    from policy_month_segments
),

-- Calculate total earned premium for each policy-month and round it
total as (
    select
        *,
        round(
            written_premium * cumulative_days_lapsed / policy_days, 2 --ex: PN000 wp = 1200 * cdl = 17 for 05/2020, rounded = 55.89. This also fixes our penny issue with October
        ) as total_earned_rounded
    from policy_cumulative_days
),

-- Determine the monthly earned premium by subtracting last month total premium from current
final as (
    select
        policy_number,
        month_start,
        coalesce(
            total_earned_rounded - lag(total_earned_rounded)
              over (partition by policy_number order by month_start),
            total_earned_rounded
        ) as earned_premium
    from total
)

select *
from final
order by policy_number, month_start
