
select
    policy_number,
    to_date(effective_date) as effective_date,
    to_date(expiration_date) as expiration_date,
    cast(written_premium as float) as written_premium
from {{ ref('policy_data') }}
