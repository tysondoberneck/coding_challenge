{{ config(materialized = 'table') }}

-- Generates one row per month from Jan‑2020 through Dec‑2021
-- We could add it as a cte, but I think its cleaner as its own table and reusable if needed anywhere else in the future
select
    dateadd(month, seq4(), date '2020-01-01')   as month_start,
    last_day(dateadd(month, seq4(), date '2020-01-01')) as month_end
from table(generator(rowcount => 24))
