{{ config(materialized='view') }}

select
    account_id,
    customer_id,
    account_type,
    currency,
    opened_date::date as opened_date
from {{ source('staging', 'accounts') }}
where account_id is not null
