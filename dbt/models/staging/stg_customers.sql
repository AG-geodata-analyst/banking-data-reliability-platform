{{ config(materialized='view') }}

select
    customer_id,
    full_name,
    email,
    country,
    signup_date::date as signup_date
from {{ source('staging', 'customers') }}
where customer_id is not null
