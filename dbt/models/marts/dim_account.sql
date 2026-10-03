{{ config(materialized='table') }}

select
    a.account_id,
    a.customer_id,
    c.full_name as customer_name,
    c.country as customer_country,
    a.account_type,
    a.currency,
    a.opened_date
from {{ ref('stg_accounts') }} a
left join {{ ref('stg_customers') }} c using (customer_id)
