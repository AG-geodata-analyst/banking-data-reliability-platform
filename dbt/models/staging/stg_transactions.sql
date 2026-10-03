{{ config(materialized='view') }}

select
    transaction_id,
    account_id,
    transaction_date,
    amount::numeric(18,2) as amount,
    currency,
    status,
    merchant
from {{ source('staging', 'transactions') }}
where transaction_id is not null
  and amount::numeric > 0
