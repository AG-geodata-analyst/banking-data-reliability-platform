
  create view "airflow"."staging"."stg_transactions__dbt_tmp"
    
    
  as (
    

select
    transaction_id,
    account_id,
    transaction_date,
    amount::numeric(18,2) as amount,
    currency,
    status,
    merchant
from "airflow"."staging"."transactions"
where transaction_id is not null
  and amount::numeric > 0
  );