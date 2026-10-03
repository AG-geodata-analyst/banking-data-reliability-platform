
  
    

  create  table "airflow"."analytics"."fact_transactions__dbt_tmp"
  
  
    as
  
  (
    

select
    t.transaction_id,
    t.account_id,
    a.customer_id,
    a.customer_country,
    a.account_type,
    t.transaction_date::date       as transaction_date,
    date_trunc('day', t.transaction_date)::date as transaction_day,
    extract(hour from t.transaction_date)       as transaction_hour,
    t.amount,
    t.currency,
    t.status,
    t.merchant
from "airflow"."staging"."stg_transactions" t
left join "airflow"."analytics"."dim_account" a using (account_id)
  );
  