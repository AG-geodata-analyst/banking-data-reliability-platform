

select
    a.account_id,
    a.customer_id,
    c.full_name as customer_name,
    c.country as customer_country,
    a.account_type,
    a.currency,
    a.opened_date
from "airflow"."staging"."stg_accounts" a
left join "airflow"."staging"."stg_customers" c using (customer_id)