

select
    account_id,
    customer_id,
    account_type,
    currency,
    opened_date::date as opened_date
from "airflow"."staging"."accounts"
where account_id is not null