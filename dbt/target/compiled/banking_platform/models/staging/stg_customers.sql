

select
    customer_id,
    full_name,
    email,
    country,
    signup_date::date as signup_date
from "airflow"."staging"."customers"
where customer_id is not null