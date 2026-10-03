

select
    customer_id,
    full_name,
    email,
    country,
    signup_date,
    date_part('year', signup_date) as signup_year
from "airflow"."staging"."stg_customers"