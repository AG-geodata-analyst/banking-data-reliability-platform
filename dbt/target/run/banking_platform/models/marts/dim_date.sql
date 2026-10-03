
  
    

  create  table "airflow"."analytics"."dim_date__dbt_tmp"
  
  
    as
  
  (
    

with date_spine as (
    select
        generate_series(
            '2024-01-01'::date,
            '2027-12-31'::date,
            '1 day'::interval
        )::date as date_day
)

select
    date_day,
    extract(year from date_day)     as year,
    extract(month from date_day)    as month,
    extract(day from date_day)      as day,
    extract(dow from date_day)      as day_of_week,
    to_char(date_day, 'Day')        as day_name,
    to_char(date_day, 'Month')      as month_name,
    extract(quarter from date_day)  as quarter
from date_spine
  );
  