{{
  config(
    materialized = 'table',
    )
}}

/*
Model: dim_date

Description:
  Date dimension containing one row per unique order date found in the dataset.

Grain:
  One row per unique order_date.

Source:
  Derived from int_sales. Only dates that appear in the sales data are included.

Primary Key:
  date_sk — MD5 surrogate key derived from order_date.

Columns:
  date_sk          Surrogate primary key. MD5 hash of order_date.
  order_date       The calendar date of the order. Cast from raw string in int_sales.
  year             Calendar year extracted from order_date (e.g. 2022).
  month            Calendar month number (1–12).
  day              Day of month (1–31).

Usage Notes:
  - Join to fact_sales on date_sk.
  - All dates are based on order placement date, not shipment or delivery date.
*/
With date_source as (
    SELECT 
        order_date
    FROM 
    {{ ref('int_sales') }}
),
deduplicate_records as (
  SELECT 
    order_date,
    EXTRACT(year from order_date)    as year,
    EXTRACT(quarter from order_date) as quarter,
    EXTRACT(month from order_date)   as month,
    EXTRACT(week from order_date)    as week_of_year,
    EXTRACT(day from order_date)     as day,
    EXTRACT(dow from order_date)     as day_of_week,       -- 0=Sunday
    DAYNAME(order_date)              as day_name,           -- 'Monday'
    MONTHNAME(order_date)            as month_name,         -- 'April'
    CASE WHEN EXTRACT(dow from order_date) IN (0,6)
         THEN FALSE ELSE TRUE END    as is_weekday,
    CASE WHEN EXTRACT(month from order_date) IN (1,2,3)  THEN 'Q1'
         WHEN EXTRACT(month from order_date) IN (4,5,6)  THEN 'Q2'
         WHEN EXTRACT(month from order_date) IN (7,8,9)  THEN 'Q3'
         ELSE 'Q4' END              as quarter_label,
    ROW_NUMBER() OVER(PARTITION BY order_date ORDER BY order_date) as rn
  FROM date_source
)

SELECT 
    {{dbt_utils.generate_surrogate_key(['order_date'])}} as date_sk,
    order_date,
    EXTRACT(year from order_date)    as year,
    EXTRACT(quarter from order_date) as quarter,
    EXTRACT(month from order_date)   as month,
    EXTRACT(week from order_date)    as week_of_year,
    EXTRACT(day from order_date)     as day,
    EXTRACT(dow from order_date)     as day_of_week,       -- 0=Sunday
    DAYNAME(order_date)              as day_name,           -- 'Monday'
    MONTHNAME(order_date)            as month_name,         -- 'April'
    CASE WHEN EXTRACT(dow from order_date) IN (0,6)
         THEN FALSE ELSE TRUE END    as is_weekday,
    CASE WHEN EXTRACT(month from order_date) IN (1,2,3)  THEN 'Q1'
         WHEN EXTRACT(month from order_date) IN (4,5,6)  THEN 'Q2'
         WHEN EXTRACT(month from order_date) IN (7,8,9)  THEN 'Q3'
         ELSE 'Q4' END              as quarter_label
FROM
  deduplicate_records 
WHERE 
  rn = 1