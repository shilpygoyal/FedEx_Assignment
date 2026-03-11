{{
  config(
    materialized = 'table',
    )
}}

With date_source as (
    SELECT 
        order_date
    FROM 
    {{ ref('int_sales') }}
),
deduplicate_records as (
  SELECT 
    order_date,
    EXTRACT(year from order_date) as year,
    EXTRACT(month from order_date) as month,
    EXTRACT(day from order_date) as day,
    ROW_NUMBER() OVER(PARTITION BY order_date ORDER BY order_date) as rn
  FROM date_source
)

SELECT 
    {{dbt_utils.generate_surrogate_key(['order_date'])}} as date_sk,
    order_date,
    EXTRACT(year from order_date) as year,
    EXTRACT(month from order_date) as month,
    EXTRACT(day from order_date) as day
FROM
  deduplicate_records 
WHERE 
  rn = 1