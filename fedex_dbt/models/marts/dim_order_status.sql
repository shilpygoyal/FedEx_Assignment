{{
  config(
    materialized = 'table',
    )
}}

With order_status_source as (
    SELECT 
        order_status,
        courier_status,
        is_cancelled
    FROM 
    {{ ref('int_sales') }}
),
deduplicate_records as (
  SELECT 
    order_status,
    courier_status,
    is_cancelled,
    ROW_NUMBER() OVER(PARTITION BY order_status,courier_status,is_cancelled ORDER BY order_status) as rn
  FROM order_status_source
)

SELECT 
    {{dbt_utils.generate_surrogate_key(['order_status','courier_status','is_cancelled'])}} as order_status_sk,
    order_status,
    courier_status,
    is_cancelled
FROM
  deduplicate_records 
WHERE 
  rn = 1