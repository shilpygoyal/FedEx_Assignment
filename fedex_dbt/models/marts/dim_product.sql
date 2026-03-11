{{
  config(
    materialized = 'table',
    )
}}
With product_source as (
SELECT 
    sku,
    asin,
    style,
    category,
    size
FROM 
   {{ ref('int_sales') }}
),
deduplicate_records as (
  SELECT 
    sku,
    asin,
    style,
    category,
    size,
    ROW_NUMBER() OVER(PARTITION BY sku,style,size ORDER BY sku) as rn
  FROM product_source
)
SELECT 
  {{ dbt_utils.generate_surrogate_key(['sku','style'])}} as product_sk,
  sku,
  style,
  asin,
  category,
  size
FROM
  deduplicate_records 
WHERE 
  rn = 1