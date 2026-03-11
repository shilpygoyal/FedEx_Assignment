{{
  config(
    materialized = 'table',
    )
}}

With channel_source as (
SELECT 
    sales_channel,
    fulfilment,
    fulfilled_by,
    ship_service_level
FROM 
   {{ ref('int_sales') }}
),
deduplicate_records as (
  SELECT 
    sales_channel,
    fulfilment,
    fulfilled_by,
    ship_service_level,
    ROW_NUMBER() OVER(PARTITION BY sales_channel,fulfilment,ship_service_level ORDER BY sales_channel) as rn
  FROM 
    channel_source
)

SELECT 
    {{dbt_utils.generate_surrogate_key(['sales_channel','fulfilment','ship_service_level'])}} as channel_sk,
    sales_channel,
    fulfilment,
    fulfilled_by,
    ship_service_level
FROM
  deduplicate_records 
WHERE 
  rn = 1