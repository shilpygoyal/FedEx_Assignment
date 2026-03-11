{{
  config(
    materialized = 'table',
    )
}}

/*
Model: dim_channel

Description:
  Channel dimension containing one row per unique combination of sales
  channel, fulfilment type, and shipping service level. Enables analysis
  of order volume, revenue, and performance across different sales and
  fulfilment routes.

Grain:
  One row per unique (sales_channel, fulfilment, ship_service_level)
  combination.

Source:
  Derived from int_sales. All string values are lowercased for consistency.

Primary Key:
  channel_sk — MD5 surrogate key derived from sales_channel, fulfilment,
  and ship_service_level.

Deduplication:
  ROW_NUMBER() partitions on (sales_channel, fulfilment, ship_service_level)
  to collapse repeated combinations across orders.

Columns:
  channel_sk          Surrogate primary key. MD5 hash of sales_channel,
                      fulfilment, and ship_service_level.
  sales_channel       The platform through which the order was placed
                      (e.g. 'fashionable.in', 'non-fashionable').
  fulfilment          Party responsible for fulfilment
                      (e.g. 'merchant', 'fashionable').
  fulfilled_by        Operational fulfilment method
                      (e.g. 'easy ship', 'self-ship').
  ship_service_level  Shipping speed tier selected by the customer
                      (e.g. 'standard', 'expedited').

Usage Notes:
  - Join to fact_sales on channel_sk.
*/

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