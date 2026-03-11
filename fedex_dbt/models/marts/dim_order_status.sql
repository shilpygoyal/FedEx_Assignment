{{
  config(
    materialized = 'table',
    )
}}

/*
Model: dim_order_status

Description:
  Order status dimension containing one row per unique combination of
  order status, courier status, and cancellation flag.

Grain:
  One row per unique (order_status, courier_status, is_cancelled)
  combination.

Source:
  Derived from int_sales, which applies the clean_order_status() macro to
  normalise raw order status values into standardised categories, and
  infers courier_status for cancelled orders where it was originally NULL.

Primary Key:
  order_status_sk — MD5 surrogate key derived from order_status,
  courier_status, and is_cancelled.

Deduplication:
  ROW_NUMBER() partitions on (order_status, courier_status, is_cancelled)
  to collapse repeated status combinations across orders.

Columns:
  order_status_sk   Surrogate primary key. MD5 hash of order_status,
                    courier_status, and is_cancelled.
  order_status      Cleaned, standardised order lifecycle status.
                    Possible values: pending, shipped, delivered, returned,
                    exception, cancelled, unknown.
                    Derived by clean_order_status() macro in int_sales.
  courier_status    Raw courier tracking status from the logistics provider
                    (e.g. 'shipped', 'delivered to buyer', 'cancelled').
                    NULL values for cancelled orders are imputed as
                    'cancelled' in int_sales.
  is_cancelled      Boolean flag. TRUE if the order was cancelled.
                    Derived from order_status = 'cancelled' in int_sales.

Usage Notes:
  - Join to fact_sales on order_status_sk using a three-part join condition:
      order_status + COALESCE(courier_status, 'unknown') + is_cancelled.
    The COALESCE guard is necessary because courier_status can be NULL
    for some non-cancelled statuses.
*/

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