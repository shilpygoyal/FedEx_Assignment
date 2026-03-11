{{
  config(
    materialized = 'table',
    )
}}

/*
Model: fact_sales

Description:
  Central fact table containing one row per order line item, representing
  individual sales transactions. Joins to all five dimension tables.

Grain:
  One row per unique (order_id, sku) combination — i.e. one row per
  product line within an order.

Source:
  Derived from int_sales (all measures and degenerate dimensions) joined
  to five dimension tables: dim_date, dim_product, dim_location,
  dim_channel, dim_order_status.

Primary Key:
  fact_sales_sk — MD5 surrogate key derived from order_id and sku.

Foreign Keys:
  date_sk           → dim_date.date_sk
  product_sk        → dim_product.product_sk
  location_sk       → dim_location.location_sk
  channel_sk        → dim_channel.channel_sk
  order_status_sk   → dim_order_status.order_status_sk

Columns:
  -- Surrogate Key
  fact_sales_sk       Surrogate primary key. MD5 hash of order_id and sku.

  -- Foreign Keys
  date_sk             Links to dim_date for calendar-based analysis.
  product_sk          Links to dim_product for product attribute filtering.
  location_sk         Links to dim_location for geographic analysis.
  channel_sk          Links to dim_channel for fulfilment and channel analysis.
  order_status_sk     Links to dim_order_status for lifecycle state analysis.

  -- Degenerate Dimensions (attributes with no dedicated dimension table)
  order_id            Original order identifier from the source system.
                      Not unique at fact grain — one order may have
                      multiple SKUs and therefore multiple rows.
  promotion_id        Promotion or discount code applied to the order, if any.
                      Defaults to 'unknown' where NULL in source.
  b2b                 Boolean flag. TRUE if the order was placed by a
                      business customer (B2B), FALSE for consumer orders.
  currency            Currency of the transaction. Defaults to 'INR' where
                      NULL in source. All transactions in this dataset are INR.

  -- Measures
  order_value         Derived measure: quantity * amount. Represents the
                      total monetary value of the order line.
                      Guaranteed non-negative — both inputs are cleaned
                      in int_sales (ABS(amount), quantity >= 0).
  quantity            Number of units ordered for this line item.
                      Guaranteed >= 0. Source negatives clamped to 0
                      in int_sales.
  amount              Unit price of the product at time of order.
                      Guaranteed >= 0 via ABS() applied in int_sales.

Usage Notes:
  - All dimension joins are LEFT JOINs. A NULL foreign key in any dimension
    indicates a data quality gap in the source.
*/

WITH sales as (
    SELECT
        *
    FROM
        {{ ref('int_sales') }}
),
product_dim as (
    SELECT
        * 
    FROM
        {{ ref('dim_product') }}
),
 location_dim as (
    SELECT
        * 
    FROM
        {{ ref('dim_location') }}
),
 date_dim as (
    SELECT
        * 
    FROM
        {{ ref('dim_date') }}
),
 channel_dim as (
    SELECT
        * 
    FROM
        {{ ref('dim_channel') }}
),
 order_status_dim as (
    SELECT
        * 
    FROM
        {{ ref('dim_order_status') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['s.order_id','s.sku']) }} AS fact_sales_sk,

    -- foreign keys
    d.date_sk,
    p.product_sk,
    l.location_sk,
    c.channel_sk,
    os.order_status_sk,

    -- degenerate dimensions
    s.order_id,
    s.promotion_id,
    s.b2b,
    s.currency,

    -- measures
    s.order_value,
    s.quantity,
    s.amount

FROM sales s

LEFT JOIN date_dim d
ON s.order_date = d.order_date

LEFT JOIN product_dim p
    ON s.sku = p.sku
    AND s.style = p.style
    AND s.size = p.size

LEFT JOIN channel_dim c
    ON s.sales_channel = c.sales_channel
    AND s.fulfilment = c.fulfilment
    AND s.ship_service_level = c.ship_service_level

LEFT JOIN location_dim l
    ON l.ship_city = s.ship_city
    AND l.ship_state = s.ship_state
    AND l.ship_postal_code = s.ship_postal_code

LEFT JOIN order_status_dim os
    ON s.order_status = os.order_status
AND COALESCE(s.courier_status,'unknown') =
    COALESCE(os.courier_status,'unknown')
AND s.is_cancelled = os.is_cancelled