{{
  config(
    materialized = 'table',
    )
}}

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