{{
  config(
    materialized = 'view',
    )
}}

WITH source as (
    SELECT 
        *
    FROM 
        {{ ref('stg_sales') }}
),
state_map as (
    SELECT 
        * 
    FROM 
        {{ ref('india_state_mapping') }}
),
columns_standardized as (
    SELECT
        index as row_id,
        order_id,
        cast(date as DATE) as order_date,
        lower(status) as order_status,
        lower(fulfilment) as fulfilment,
        lower(sales_channel) as sales_channel,
        lower(ship_service_level) as ship_service_level,
        style,
        sku,
        lower(category) as category,
        size,
        asin,
        lower(courier_status) as courier_status,
        quantity,
        currency,
        amount,
        lower(ship_city) as ship_city,
        split_part(lower(ship_state),'/',1) as ship_state,
        ship_postal_code,
        lower(ship_country) as ship_country,
        promotion_id,
        b2b,
        fulfilled_by
    FROM
        source
),
clean_values as (
    SELECT
        row_id,
        order_id,
        order_date,
        order_status,
        fulfilment,
        sales_channel,
        ship_service_level,
        style,
        sku,
        category,
        size,
        asin,
        case when order_status='cancelled' and courier_status is null then 'cancelled' else courier_status end as courier_status,
        case when quantity>=0 then quantity ELSE 0 end as quantity,
        COALESCE(currency,'INR') as currency,
        case when amount IS NOT NULL then ABS(amount) ELSE 0 end as amount,
        COALESCE(ship_city,'unknown') as ship_city,
        COALESCE(ship_state,'unknown') as ship_state,
        ship_postal_code,
        COALESCE(ship_country,'unknown') as ship_country,
        COALESCE(promotion_id,'unknown') as promotion_id,
        b2b,
        COALESCE(fulfilled_by,'unknown') as fulfilled_by
    FROM
        columns_standardized
),
derived_columns as (

    SELECT 
        c.row_id,
        c.order_id,
        c.order_date,
       {{ clean_order_status('c.order_status') }} as order_status,
        c.fulfilment,
        c.sales_channel,
        c.ship_service_level,
        c.style,
        c.sku,
        c.category,
        c.size,
        c.asin,
        c.courier_status,
        c.quantity,
        c.currency,
        c.amount,
        c.ship_city,
        COALESCE(m.state_name, c.ship_state) as ship_state,
        c.ship_postal_code,
        c.ship_country,
        c.promotion_id,
        c.b2b,
        c.fulfilled_by,
        c.quantity * c.amount as order_value,
        CASE 
            WHEN c.order_status = 'cancelled' 
            THEN TRUE 
            ELSE FALSE 
        END as is_cancelled
    FROM 
        clean_values c
    LEFT JOIN 
        state_map m
    ON 
        UPPER(c.ship_state) = m.state_code
)

SELECT * FROM derived_columns