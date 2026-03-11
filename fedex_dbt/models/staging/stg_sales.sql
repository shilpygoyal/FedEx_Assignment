{{ config(materialized = 'view') }}

WITH source as (
    SELECT * from {{ source('fedex', 'sale_report') }}
),
column_renamed as (
    SELECT
        index,
        "Order ID" as order_id,
        Date as date,
        Status as status,
        Fulfilment as fulfilment,
        "Sales Channel" as sales_channel,
        "ship-service-level" as ship_service_level,
        Style as style,
        SKU as sku,
        Category as category,
        Size as size,
        ASIN as asin,
        "Courier Status" as courier_status,
        Qty as quantity,
        currency,
        Amount as amount,
        "ship-city" as ship_city,
        "ship-state" as ship_state,
        "ship-postal-code" as ship_postal_code,
        "ship-country" as ship_country,
        "promotion-ids" as promotion_id,
        B2B as b2b,
        "fulfilled-by" as fulfilled_by
        FROM 
            source
)

SELECT 
    *
FROM
    column_renamed