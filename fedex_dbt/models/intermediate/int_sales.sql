{{
  config(
    materialized = 'view',
    )
}}

/*
Model: int_sales

Description:
  Central intermediate model that applies all business logic, data cleaning,
  and standardisation to the raw sales data. This is the single source of
  truth for cleaned sales records — all downstream dimension and fact models
  reference this model exclusively.

  Transformations applied in this model (in CTE order):
    1. columns_standardized — type casting, string lowercasing, state cleaning
    2. clean_values         — NULL handling, negative value correction
    3. derived_columns      — state name resolution, status normalisation,
                              flag and measure derivation

Materialization:
  View — recomputed on each downstream reference. All five dimension tables
  and fact_sales build from this model.
  
Source:
  stg_sales — column-renamed view over the raw sale_report table.
  india_state_mapping — seed table mapping state abbreviations to full names.

Columns:
  row_id              Stable row identifier from the source CSV index column.
  order_id            Order identifier from the source system.
  order_date          Order placement date. Cast from raw string to DATE.
  order_status        Cleaned, standardised order status. Applied via the
                      clean_order_status() macro. Possible values: pending,
                      shipped, delivered, returned, exception, cancelled, unknown.
  fulfilment          Fulfilment party, lowercased (e.g. 'merchant').
  sales_channel       Sales platform, lowercased (e.g. 'fashionable.in').
  ship_service_level  Shipping tier, lowercased (e.g. 'standard').
  style               Product style code (unchanged from source).
  sku                 Stock Keeping Unit (unchanged from source).
  category            Product category, lowercased.
  size                Product size variant (unchanged from source).
  asin                Amazon Standard Identification Number (unchanged).
  courier_status      Courier tracking status, lowercased. NULL values for
                      cancelled orders are imputed as 'cancelled'.
  quantity            Units ordered. Negative values clamped to 0.
  currency            Transaction currency. Defaults to 'INR' where NULL.
  amount              Unit price. NULL values defaulted to 0; negatives
                      converted to ABS(amount).
  ship_city           Destination city, lowercased. Defaults to 'unknown'.
  ship_state          Full state name resolved from abbreviation via
                      india_state_mapping seed. Defaults to 'unknown' if
                      unmapped or NULL. Cleaned of '/' separator variants.
  ship_postal_code    Indian PIN code (unchanged from source).
  ship_country        Destination country code, lowercased. Defaults to 'unknown'.
  promotion_id        Promotion code applied to the order. Defaults to 'unknown'.
  b2b                 Boolean. TRUE for business-to-business orders.
  fulfilled_by        Operational fulfilment method. Defaults to 'unknown'.
  order_value         Derived measure: quantity * amount. Total line value.
  is_cancelled        Boolean flag. TRUE where order_status = 'cancelled'.

*/

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
/* Step 1: Type casting and string standardisation */
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
/* Step 2: NULL handling and value correction */
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
/* Step 3: Business logic — status normalisation, state resolution, derived columns */
derived_columns as (
    SELECT 
        c.row_id,
        c.order_id,
        c.order_date,
        -- Normalise raw status values into standard analytics categories
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
        -- Derived measure: total line value
        c.quantity * c.amount as order_value,
        -- Derived flag: marks cancelled orders for easy filtering
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
),
deduped as (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, sku
            ORDER BY row_id
        ) as rn
    FROM derived_columns
)

SELECT * EXCLUDE (rn) FROM deduped WHERE rn = 1