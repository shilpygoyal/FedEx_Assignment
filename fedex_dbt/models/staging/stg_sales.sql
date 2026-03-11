{{ config(materialized = 'view') }}

/*
Model: stg_sales

Description:
  Staging model for the raw Fashionable sales report. This model is the
  single entry point for all source data into the dbt pipeline. It performs
  only column renaming and selection — no business logic, filtering, casting,
  or cleaning is applied here.

Materialization:
  View — no storage cost. Recomputed on each downstream reference.

Source:
  fedex.sale_report — loaded into DuckDB via scripts/pipeline.py using
  read_csv_auto() on 'data/Fashionable Sale Report.csv'.

Columns:
  index               Row number from the original CSV file. Carried forward
                      as a stable row identifier. Renamed to row_id in
                      int_sales.
  order_id            Unique order identifier from the source system.
                      Maps to raw column "Order ID".
  date                Raw order date string as read from CSV (e.g. '04-30-22').
                      Cast to DATE type in int_sales.
  status              Raw order lifecycle status string.
                      Normalised via clean_order_status() macro in int_sales.
  fulfilment          Party responsible for fulfilling the order
                      (e.g. 'Merchant', 'Fashionable').
  sales_channel       Platform through which the order was placed
                      (e.g. 'Fashionable.in').
  ship_service_level  Shipping speed selected by the customer
                      (e.g. 'Standard', 'Expedited').
  style               Product style code (e.g. 'SET389').
  sku                 Stock Keeping Unit — internal product identifier.
  category            Product category (e.g. 'kurta', 'Set').
  size                Product size variant (e.g. 'S', 'M', 'XL', '3XL').
  asin                Amazon Standard Identification Number.
  courier_status      Logistics tracking status from the courier provider.
  quantity            Number of units ordered. Raw value — may contain
                      negatives or nulls; cleaned in int_sales.
  currency            Transaction currency code. Mostly 'INR'.
  amount              Unit price at time of order. Raw value — may contain
                      nulls or negatives; cleaned in int_sales.
  ship_city           Destination city of the shipment.
  ship_state          Destination state. May contain '/' separators
                      (e.g. 'MAHARASHTRA/GOA'); split in int_sales.
  ship_postal_code    Indian PIN code of the delivery address.
  ship_country        Destination country code (e.g. 'IN').
  promotion_id        Promotion or discount identifier applied to the order.
                      May contain multiple concatenated codes.
  b2b                 Boolean flag indicating a business-to-business order.
  fulfilled_by        Operational fulfilment method (e.g. 'Easy Ship').

Known Limitations:
  - No data quality checks are applied at this layer. All validation
    occurs in int_sales and via source tests in sources.yml.
*/


WITH source as (
    SELECT * EXCLUDE ("Unnamed: 22") from {{ source('fedex', 'sale_report') }}
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