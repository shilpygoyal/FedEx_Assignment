{{
  config(
    materialized = 'table',
    )
}}

/*
Model: dim_product

Description:
  Product dimension containing one row per unique product variant, defined
  by the combination of SKU, style, and size. Captures the descriptive
  attributes of each product offered in the Fashionable catalogue.

Grain:
  One row per unique (sku, style, size) combination.

Source:
  Derived from int_sales. Product attributes are extracted from order-level
  data.

Primary Key:
  product_sk — MD5 surrogate key derived from sku and style.

Deduplication:
  ROW_NUMBER() partitions on (sku, style, size) to eliminate duplicate
  product records that arise from the same product appearing across
  multiple orders.

Columns:
  product_sk    Surrogate primary key. MD5 hash of sku and style.
  sku           Stock Keeping Unit — the internal product identifier.
  style         Style code grouping related product variants (e.g. SET389).
  asin          Amazon Standard Identification Number for marketplace listing.
  category      Product category (e.g. kurta, Set, Western Dress).
                Lowercased in int_sales for consistency.
  size          Size variant of the product (e.g. S, M, L, XL, 3XL).

Usage Notes:
  - Join to fact_sales on product_sk.
  - asin is an attribute, not a key — the same ASIN may appear across sizes.
*/
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
  {{ dbt_utils.generate_surrogate_key(['sku','style','size'])}} as product_sk,
  sku,
  style,
  asin,
  category,
  size
FROM
  deduplicate_records 
WHERE 
  rn = 1