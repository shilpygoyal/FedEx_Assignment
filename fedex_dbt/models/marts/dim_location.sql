{{
  config(
    materialized = 'table',
    )
}}

/*
Model: dim_location

Description:
  Location dimension containing one row per unique shipping destination,
  defined by the combination of city, state, and postal code.

Grain:
  One row per unique (ship_city, ship_state, ship_postal_code) combination.

Source:
  Derived from int_sales. State abbreviations have been resolved to full
  state names via a LEFT JOIN to the india_state_mapping seed table.
  Cities and states are lowercased for consistency.

Primary Key:
  location_sk — MD5 surrogate key derived from ship_city, ship_state,
  and ship_postal_code.

Deduplication:
  ROW_NUMBER() partitions on (ship_city, ship_state, ship_postal_code)
  to handle repeated location combinations across multiple orders.

Columns:
  location_sk        Surrogate primary key. MD5 hash of city, state, postal code.
  ship_city          Destination city, lowercased (e.g. 'mumbai', 'bengaluru').
                     Defaults to 'unknown' where source value is NULL.
  ship_state         Full state name resolved from abbreviation via seed table
                     (e.g. 'MH' → 'Maharashtra'). Defaults to 'unknown' if
                     unmapped or NULL.
  ship_postal_code   Indian PIN code of the delivery address.
  ship_country       Destination country code (e.g. 'in'). Lowercased.
                     Defaults to 'unknown' where NULL.

Usage Notes:
  - Join to fact_sales on location_sk.
  - All location values reflect the shipping address, not billing address.
  - ship_state values have been cleaned: raw values containing '/' separators
    (e.g. 'MAHARASHTRA/GOA') are split and only the first part is retained.
*/
With location_source as (
    SELECT 
        ship_city,
        ship_state,
        ship_postal_code,
        ship_country
    FROM 
        {{ ref('int_sales') }}
),
deduplicate_records as (
  SELECT 
    ship_city,
    ship_state,
    ship_postal_code,
    ship_country,
    ROW_NUMBER() OVER(PARTITION BY ship_city,ship_state,ship_postal_code ORDER BY ship_city) as rn
  FROM 
    location_source
)

SELECT 
    {{dbt_utils.generate_surrogate_key(['ship_city','ship_state','ship_postal_code'])}} as location_sk,
    ship_city,
    ship_state,
    ship_postal_code,
    ship_country
FROM
  deduplicate_records 
WHERE 
  rn = 1