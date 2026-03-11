{{
  config(
    materialized = 'table',
    )
}}

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