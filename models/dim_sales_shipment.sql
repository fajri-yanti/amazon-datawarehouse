-- dim_channel
{{
  config(
    materialized='table'
  )
}}

With ship AS (
SELECT DISTINCT 
  `ship-city` AS ship_city,
  `ship-state` AS ship_state,
  `ship-country` AS ship_country
FROM
    {{ source('bronze', 'amazon_sale_report') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['ship_city', 'ship_state']
    ) }} AS ship_id, *
FROM ship