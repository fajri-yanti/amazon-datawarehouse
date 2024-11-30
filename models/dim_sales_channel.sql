-- dim_channel
{{
  config(
    materialized='table'
  )
}}

With channel AS (
SELECT DISTINCT 
  `Sales Channel` AS sales_channel,
  `B2B` 
FROM
    {{ source('bronze', 'amazon_sale_report') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['sales_channel', 'B2B']
    ) }} AS channel_id,
    sales_channel,
    B2B
FROM channel