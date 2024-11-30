-- dim_fulfillment
{{ 
    config(
        materialized='table'
    ) 
}}

WITH promotion AS (
    SELECT DISTINCT
        `promotion-ids` AS promotion_id,
        `SKU` AS sku,
        `Category` AS category,
        `Size` AS size,
        `ASIN`  
    FROM {{ source('bronze', 'amazon_sale_report')}}
)


SELECT
    *
FROM promotion

