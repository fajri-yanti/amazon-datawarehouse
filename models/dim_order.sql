{{
    config(
        materialized='table'
    )
}}

WITH salesorder AS (
    SELECT DISTINCT
        `index`,
        `Order ID` AS order_id,
        `Date` AS date,
        `SKU` AS sku,
        `Status` AS status,
        `Courier Status` AS courier_status,
        `QTY` AS qty,
        `Amount` AS amount,
        `currency`
    FROM {{ source('bronze', 'amazon_sale_report') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['index', 'order_id']
    ) }} AS salesorder_id,
    order_id,
    date,
    sku,
    status,
    courier_status,
    qty,
    amount,
    currency
FROM salesorder