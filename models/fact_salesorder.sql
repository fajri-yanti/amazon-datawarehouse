-- fact_salesorder
{{
  config(
    materialized='table'
  )
}}

WITH sales_data AS (
  SELECT
    `Date` AS date,
    {{ dbt_utils.generate_surrogate_key(['`Order ID`', 'index']) }} AS salesorder_id,
    {{ dbt_utils.generate_surrogate_key(['SKU']) }} AS product_id,
    {{ dbt_utils.generate_surrogate_key(['`Fulfilment`', '`fulfilled-by`']) }} AS fulfillment_id,
    {{ dbt_utils.generate_surrogate_key(['`promotion-ids`']) }} AS promotion_id,
    {{ dbt_utils.generate_surrogate_key(['`Sales Channel `', 'B2B']) }} AS channel_id,
    {{ dbt_utils.generate_surrogate_key( ['`ship-city`', '`ship-state`']) }} AS ship_id,
    `Status` AS status,
    `Qty` AS qty,
    `Amount` AS amount
  FROM
    {{ source('bronze', 'amazon_sale_report') }}
)

SELECT
  date,
  salesorder_id,
  product_id,
  fulfillment_id,
  promotion_id,
  channel_id,
  ship_id,
  status,
  SUM(qty) AS total_qty,
  SUM(amount) AS total_amount
FROM
  sales_data
GROUP BY
  date,
  salesorder_id,
  product_id,
  status,
  fulfillment_id,
  promotion_id,
  channel_id,
  ship_id