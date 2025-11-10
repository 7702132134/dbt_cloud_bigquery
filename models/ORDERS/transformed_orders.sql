-- models/staging/stg_orders.sql
{{ config(
    materialized='table',
    schema='staging'
) }}

WITH source AS (
    SELECT
        order_id,
        customer_id,
        CAST(order_date AS DATE) AS order_date,
        INITCAP(product_name) AS product_name,
        INITCAP(category) AS category,
        quantity,
        ROUND(unit_price, 2) AS unit_price,
        ROUND(total_amount, 2) AS total_amount,
        UPPER(order_status) AS order_status,
        INITCAP(region) AS region
    FROM {{ source('bigquery_source', 'orders') }}
),

final AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        EXTRACT(YEAR FROM order_date) AS order_year,
        EXTRACT(MONTH FROM order_date) AS order_month,
        category,
        product_name,
        quantity,
        unit_price,
        total_amount,
        order_status,
        region
    FROM source
)

SELECT * FROM final

