WITH source AS(
    SELECT 
    *
    FROM 
    {{ source('orders','ORDERS_STREAM') }}
)

SELECT 
    *
    FROM source