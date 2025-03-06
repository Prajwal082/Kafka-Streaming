WITH source_products AS (
    SELECT
        PRODUCTID Product_Id, 
        PRODUCTNAME AS Product_Name, 
        CATEGORY AS Category, 
        BRAND AS Brand, 
        UNITPRICE AS Unit_Price
    FROM 
        {{ source('orders','ORDERS_STREAM') }}
)

SELECT 
    *
    FROM source_products