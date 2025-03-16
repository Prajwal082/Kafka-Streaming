WITH source_shipping AS (
    SELECT
        ORDERID AS Order_Id,
        SHIPPINGMETHOD AS Method, 
        SHIPPING_STREET AS Street, 
        SHIPPING_CITY AS City, 
        SHIPPING_STATE AS State, 
        SHIPPING_ZIPCODE AS Zip, 
        SHIPPING_COUNTRY AS Country, 
        ESTIMATEDDELIVERY AS Estimated_Delivery_Date
    FROM 
        {{ source('orders','ORDERS') }}
)

SELECT
    *
    FROM 
    source_shipping