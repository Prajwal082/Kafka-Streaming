WITH source_customers AS (
    SELECT
        CUSTOMERID AS Customer_ID,
        CUSTOMERFIRSTNAME AS First_Name,
        CUSTOMERLASTNAME AS Last_Name,
        CUSTOMERPHONE AS Phone,
        CUSTOMEREMAIL AS Email,
        REGION AS Region,
        CITY AS City,
        PINCODE AS Pincode
    FROM 
        {{ source('orders','ORDERS_STREAM') }}
) 

SELECT 
    *
    FROM source_customers