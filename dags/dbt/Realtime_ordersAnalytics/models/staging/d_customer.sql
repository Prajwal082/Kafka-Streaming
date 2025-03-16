WITH source_customers AS (
    SELECT
        CUSTOMERID AS Customer_ID,
        CUSTOMER_FIRSTNAME AS First_Name,
        CUSTOMER_LASTNAME AS Last_Name,
        CUSTOMER_PHONE AS Phone,
        CUSTOMER_EMAIL AS Email,
        REGION AS Region,
        CITY AS City,
        PINCODE AS Pincode
    FROM 
        {{ source('orders','ORDERS') }}
) 

SELECT 
    *
    FROM source_customers