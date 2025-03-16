WITH source_orders AS(
    SELECT 
        ORDERID AS Order_Id,
        ORDERDATE AS Order_date,
        CUSTOMERID AS Customer_Id,
        PRODUCTID AS Product_Id,
        DISCOUNT AS Discount,
        TOTALUNITS AS Total_Units,
        POS,
        ORDERSTATUS AS Order_Status,
        TOTALAMOUNT AS Total_Amount,
        TIMESTAMP AS System_Timestamp
    FROM 
    {{ source('orders','ORDERS') }}
)

SELECT 
    *
    FROM source_orders