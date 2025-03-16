WITH source_payments AS (
    SELECT
        ORDERID AS Order_Id,
        PAYMENTMETHOD AS Payment_Method,
        PAYMENTSTATUS AS Payment_Status,
        TRANSACTIONID AS Transaction_Id
        FROM 
        {{ source('orders','ORDERS')}}
)

SELECT
    *
    FROM
    source_payments