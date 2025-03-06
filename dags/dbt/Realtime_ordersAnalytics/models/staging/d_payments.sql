WITH source_payments AS (
    SELECT
        ORDERID AS Order_Id,
        PAYMENTMETHOD AS Payment_Method,
        PAYMENTSTATUS AS Payment_Status,
        TRANSACTIONID AS Transaction_Id
        FROM 
        {{ source('orders','ORDERS_STREAM')}}
)

SELECT
    *
    FROM
    source_payments