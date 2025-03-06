SELECT

    SUM(CASE WHEN Order_Status = 'DELIVERED' THEN 1 END) AS Orders_Delivered,
    SUM(CASE WHEN Order_Status = 'SHIPPED' THEN 1 END) AS Orders_Shipped,
    SUM(CASE WHEN Order_Status = 'PENDING' THEN 1 END) AS Orders_Pending,
    SUM(CASE WHEN Order_Status = 'IN-PROGRESS' THEN 1 END) AS Orders_Inprogress,
    SUM(CASE WHEN Order_Status = 'CANCELLED' THEN 1 END) AS Orders_Cancelled,
    SUM(CASE WHEN Order_Status = 'TRANSACTION FAILED' THEN 1 END) AS Orders_With_Transaction_Failed,
    CONCAT(
        ROUND(
            (
                SUM(CASE WHEN Order_Status = 'DELIVERED' THEN 1 END) / COUNT(1)
            ) * 100
            ,2
        ),
        '%'
    ) AS  Percaentage_Orders_Delivered,
    CONCAT(
        ROUND(
            (
                SUM(CASE WHEN Order_Status = 'CANCELLED' THEN 1 END) / COUNT(1)
            ) * 100
            ,2
        ),
        '%'
    ) AS  Percaentage_Orders_Cancelled
FROM
    {{ ref("f_orders")}}