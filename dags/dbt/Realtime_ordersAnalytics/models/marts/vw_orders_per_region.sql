SELECT
    c.REGION AS Region,
    COUNT(CASE WHEN o.Order_Status = 'DELIVERED' THEN 1 END) AS Count
    FROM 
    {{ ref('f_orders') }} o
    LEFT JOIN {{ ref('d_customer')}} c
    ON o.Customer_Id = c.Customer_Id
    GROUP BY c.REGION

