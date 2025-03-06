SELECT
    o.Customer_Id,
    CONCAT_WS(' ',c.First_Name,c.Last_Name) AS Customer_Name,
    ROUND(AVG(o.Total_Amount),4) AS Average_Order_Value
FROM
    {{ref("f_orders")}} o 
    JOIN {{ref("d_customer")}} c
    on o.Customer_Id = c.Customer_Id
    GROUP BY o.Customer_Id,c.First_Name,c.Last_Name
    ORDER BY COUNT(o.Customer_Id) DESC,Average_Order_Value DESC