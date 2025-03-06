SELECT
        TOP 3
        p.Product_Name,
        p.Brand,
        sum(o.Total_Units) as Total_Sales
    FROM {{ ref('f_orders') }} o
    INNER JOIN {{ ref('d_product')}} p
    ON o.Product_Id = p.Product_Id
    GROUP BY p.Product_Name,p.Category,p.Brand
    ORDER BY Total_Sales DESC
