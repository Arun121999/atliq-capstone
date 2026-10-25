SELECT 
    o.order_id, 
    o.customer_id, 
    o.order_date, 
    CASE 
        WHEN LOWER(TRIM(o.status)) IN ('placed') THEN 'Placed'
        WHEN LOWER(TRIM(o.status)) IN ('shipped') THEN 'Shipped'
        WHEN LOWER(TRIM(o.status)) IN ('delivered') THEN 'Delivered'
        WHEN LOWER(TRIM(o.status)) IN ('cancelled', 'canceled') THEN 'Cancelled'
        WHEN LOWER(TRIM(o.status)) IN ('returned') THEN 'Returned'
        ELSE 'Placed'
    END AS status,
    o.order_amount
FROM {{ source('silver', 'orders') }} o
JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.customer_id
WHERE NOT c.is_test