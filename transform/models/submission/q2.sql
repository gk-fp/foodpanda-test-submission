{{
    config(
        materialized='view'
    )
}}

SELECT v.vendor_name, count(o.customer_id) AS customer_count, ROUND(SUM(gmv_local),2) AS total_gmv
FROM {{ ref('orders') }} AS o
JOIN {{ ref('vendors') }} AS v ON o.vendor_id = v.id
WHERE o.country_name = 'Taiwan'
GROUP BY v.vendor_name
ORDER BY total_gmv DESC