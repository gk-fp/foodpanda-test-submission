{{
    config(
        materialized='view'
    )
}}

SELECT r.country_name, r.vendor_name, ROUND(r.total_gmv, 2)
FROM (
SELECT o.country_name, v.vendor_name, SUM(gmv_local) AS total_gmv, 
RANK() over(PARTITION BY(o.country_name) ORDER BY SUM(gmv_local) DESC) AS rank
FROM {{ ref('orders') }} AS o
JOIN {{ ref('vendors') }} AS v ON o.vendor_id = v.id
GROUP BY o.country_name, v.vendor_name) AS r
WHERE r.rank = 1