{{
    config(
        materialized='view'
    )
}}


SELECT CAST(r.year AS datetime) AS year, r.country_name, r.vendor_name, ROUND(r.total_gmv, 2) AS total_gmv
FROM (
SELECT o.date_local as year, o.country_name, v.vendor_name, SUM(gmv_local) AS total_gmv, 
RANK() over(PARTITION BY o.date_local, o.country_name ORDER BY o.date_local ASC, SUM(gmv_local) DESC) AS rank
FROM {{ ref('orders') }} AS o
JOIN {{ ref('vendors') }} AS v ON o.vendor_id = v.id
GROUP BY o.date_local, o.country_name, v.vendor_name
ORDER BY year, country_name) AS r
WHERE r.rank <= 2