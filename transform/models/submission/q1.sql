{{
    config(
        materialized='view'
    )
}}

-- 1 - Find the Total GMV by country

SELECT country_name, ROUND(SUM(gmv_local),2) as total_gmv
FROM {{ ref('orders') }} AS o
group by country_name