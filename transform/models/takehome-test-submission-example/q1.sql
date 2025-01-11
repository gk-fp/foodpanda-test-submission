{{
    config(
        materialized='view'
    )
}}

SELECT
 country_name,
 ROUND(SUM(gmv_local), 2) AS total_gmv
FROM `dhh---analytics-apac.sandbox_ssc.orderscsc`
GROUP BY country_name
ORDER BY total_gmv DESC
