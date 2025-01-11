{{
    config(
        materialized='view'
    )
}}

SELECT
   vendors.vendor_name,
   COUNT(DISTINCT orders.customer_id) AS customer_count,
   SUM(orders.gmv_local) AS total_gmv,
FROM `dhh---analytics-apac.sandbox_ssc.orderscsc` AS orders
JOIN `dhh---analytics-apac.sandbox_ssc.vendorscsv` AS vendors
   ON orders.vendor_id = vendors.id
WHERE orders.country_name = 'Taiwan'
GROUP BY vendors.vendor_name
ORDER BY customer_count DESC
