{{
    config(
        materialized='view'
    )
}}

WITH Vendor_GMV AS (
   SELECT
       orders.vendor_id,
       vendors.vendor_name,
       orders.country_name,
       SUM(orders.gmv_local) AS total_gmv
   FROM `dhh---analytics-apac.sandbox_ssc.orderscsc` AS orders
   JOIN `dhh---analytics-apac.sandbox_ssc.vendorscsv` AS vendors
       ON orders.vendor_id = vendors.id
   GROUP BY orders.vendor_id, vendors.vendor_name, orders.country_name

)

SELECT
   country_name,
   vendor_name,
   ROUND(total_gmv, 2)
FROM ( SELECT
               country_name,
            vendor_name,
               total_gmv,
ROW_NUMBER() OVER (PARTITION BY country_name ORDER BY total_gmv DESC) AS rn
   FROM Vendor_GMV
) AS Ranked_Vendors
WHERE
   rn = 1
ORDER BY
   country_name;
