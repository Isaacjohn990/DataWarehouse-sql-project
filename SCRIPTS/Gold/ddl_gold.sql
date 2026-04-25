/*
=================================================================================
DDL Scripts: Create Gold view
=================================================================================
Script Purpose: 
This script creates views for the Gold layer in the data warehouse.  
The Gold layer contains the final, refined dimension and fact tables 
that form the (Star Schema) of the data warehouse.

Each view applies necessary transformations and combines data 
from the Silver layer to deliver clean, enriched, and business-ready datasets
for analytics and reporting.

===================================================================================
*/

-- ===========================================================
-- Create Dimension: Gold.customer_dim
-- ===========================================================

-- Creating gold layer
-- For customer dimension, we will use crm_cust_info as the main source and enrich it with data from erp_cust_AZ12 and erp_loc_a101. 
-- We will create a surrogate key for the customer dimension using ROW_NUMBER() function.
IF OBJECT_ID (Gold.customer_dim , v) IS NOT NULL
  DROP VIEW Gold.customer_dim
  
GO
  
CREATE VIEW Gold.customer_dim AS 
SELECT 
ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key, --- Creating surrogate key for customer dimension
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name,
ci.cst_lastname AS last_name,
la.cntry AS country,
ci.cst_marital_status AS marital_status,
CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr --- CRM IS THE MASTER FOR GENDER INFO
	 ELSE COALESCE (ca.gen, 'n/a') 
END AS gender,
ca.bdate AS birthdate,
ci.cst_create_date AS create_date
FROM Silver.crm_cust_info ci
LEFT JOIN Silver.erp_cust_AZ12 ca
ON		  ci.cst_key = ca.cid
LEFT JOIN Silver.erp_loc_a101 la
ON TRIM(ci.cst_key) = TRIM(REPLACE(la.cid, '_', ''))



-- ===========================================================
-- Create Dimension: Gold.Product_dim
-- ===========================================================
IF OBJECT_ID (Gold.product_dim , v) IS NOT NULL
  DROP VIEW Gold.product_dim

GO

CREATE VIEW Gold.Product_dim  AS
SELECT 
ROW_NUMBER() OVER (ORDER BY pt.prd_start_dt, pt.prd_key ) AS Product_key,
pt.prd_id AS product_id,
pt.cat_id AS category_id,
pv.cat AS category,
pv.subcat AS sub_category,
maintenance,
pt.prd_key AS product_number,
pt.prd_nm AS prodct_name ,
pt.prd_cost AS cost,
pt.prd_line AS product_line,
pt.prd_start_dt AS start_date
FROM Silver.crm_prd_info pt
LEFT JOIN Silver.erp_px_cat_g1v2 pv
ON pt.cat_id = pv.id
WHERE prd_end_dt IS NULL --- Filtering out all historical data

-- ===========================================================
-- Create Fact: Gold.Sales_fact
-- ===========================================================

--- Building Gold layer for Sales 
--- We used the dimension's surrogate keys, instead of IDS, to easily connect facts with dimension
--- (order_number, product_key, customer_id) - Dimensions keys
--- (Sales_order, shipping_date, due_date) - Dates
--- (sales_amount, sales_quantity, sales_price) - Measures

IF OBJECT_ID (Gold.Sales_fact, v) IS NOT NULL
  DROP VIEW Gold.sales_fact

GO

CREATE VIEW Gold.sales_fact AS
SELECT 
sa.sls_ord_num AS order_number,
pr.Product_key,
cu.customer_id,
sa.sls_order_dt AS sales_order,
sa.sls_ship_dt AS shipping_date,
sa.sls_due_dt AS due_date,
sa.sls_sales AS sales_amount,
sa.sls_quantity AS quantity,
sa.sls_price AS price
FROM Silver.crm_sales_details sa
LEFT JOIN Gold.Product_dim pr    
ON	 sa.sls_prd_key = pr.product_number
LEFT JOIN Gold.customer_dim cu
ON sa.sls_cust_id = cu.customer_id



