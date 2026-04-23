/*
===============================================================================
Quality Checks for Silver Layer
===============================================================================
Script Purpose:
    This script performs data quality checks on the Silver layer to ensure:
    - Data consistency and accuracy
    - Standardization of values
    - Referential integrity
    - Business rule compliance

    Checks include:
        - Null or duplicate primary keys
        - Unwanted leading/trailing spaces in string fields
        - Data standardization and consistency
        - Invalid date ranges or illogical date orders
        - Consistency between related fields

Usage Notes:
    - Run this script AFTER executing Silver.load_silver
    - Zero rows returned = Check passed
    - Any rows returned = Data quality issue → Investigate and fix
    - Recommended to run as part of the daily/ETL validation process

===============================================================================
*/


--- ======================================================
--- Checks for data quality in 'Silver.crm_cust_info'
--- ======================================================

--- Removing duplicates
SELECT 
*
FROM (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM Silver.crm_cust_info
) t
WHERE flag_last = 1;


--- Check for unwanted spaces
--- Expectations: No results

SELECT 
cst_firstname,
cst_lastname
FROM Silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname) AND cst_lastname != TRIM(cst_lastname); 

SELECT
cst_gndr
FROM Silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

--- Data standardization & consistency
SELECT DISTINCT cst_marital_status
FROM Silver.crm_cust_info; 

SELECT *
FROM Silver.crm_cust_info;


--- ======================================================
--- Checks for data quality in 'Silver.crm_prd_info'
--- ======================================================

-- Check for Duplicates & NUll in primary key: Quality checks
-- Expectations : No results

SELECT
prd_id,
COUNT(*)
FROM Silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT (*)> 1 OR prd_id IS NULL;


--- Checking for unwanted spaces
--- Expectations: No results

SELECT prd_nm
FROM Silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);



--- Check for NULL or negative values 
--- Expectations: No results

SELECT prd_cost
FROM Silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;


--- Data standardization & consistency
SELECT DISTINCT prd_line
FROM Silver.crm_prd_info; 

-- Check for invalid Dates order
SELECT *
FROM Silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt



--- ======================================================
--- Checks for data quality in 'Silver.crm_sales_details'
--- ======================================================

--- Quality checks for Silver.crm_sales_details
--- check for invalid dates 
SELECT 
NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM Bronze.crm_sales_info
WHERE sls_due_dt <= 0 
OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101

--- Check for invalid dates order
SELECT *
FROM Bronze.crm_sales_info
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt


--- check for Data consistency: Between Sales, Quantity, and price
-- << Sales: quantity * price
-- << values must not be NULL, negative, or zero.
-- << Derived rules: 
-- 1. If Sales is negative, zero, or NULL, deriv it using quantity and price
-- 2. If Price is zero or Null, calculate using Sales and quantity.
-- 3. If price is negative value, convert to positive value.


SELECT DISTINCT
    sls_sales AS old_sls_sales,
    sls_quantity,
    sls_price AS old_sls_price,

    -- Cleaned Sales Amount
    CASE 
        WHEN sls_sales IS NULL 
          OR sls_sales <= 0 
          OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales 
    END AS sls_sales,

    -- Cleaned Price
    CASE 
        WHEN sls_price IS NULL 
          OR sls_price <= 0 
        THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price 
    END AS sls_price

FROM Bronze.crm_sales_info
WHERE sls_sales != sls_price * sls_quantity
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_price, sls_quantity;



--- ======================================================
--- Checks for data quality in 'Silver.erp_cust_AZ12'
--- ======================================================


--- Data standardization and Consistency

SELECT DISTINCT
bdate
FROM Silver.erp_cust_AZ12;


--- Identifying Out-of -range Dates
SELECT DISTINCT
bdate
FROM Silver.erp_cust_AZ12
WHERE bdate < '1923-01-01' OR bdate > GETDATE()

--- Checking for gen
SELECT DISTINCT
gen AS old_gen,

CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'FEMALE'
	 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'MALE'
	 ELSE 'n/a'
END AS original_gen

FROM Silver.erp_cust_AZ12;


--- ======================================================
--- Checks for data quality in 'Silver.erp_loc_a101'
--- ======================================================
--- Data Standardization & consistency check

SELECT DISTINCT
cntry AS old_cntry,
CASE WHEN TRIM (cntry) = 'DE' THEN 'Germany'
	 WHEN TRIM (cntry) IN ('US', 'USA') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END AS updated_cntry
FROM Silver.erp_loc_a101


--- ======================================================
--- Checks for data quality in 'Silver.erp_px_cat_g1v2'
--- ======================================================
--- Data Standarization and Consistency

SELECT DISTINCT
maintenance
FROM Silver.erp_px_cat_g1v2

--- Checking for unwanted spaces

SELECT *
FROM Silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)



