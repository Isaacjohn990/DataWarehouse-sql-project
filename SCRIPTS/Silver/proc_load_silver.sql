/*
===============================================================================
Script Purpose:
    This stored procedure performs the ETL process to load the Silver layer 
    from the Bronze layer (Bronze → Silver).

    Actions Performed:
        - Truncates all tables in the 'silver' schema.
        - Inserts transformed, cleansed, and standardized data from the 
          'bronze' schema into the corresponding 'silver' tables.
        - Applies business rules, data quality checks, and transformations 
          as required for the Silver layer.

Parameters:
    None.

Returns:
    None.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

GO
CREATE OR ALTER PROCEDURE Silver.load_Silver
AS
BEGIN
	   BEGIN TRY


--- TRANSFORMING AND LOADING DATA INTO SILVER.CRM_CUST_INFO
PRINT '================================================';
PRINT 'Loading Silver Layer';
PRINT '================================================';

PRINT '------------------------------------------------';
PRINT 'Loading CRM Tables';
PRINT '------------------------------------------------';


PRINT '>> Truncating Table: Silver.crm_cust_info';
TRUNCATE TABLE Silver.crm_cust_info;

PRINT '>> Inserting Data into: Silver.crm_cust_info';
INSERT INTO Silver.crm_cust_info (
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date)

SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	 ELSE 'n/a'
END cst_marital_status,

CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	 ELSE 'n/a'
END cst_gndr,

cst_create_date
FROM (
	SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM Bronze.crm_cust_info
	WHERE cst_id IS NOT  NULL
)t WHERE flag_last =1;


--- TRANSFORMING AND LOADING DATA INTO SILVER.CRM_PRD_INFO
PRINT '>> Truncating Table: Silver.crm_prd_info';
TRUNCATE TABLE Silver.crm_prd_info;

PRINT '>> Inserting Data into: Silver.crm_prd_info';
INSERT INTO Silver.crm_prd_info (
prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
)
SELECT
prd_id,
REPLACE(SUBSTRING(prd_key,1,5),'-', '_') AS cat_id,
SUBSTRING (prd_key,7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
CASE UPPER(TRIM (prd_line))
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Rod'
	WHEN 'S' THEN 'Other Sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
END AS prd_line,
CAST (prd_start_dt AS DATE) AS prd_start_dt,

DATEADD(DAY, -1, 
            LEAD(TRY_CAST(prd_start_dt AS DATE)) 
                 OVER (PARTITION BY prd_key 
                       ORDER BY TRY_CAST(prd_start_dt AS DATE))
           ) AS prd_end_dt

FROM Bronze.crm_prd_info


--- CLEAN AND LOAD DATA INTO SILVER.CRM_SALES_DETAILS
PRINT '>> Truncating Table: Silver.crm_sales_details';
TRUNCATE TABLE Silver.crm_sales_details;

PRINT '>> Inserting Data into: Silver.crm_sales_details';

INSERT INTO Silver.crm_sales_details (
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
)
SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN (sls_order_dt) != 8 THEN NULL
	ELSE CAST (CAST (sls_order_dt AS VARCHAR) AS DATE) --- coverting to varchar, then to date
END AS sls_order_dt,

CASE WHEN sls_ship_dt = 0 OR LEN (sls_ship_dt) != 8 THEN NULL
	ELSE CAST (CAST (sls_ship_dt AS VARCHAR) AS DATE) --- coverting to varchar, then to date
END AS sls_ship_dt,

CASE WHEN sls_due_dt = 0 OR LEN (sls_due_dt) != 8 THEN NULL
	ELSE CAST (CAST (sls_due_dt AS VARCHAR) AS DATE) --- coverting to varchar, then to date
END AS sls_due_dt,

CASE 
     WHEN sls_sales IS NULL 
     OR sls_sales <= 0 
     OR sls_sales != sls_quantity * ABS(sls_price)
     THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales 
END AS sls_sales,
sls_quantity,

CASE 
    WHEN sls_price IS NULL 
    OR sls_price <= 0 
    THEN sls_sales / NULLIF(sls_quantity, 0)
    ELSE sls_price 
    END AS sls_price

FROM Bronze.crm_sales_info



--- TRANSFORMATION AND LOADING DATA INTO SILVER.ERP_CUST_AZ12

PRINT '------------------------------------------------';
PRINT 'Loading ERP Tables';
PRINT '------------------------------------------------';

PRINT '>> Truncating Table: Silver.erp_cust_AZ12';
TRUNCATE TABLE Silver.erp_cust_AZ12;

PRINT '>> Inserting Data into: Silver.erp_cust_AZ12';
INSERT INTO Silver.erp_cust_AZ12 (
cid,
bdate,
gen
)

SELECT 
CASE WHEN cid LIKE 'NAS%'THEN SUBSTRING(cid,4, LEN(cid)) --- Remove 'NSA' prefix if present
	 ELSE cid
END AS updated_cid,

CASE WHEN bdate > GETDATE() THEN NULL  --- Set future birthdates to NULL
	ELSE bdate
END AS bdate,

CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'FEMALE' 
	 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'MALE'
	 ELSE 'n/a'
END AS original_gen  ---- Normalize gender values
FROM Bronze.erp_cust_AZ12;




--- CLEANING & LOADING INTO SILVER.ERP_LOC_A101
PRINT '>> Truncating Table: Silver.erp_loc_a101';
TRUNCATE TABLE Silver.erp_loc_a101;

PRINT '>> Inserting Data into: Silver.erp_loc_a101';
INSERT INTO Silver.erp_loc_a101 (cid, cntry)
SELECT 
REPLACE (cid,'-','_') cid,

CASE WHEN TRIM (cntry) = 'DE' THEN 'Germany'
	 WHEN TRIM (cntry) IN ('US', 'USA') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END AS updated_cntry

FROM Bronze.erp_loc_a101


--- CLEANING AND LOADING DATA INTO SILVER.ERP_PX_CAT_G1V2
PRINT '>> Truncating Table: Silver.erp_px_cat_g1v2';
TRUNCATE TABLE Silver.erp_px_cat_g1v2;

PRINT '>> Inserting Data into: Silver.erp_px_cat_g1v2';

INSERT INTO Silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
SELECT 
id,
cat,
subcat,
maintenance
FROM Bronze.erp_px_cat_g1v2
PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';		
PRINT '=========================================='
	  END TRY
	   BEGIN CATCH
	   
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	  END CATCH
END
