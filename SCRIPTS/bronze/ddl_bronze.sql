/* 
====================================================================
DDL Script: Create Bronze Tables
====================================================================
Scripts Purpose:
    This script creates table in the 'bronze' schema, dropping existing
    tables if they already exist.
RUN this script to re-define the DDL structure of 'bronze' Tables
====================================================================
*/

--- Create Table for the bronze Layer

IF OBJECT_ID ('Bronze.crm_cust_info', 'U') IS NOT NULL	
DROP TABLE Bronze.crm_cust_info;

CREATE TABLE Bronze.crm_cust_info (
cst_id			     INT,
cst_key				 NVARCHAR(50),
cst_firstname		 NVARCHAR(50),
cst_lastname		 NVARCHAR(50),
cst_marital_status	 NVARCHAR(50),
cst_gndr			 NVARCHAR(50),
cst_create_date		 VARCHAR(50)
);


IF OBJECT_ID ('Bronze.crm_prd_info', 'U') IS NOT NULL	
DROP TABLE Bronze.crm_prd_info;

CREATE TABLE Bronze.crm_prd_info (
prd_id				INT,
prd_key				NVARCHAR(50),
prd_nm			    NVARCHAR(50),
prd_cost		    INT,
prd_line			NVARCHAR(50),
prd_start_dt	    VARCHAR(50),
prd_end_dt			VARCHAR(50)
);


IF OBJECT_ID ('Bronze.crm_sales_info', 'U') IS NOT NULL 	
DROP TABLE Bronze.crm_sales_info;

CREATE TABLE Bronze.crm_sales_info (
sls_ord_num			NVARCHAR(50),
sls_prd_key			NVARCHAR(50),
sls_cust_id			INT,
sls_order_dt		VARCHAR(50),
sls_ship_dt			VARCHAR(50),
sls_due_dt			VARCHAR(50),
sls_sales			INT,
sls_quantity	    INT,
sls_price			INT
);


IF OBJECT_ID ('Bronze.erp_cust_AZ12', 'U') IS NOT NULL 	
DROP TABLE Bronze.erp_cust_AZ12;

CREATE TABLE Bronze.erp_cust_AZ12 (
cid			NVARCHAR (50),
bdate		VARCHAR(50),
gen			NVARCHAR (50)
);

IF OBJECT_ID ('Bronze.erp_loc_a101', 'U') IS NOT NULL 	
DROP TABLE Bronze.erp_loc_a101;

CREATE TABLE Bronze.erp_loc_a101 (
cid			NVARCHAR (50),
cntry		NVARCHAR (50)
);

IF OBJECT_ID ('Bronze.erp_px_cat_g1v2', 'U') IS NOT NULL 	
DROP TABLE Bronze.erp_px_cat_g1v2;

CREATE TABLE  Bronze.erp_px_cat_g1v2 (
id				NVARCHAR (50),
cat				NVARCHAR (50),
subcat			NVARCHAR (50),
maintenance		NVARCHAR (50)
);


-- Creating stored procedure to load data into the bronze layer
EXEC Bronze.load_Bronze;
GO
CREATE OR ALTER PROCEDURE Bronze.load_Bronze
AS
BEGIN
	BEGIN TRY

			PRINT '================================================';
			PRINT 'Loading data into the Bronze layer...';
			PRINT '================================================';

			PRINT '-------------------------------------------------';
			PRINT 'loading CRM tables';
			PRINT '-------------------------------------------------';

			PRINT '>> Truncating Table: Bronze.crm_cust_info';
			TRUNCATE TABLE Bronze.crm_cust_info; --- To make the table empty before we start loading

			PRINT '>> Loading data into Bronze.crm_cust_info from cust_info.csv';
			BULK INSERT Bronze.crm_cust_info
			FROM "C:\Users\USER\Documents\sql-data-warehouse-project\datasets\source_crm\cust_info.csv"
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',
				TABLOCK
				);
			SELECT COUNT (*) FROM Bronze.crm_cust_info; 


			-- for prd_info
			PRINT '>> Truncating Table: Bronze.crm_Prd_info';
			TRUNCATE TABLE Bronze.crm_prd_info;
	
			PRINT '>> Loading data into Bronze.crm_prd_info from prd_info.csv';
			BULK INSERT Bronze.crm_prd_info
			FROM "C:\Users\USER\Documents\sql-data-warehouse-project\datasets\source_crm\prd_info.csv" 
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',
				TABLOCK
				);
			SELECT COUNT (*) FROM Bronze.crm_prd_info;


			-- for sales_info
			PRINT '>> Truncating Table: Bronze.crm_sales_info';
			TRUNCATE TABLE Bronze.crm_sales_info;

			PRINT '>> Loading data into Bronze.crm_sales_info from sales_details.csv';
			BULK INSERT Bronze.crm_sales_info
			FROM "C:\Users\USER\Documents\sql-data-warehouse-project\datasets\source_crm\sales_details.csv"
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',
				TABLOCK
				);
			SELECT COUNT (*) FROM Bronze.crm_sales_info;


			PRINT '-------------------------------------------------';
			PRINT 'loading ERP tables';
			PRINT '-------------------------------------------------';


			-- for cust_az12
			PRINT '>> Truncating Table: Bronze.erp_cust_AZ12';
			TRUNCATE TABLE Bronze.erp_cust_AZ12;

			PRINT '>> Loading data into Bronze.erp_cust_AZ12 from CUST_AZ12.csv';
			BULK INSERT Bronze.erp_cust_AZ12
			FROM "C:\Users\USER\Documents\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv"
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',
				TABLOCK
				);
			SELECT COUNT (*) FROM Bronze.erp_cust_AZ12;


			-- for loc_a101
			PRINT '>> Truncating Table: Bronze.erp_loc_a101';
			TRUNCATE TABLE Bronze.erp_loc_a101;

			PRINT '>> Loading data into Bronze.erp_loc_a101 from LOC_A101.csv';
			BULK INSERT Bronze.erp_loc_a101
			FROM "C:\Users\USER\Documents\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv"
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',
				TABLOCK
				);
			SELECT  COUNT (*) FROM Bronze.erp_loc_a101;


			-- for px_cat_g1v2
			PRINT '>> Truncating Table: Bronze.erp_px_cat_g1v2';
			TRUNCATE TABLE Bronze.erp_px_cat_g1v2;

			PRINT '>> Loading data into Bronze.erp_px_cat_g1v2 from PX_CAT_G1V2.csv';
			BULK INSERT Bronze.erp_px_cat_g1v2
		FROM "C:\Users\USER\Documents\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv"
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
			);
		
	END TRY
	BEGIN CATCH
		PRINT '=======================================================';
		PRINT 'ERROR OCCURED WHILE LOADING DATA INTO THE  BRONZE LAYER';
		PRINT 'Error message' + 'ERROR MESSAGE()';
		PRINT 'Error message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=======================================================';
	END CATCH
END

