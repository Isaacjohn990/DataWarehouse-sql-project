/*
==============================================================================
Stored Procedure: Load Bronze layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedures loads data into the 'bronze' schema from external CSV files.
    it performs the following actions:
    - Truncates the bronze tables before loading data
    - Uses the 'BULK INSERT' command to load data from csv files to bronze tables.


Parameters:
    None: 
  This stores procedures does not accept any parameters or return any values

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/


-- Creating stored procedure to load data into the bronze layer

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

