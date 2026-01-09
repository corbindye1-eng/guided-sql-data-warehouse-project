/*
========================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
========================================================================

Script Purpose:
   This stored procedure loads data into the 'bronze' schema from external CSV files.
   It performs the following actions:
   - Truncates the bronze tables before loading data.
   - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
   None.
   This stored procedure does not accept any parameters or return any values.

Usage Example:
   EXEC bronze.load_bronze;

========================================================================
*/
--Creating a stored procedure of all the bulk inserts
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME; -- Create variables in order to determine how long it takes to load bronze layer
	BEGIN TRY-- Try put in place to run if there are no erros
		SET @start_time = GETDATE();--Determine when bronze layer loading started
		PRINT '================================'
		PRINT 'LOADING BRONZE LAYER';
		PRINT '================================';
		PRINT 'LOADING CRM TABLES';
		PRINT '================================';
		--Loading data from csv into database table

		--Truncate table first to prevent double loading of data
		PRINT 'Truncating Table : bronze.crm_cust_info';

		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT 'Inserting Data Into: bronze.crm_cust_info';
		--Now load data into table
		BULK INSERT bronze.crm_cust_info
		FROM
			'C:\Users\Declan\Documents\SQL Server Management Studio 22\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		WITH
		(
			FIRSTROW = 2,  --Tells SQL Server to treat the second line of csv as first line of data, because first line in csv is headings
			FIELDTERMINATOR = ',', --Specifies field delimiter
			TABLOCK
		)

		--Table 2
		PRINT 'Truncating Table : bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT 'Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 
			'C:\Users\Declan\Documents\SQL Server Management Studio 22\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		--TABLE 3
		PRINT 'Truncating Table : bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT 'Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM
			'C:\Users\Declan\Documents\SQL Server Management Studio 22\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'	
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT '=======================';
		PRINT 'LOADING ERP TABLES';
		PRINT '=======================';
		--TABLE 4
		PRINT 'Truncating Table : bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT 'Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM
			'C:\Users\Declan\Documents\SQL Server Management Studio 22\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ','
		);

		--TABLE 5
		PRINT 'Truncating Table : bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT 'Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM
			'C:\Users\Declan\Documents\SQL Server Management Studio 22\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ','
		);
	  
		--TABLE 6
		PRINT 'Truncating Table : bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT 'Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM
			'C:\Users\Declan\Documents\SQL Server Management Studio 22\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ','
		);
		SET @end_time = GETDATE();--Determine when bronze layer loading ended
		PRINT 'Time to load bronze layer: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';--Calculation to determine load time
	END TRY
	BEGIN CATCH --Catch errors and display the following message
		PRINT 'ERROR OCCURED';
		PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
		PRINT 'ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
	END CATCH
END
