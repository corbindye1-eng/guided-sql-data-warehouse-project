/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to
    populate the 'silver' schema tables from the 'bronze' schema.
    
Actions Performed:
    - Truncates Silver tables.
    - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

/*Extracting data from bronze table, cleaning(transforming) 
the data, and loading/inserting it into silver table*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
BEGIN TRY
	TRUNCATE TABLE silver.crm_cust_info;

	INSERT INTO silver.crm_cust_info (
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
		TRIM(cst_firstname) AS cst_firstname, --Removes uneccessary spaces
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single' --Transforms single characters into full words for better understanding(Normalization)
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			ELSE 'n/a'--Hanndling missing data
		END cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'--Transforms single characters into full words for better understanding(Normalization)
			WHEN UPPER(TRIM(cst_gndr)) ='M' THEN 'Male'
			ELSE 'n/a'--Handling missing data
		END AS cst_gndr,
		cst_create_date
	FROM 
	(
		SELECT
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last --Ranks data based on cst_id and ordering 
		FROM                                                                                     --by creation date to find duplicates and only using newest entry
			bronze.crm_cust_info
		WHERE cst_id IS NOT NULL --Prevents selecting null ids which cannot be used
	)t
	WHERE
		flag_last = 1 --Selects only newest entry per customer id


	TRUNCATE TABLE silver.crm_prd_info;

	INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm ,
		prd_cost ,
		prd_line ,
		prd_start_dt ,
		prd_end_dt  
	)
	SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') AS cat_id, --extract cat_id and transform to match that of erp_px_cat_g1v2
		SUBSTRING(prd_key,7, LEN(prd_key)) AS prd_key, --need to find the product key without the category part to be able to connect to crm_sales_details
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
			 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
			 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'other Sales'
			 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				  ELSE 'n/a'--Map product line codes to descrptive values (data normalization)
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,--Simplifying date to not show hours and minutes
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE)AS prd_end_dt --Set end date for product equal to day before start date of new entry of same product
	FROM 
		bronze.crm_prd_info;

	TRUNCATE TABLE silver.crm_sales_details;

	INSERT INTO silver.crm_sales_details(
		sls_ord_num ,
		sls_prd_key ,
		sls_cust_id ,
		sls_order_dt ,
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
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) <> 8 THEN NULL --CHecking if date is 0 or not matching date length 
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)   --Changing the data from an integer to a date
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) <> 8 THEN NULL --CHecking if date is 0 or not matching date length 
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) --Changing the data from an integer to a date
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) <> 8 THEN NULL --Checking if date is 0 or not matching date length 
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) --Changing the data from an integer to a date
		END AS sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales <> sls_quantity * ABS(sls_price) --Checking if sales are null a negative or not equla to price*quant
			THEN sls_quantity * ABS(sls_price) --applying the price*qunat formula to determine sales
			ELSE sls_sales
		END AS  sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <= 0  --Checking if price is null or negative
			THEN sls_sales / NULLIF(sls_quantity,0) --Reversing the previuos clculation to determine the price
			ELSE sls_price
		END AS sls_price
	FROM bronze.crm_sales_details;

	TRUNCATE TABLE silver.erp_cust_az12;

	INSERT INTO silver.erp_cust_az12(
		cid,
		bdate,
		gen
	)
	SELECT 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) --Getting rid of NAS in customer id to match other tables
			ELSE cid
		END cid,
		CASE WHEN bdate > GETDATE() THEN NULL   --Getting rid of impossible birht dates
			ELSE bdate
		END AS bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F' ,'FEMALE')  THEN 'Female'   --Normalizing the values in column
			 WHEN UPPER(TRIM(gen)) IN ('M' ,'MALE')  THEN 'Male'  
			 ELSE 'N/A'
		END AS gen
	FROM
		bronze.erp_cust_az12;

	TRUNCATE TABLE silver.erp_loc_a101;

	INSERT INTO silver.erp_loc_a101(
		cid,
		cntry
	)
	SELECT
		REPLACE(cid,'-','') as cid,   --Getting rid of dash in customer id in order to match other tables
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN(cntry) IN ('US','USA') THEN 'United States'    --Normalizing country codes
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END as cntry
	FROM 
		bronze.erp_loc_a101;

	TRUNCATE TABLE silver.erp_px_cat_g1v2
	INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance
	)
	SELECT 
		id,
		cat,
		subcat,
		maintenance
	FROM 
		bronze.erp_px_cat_g1v2
END TRY 
BEGIN CATCH 
PRINT 'ERRORS'
END CATCH 
END
