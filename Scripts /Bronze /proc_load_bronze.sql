/*
============================================================================
Stored Procedure

Script Purpose:
  This code is to load data into the bronze schema from csv files.
  This code
    - Truncates(empties) the bronze tables before loading.
    - Used the BULK INSERT command to load all the data from the csv files to the bronze tables at once.
============================================================================
*/

-- creating a stored procedure to load bult data

EXEC bronze.load_bronze

-- Creating a stored procedure
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	PRINT('=========================================================');
	PRINT('Loading Bronze Layer');
	PRINT('=========================================================');

	PRINT('---------------------------------------------------------');
	PRINT('Loading CRM Tables');
	PRINT('---------------------------------------------------------');

	-- BULK loading data to tables

	-- emptying the table before loading it
	TRUNCATE TABLE bronze.crm_cust_info;

	BULK INSERT bronze.crm_cust_info
	FROM 'C:\Users\srikr\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	WITH 
	(
	FIRSTROW = 2,
	-- THIS MEANS THE DATA STARTS FROM ROW 2, AND WE ARE ASKING TO SKIP THE FIRST ROW WHICH IS HEADERS
	FIELDTERMINATOR = ',',
	TABLOCK
	);

	-- SELECT COUNT(*) AS 'No. of Rows' FROM bronze.crm_cust_info

	-------------------------------------------

	TRUNCATE TABLE bronze.crm_prd_info;

	BULK INSERT bronze.crm_prd_info
	FROM 'C:\Users\srikr\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH 
	(
	FIRSTROW = 2,
	-- THIS MEANS THE DATA STARTS FROM ROW 2, AND WE ARE ASKING TO SKIP THE FIRST ROW WHICH IS HEADERS
	FIELDTERMINATOR = ',',
	TABLOCK
	);

	-- SELECT COUNT(*) AS 'No. of Rows' FROM bronze.crm_prd_info

	-------------------------------------------

	TRUNCATE TABLE bronze.crm_sales_details;

	BULK INSERT bronze.crm_sales_details
	FROM 'C:\Users\srikr\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH 
	(
	FIRSTROW = 2,
	-- THIS MEANS THE DATA STARTS FROM ROW 2, AND WE ARE ASKING TO SKIP THE FIRST ROW WHICH IS HEADERS
	FIELDTERMINATOR = ',',
	TABLOCK
	);

	-- SELECT COUNT(*) AS 'No. of Rows' FROM bronze.crm_sales_details

	-------------------------------------------

	PRINT('---------------------------------------------------------');
	PRINT('Loading ERP Tables');
	PRINT('---------------------------------------------------------');

	TRUNCATE TABLE bronze.erp_cust_az12;

	BULK INSERT bronze.erp_cust_az12
	FROM 'C:\Users\srikr\Desktop\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
	WITH 
	(
	FIRSTROW = 2,
	-- THIS MEANS THE DATA STARTS FROM ROW 2, AND WE ARE ASKING TO SKIP THE FIRST ROW WHICH IS HEADERS
	FIELDTERMINATOR = ',',
	TABLOCK
	);

	-- SELECT COUNT(*) AS 'No. of Rows' FROM bronze.erp_cust_az12

	-------------------------------------------

	TRUNCATE TABLE bronze.erp_loc_a101;

	BULK INSERT bronze.erp_loc_a101
	FROM 'C:\Users\srikr\Desktop\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
	WITH 
	(
	FIRSTROW = 2,
	-- THIS MEANS THE DATA STARTS FROM ROW 2, AND WE ARE ASKING TO SKIP THE FIRST ROW WHICH IS HEADERS
	FIELDTERMINATOR = ',',
	TABLOCK
	);

	-- SELECT COUNT(*) AS 'No. of Rows' FROM bronze.erp_loc_a101

	-------------------------------------------

	TRUNCATE TABLE bronze.erp_px_cat_g1v2;

	BULK INSERT bronze.erp_px_cat_g1v2
	FROM 'C:\Users\srikr\Desktop\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
	WITH 
	(
	FIRSTROW = 2,
	-- THIS MEANS THE DATA STARTS FROM ROW 2, AND WE ARE ASKING TO SKIP THE FIRST ROW WHICH IS HEADERS
	FIELDTERMINATOR = ',',
	TABLOCK
	);

	-- SELECT COUNT(*) AS 'No. of Rows' FROM bronze.erp_px_cat_g1v2

END

-------------------------------------------

===================================================================================
