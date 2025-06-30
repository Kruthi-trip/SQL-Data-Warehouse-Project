/*
===================================================================
  Script purpose:
    In this script we are 
    1. creating a new Datawarehouse
    2. Creating Schemas
    3. Creating Tables in the bronze schema
    4. Creating a stored procedure to load bulk data into the bronze tables.
===================================================================
*/

-- Selecting the master data
USE master;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

-- selecting the newly created datawarehouse
USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

==================================================================

-- creating tables

CREATE TABLE bronze.crm_cust_info 
(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_material_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
);

-------------------------------------------

CREATE TABLE bronze.crm_prd_info 
(
	prd_id INT,
	prd_key	NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME
);

-------------------------------------------

CREATE TABLE bronze.crm_sales_details
(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);

-------------------------------------------

CREATE TABLE bronze.erp_cust_az12
(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50)
);

-------------------------------------------

CREATE TABLE bronze.erp_loc_a101
(
	cid NVARCHAR(50),
	cntry NVARCHAR(50)
);

-------------------------------------------

CREATE TABLE bronze.erp_px_cat_g1v2
(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50)
);

-------------------------------------------

-- to check the current database
SELECT DB_NAME() AS current_database;

USE DataWarehouse;
GO

-------------------------------------------

============================================================================

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
