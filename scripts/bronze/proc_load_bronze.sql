/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
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
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze as 
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME,  @batch_start_time DATETIME, @batch_end_time DATETIME;
BEGIN TRY
set @batch_start_time = GETDATE();
PRINT '==============================================';
PRINT 'Loading Bronze Layer';
PRINT '==============================================';

PRINT '----------------------------------------------';
PRINT 'Loading CRM Tables';
PRINT '----------------------------------------------';

SET @start_time = GETDATE();
PRINT '>> Truncating Table: bronze.crm_cust_info';
TRUNCATE TABLE bronze.crm_cust_info;

PRINT '>> Inserting Data Into: bronze.crm_cust_info';
bulk insert bronze.crm_cust_info
from 'C:\Users\q\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
with (
    FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
SET @end_time = GETDATE();
PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS  NVARCHAR) + ' seconds';
PRINT '>>----------------------------------------------------------';



SET @start_time = GETDATE();
PRINT '>> Truncating Table: bronze.crm_prd_info';
TRUNCATE TABLE bronze.crm_prd_info;
PRINT '>> Inserting Data Into: bronze.crm_prd_info';
bulk insert bronze.crm_prd_info
from 'C:\Users\q\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
with (
    FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
SET @end_time = GETDATE();
PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS  NVARCHAR) + ' seconds';
PRINT '>>----------------------------------------------------------';


SET @start_time = GETDATE();
PRINT '>> Truncating Table: bronze.crm_sales_details';
TRUNCATE TABLE bronze.crm_sales_details;

PRINT '>> Inserting Data Into: bronze.crm_sales_details';
bulk insert bronze.crm_sales_details
from 'C:\Users\q\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
with (
    FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
SET @end_time = GETDATE();
PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS  NVARCHAR) + ' seconds';
PRINT '>>----------------------------------------------------------';


PRINT '----------------------------------------------';
PRINT 'Loading ERP Tables';
PRINT '----------------------------------------------';

SET @start_time = GETDATE();
PRINT '>> Truncating Data Into: bronze.erp_loc_a101';
TRUNCATE TABLE bronze.erp_loc_a101;

PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
bulk insert bronze.erp_loc_a101
from 'C:\Users\q\Desktop\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
with (
    FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
SET @end_time = GETDATE();
PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS  NVARCHAR) + ' seconds';
PRINT '>>----------------------------------------------------------';


SET @start_time = GETDATE();
PRINT '>> Truncating Data Into: bronze.erp_cust_az12';
TRUNCATE TABLE bronze.erp_cust_az12;
PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
bulk insert bronze.erp_cust_az12
from 'C:\Users\q\Desktop\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
with (
    FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
SET @end_time = GETDATE();
PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS  NVARCHAR) + ' seconds';
PRINT '>>----------------------------------------------------------';


SET @start_time = GETDATE();
PRINT '>> Truncating Data Into: bronze.erp_px_cat_g1v2';
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
bulk insert bronze.erp_px_cat_g1v2
from 'C:\Users\q\Desktop\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
with (
    FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
SET @end_time = GETDATE();
PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS  NVARCHAR) + ' seconds';
PRINT '>>----------------------------------------------------------';

SET @batch_end_time = GETDATE();
PRINT '>>----------------------------------------------------------';
PRINT 'Loading Bronze Layer is completed';
PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS  NVARCHAR) + ' seconds';
PRINT '>>----------------------------------------------------------';

  END TRY
  BEGIN CATCH
     PRINT '=========================================='
	 PRINT 'Error Occured During LOading Bronze Layer'
	 PRINT 'Error Message' + ERROR_MESSAGE();
	 PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
	 PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
	 PRINT '=========================================='
  END CATCH
END
