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
EXEC silver.load_silver;
GO
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		-- Loading silver.crm_cust_info
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		Insert into Silver.crm_cust_info(
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date)


        select cst_id ,
        cst_key,
        TRIM(cst_firstname) as cst_firstname,
        TRIM(cst_lastname)as cst_lastname,
        CASE WHEN UPPER(TRIM(cst_marital_status))='M' then 'Married'
            WHEN UPPER(TRIM(cst_marital_status))='S' then 'Single'
            else 'n/a'
        END cst_marital_status,
        CASE WHEN UPPER(TRIM(cst_gndr))='M' then 'Male'
            WHEN UPPER(TRIM(cst_gndr))='F' then 'Female'
            else 'n/a'
        END cst_gndr,
        cst_create_date 
        from(select *,
        ROW_NUMBER() OVER(partition by cst_id order by cst_create_date desc) as flag_last
        from Bronze.crm_cust_info where cst_id IS NOT NULL)t where 
        flag_last=1; -- Select the most recent record per customer
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.crm_prd_info
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
		INSERT into Silver.crm_prd_info(
                    prd_id ,
                    cat_id ,
                    prd_key ,
                    prd_nm ,
                    prd_cost ,
                    prd_line,
                    prd_start_dt ,
                    prd_end_date 
                    )
SELECT prd_id,
Replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key, 
prd_nm,
ISNULL(prd_cost,0) as prd_cost,
case UPPER(TRIM(prd_line))
     when 'M' then 'Mountain'
     when 'R' then 'Road'
     when 'S' then 'other sales'
     else 'n/a'
END prd_line,
CAST(prd_start_dt as date) as prd_start_dt,
CAST(LEAD(prd_start_dt) OVER(partition by prd_key order by prd_start_dt )-1 as date ) as prd_end_date
from Bronze.crm_prd_info
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading crm_sales_details
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		INSERT into Silver.crm_sales_details(
                    sls_ord_num ,
                    sls_prd_key,
                    sls_cust_id ,
                    sls_order_dt ,
                    sls_ship_dt ,
                    sls_due_dt ,
                    sls_sales ,
                    sls_quantity ,
                    sls_price 
                    )
        select 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE When sls_order_dt=0 or LEN(sls_order_dt)!=8 THEN  NULL 
            else CAST(CAST(sls_order_dt AS varchar)AS DATE)
        END sls_order_dt,
        CASE when sls_ship_dt =0  or LEN(sls_ship_dt)!=8 THEN  NULL 
            else CAST(CAST(sls_ship_dt as varchar)as DATE)
            END sls_ship_dt,
        CASE when sls_due_dt =0  or LEN(sls_due_dt)!=8 THEN  NULL 
            else CAST(CAST(sls_due_dt as varchar)as DATE)
            END sls_due_dt,
        CASE when sls_sales is NULL or sls_sales<=0 or sls_sales!=sls_quantity* ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                else sls_sales
        END sls_sales,   
        sls_quantity,
        CASE  when sls_price is NULL or sls_price<=0
            THEN sls_sales/NULLIF(sls_quantity,0)
            else sls_price
        END sls_price
        from Bronze.crm_sales_details; 	
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading erp_cust_az12
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
		INSERT into Silver.erp_cust_az12(cid,bdate,gen)
        select 
        CASE when cid like 'NAS%' then SUBSTRING(cid,4,LEN(cid))
            else cid
        END  as cid,
        CASE when bdate > GETDATE() then NULL
            else bdate
        END as bdate,
        CASE when  REPLACE(REPLACE(TRIM(gen), CHAR(13), ''), CHAR(10), '') IN ('F','FEMALE') then 'FEMALE'
            when   REPLACE(REPLACE(TRIM(gen), CHAR(13), ''), CHAR(10), '')IN ('M','MALE') then 'MALE'
            else 'n/a'
        END as gen       
        from Bronze.erp_cust_az12;
	    SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

        -- Loading erp_loc_a101
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		Insert into Silver.erp_loc_a101(cid,cntry)
        select 
        REPLACE(cid,'-','') as cid,
        CASE when REPLACE(REPLACE(trim(cntry),char(10),' '),char(13),'') = 'DE' then 'Germany'
            when REPLACE(REPLACE(trim(cntry),char(10),' '),char(13),'') in ('US','USA') then 'United States'
            when REPLACE(REPLACE(trim(cntry),char(10),' '),char(13),'')  = '' then 'n/a'
            else REPLACE(REPLACE(trim(cntry),char(10),' '),char(13),'')
        END as cntry
        from bronze.erp_loc_a101;
	    SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
		
		-- Loading erp_px_cat_g1v2
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		Insert into Silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
        select id,
        cat,
        subcat,
        Replace(Replace(TRIM(maintenance),char(13),''),char(10),'') as maintenance FROM
        Bronze.erp_px_cat_g1v2

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
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
