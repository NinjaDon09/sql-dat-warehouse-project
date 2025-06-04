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
CREATE OR ALTER PROCEDURE Silver.Load_Silver
As
begin
Declare @start_time DATETIME, @end_time DATETIME,@Batch_start_time DATETIME,@Batch_end_time DATETIME;
Begin Try
	PRINT '========================================================================================';
	PRINT 'Loading Silver Layer';
	PRINT '========================================================================================';

	PRINT '----------------------------------------------------------------------------------------';
	PRINT 'LOADING CRM TABLES';		
	PRINT '-----------------------------------------------------------------------------------------';

	Set @start_time = Getdate();
PRINT '>> TRUNCATING TABLE:[Silver].[crm_cust_info]';
TRUNCATE TABLE [Silver].[crm_cust_info];
PRINT '>> Inserting Data Into : [Silver].[crm_cust_info]';	
insert into [Silver].[crm_cust_info](
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
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname,
case when UPPER(TRIM(cst_marital_status))='M' then 'Married'
		when UPPER(TRIM(cst_marital_status)) ='S' then 'Single'
else 'n/a'
end cst_marital_status,
case when UPPER(TRIM(cst_gndr))='M' then 'Male'
		when UPPER(TRIM(cst_gndr)) ='F' then 'Female'
else 'n/a'
end cst_gndr,
cst_create_date
FROM(
	SELECT
	*,
	Row_NUMBER () OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as Flag_type
	from [Bronze].[crm_cust_info]
	where cst_id IS NOT NULL)t
		where flag_type=1;
set @end_time =  Getdate(); 
	PRINT' >> LOAD DURATION: '+cast(DATEDIFF(second,@start_time,@end_time) as NVARCHAR(20))+ 'seconds';
	PRINT '------------------';

Set @start_time = Getdate();
PRINT '>> TRUNCATING TABLE:[Silver].[crm_prd_info] ';
TRUNCATE TABLE [Silver].[crm_prd_info];
PRINT '>> Inserting Data Into : [Silver].[crm_prd_info]';	
Insert into silver.crm_prd_info (
	prd_id ,
	prd_key ,
	prd_nm ,
	prd_cost,
	prd_line ,
	prd_start_dt ,
	prd_end_dt
	)
select
prd_id,
SUBSTRING (prd_key,7,LEN(prd_key)) as prd_key,
prd_nm,
ISNULL(prd_cost,0) As Prd_cost,
CASE UPPER (TRIM(prd_line))
		when 'M' then 'Mountain'
		when 'R' then 'Road'
		when 'S' then 'Other sales'
		when 'T' then 'Touring'
		else 'N/A'
		end as Prd_line,
cast(prd_start_dt as DATE) as prd_start_dt,
cast(Lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 AS DATE) as prd_end_dt 
from Bronze.crm_prd_info;
set @end_time = getdate(); 

PRINT' >> LOAD DURATION: '+cast(DATEDIFF(second,@start_time,@end_time) as NVARCHAR(20))+ 'seconds';
PRINT '------------------';

Set @start_time = Getdate();
PRINT '>> TRUNCATING TABLE:[Silver].[crm_sales_details] ';
TRUNCATE TABLE [Silver].[crm_sales_details];
PRINT '>> Inserting Data Into : [Silver].[crm_sales_details]';	

INSERT INTO [Silver].[crm_sales_details](
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
select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR  LEN(sls_order_dt)!=8 then NULL
		ELSE cast(cast(sls_order_dt as Varchar(20)) as DATE) 
		END sls_order_dt,

CASE WHEN sls_ship_dt = 0 OR  LEN(sls_ship_dt)!=8 then NULL
		ELSE cast(cast(sls_ship_dt as Varchar(20)) as DATE) 
		END sls_ship_dt,

CASE WHEN sls_due_dt = 0 OR  LEN(sls_due_dt)!=8 then NULL
		ELSE cast(cast(sls_due_dt as Varchar(20)) as DATE) 
		END sls_due_dt,

CASE WHEN sls_sales IS NULL OR sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price)
	THEN sls_quantity * ABS(sls_price)
Else sls_sales 
END AS sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <=0 
	THEN sls_sales/ NULLIF(sls_quantity,0)
	ELSE sls_price
	END AS sls_price
from [Bronze].[crm_sales_details];

set @end_time = Getdate(); 
	PRINT '----------------------------------------------------------------------------------------';
	PRINT 'LOADING ERP TABLES';		
	PRINT '-----------------------------------------------------------------------------------------';
PRINT' >> LOAD DURATION: '+cast(DATEDIFF(second,@start_time,@end_time) as NVARCHAR(20))+ 'seconds';
	PRINT '------------------';

Set @start_time = Getdate();
PRINT '>> TRUNCATING TABLE:[Silver].[erp_cust_az12] ';
TRUNCATE TABLE [Silver].[erp_cust_az12];
PRINT '>> Inserting Data Into : [Silver].[erp_cust_az12]';	

INSERT INTO silver.erp_cust_az12 (
cid,
bdate,
gen
)
SELECT
CASE
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
	ELSE cid
END AS cid, 
CASE
	WHEN bdate > GETDATE() THEN NULL
	ELSE bdate
END AS bdate, -- Set future birthdates to NULL
CASE
	WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	ELSE 'n/a'
END AS gen -- Normalize gender values and handle unknown cases
FROM bronze.erp_cust_az12;
set @end_time =Getdate(); 
PRINT' >> LOAD DURATION: '+ cast(DATEDIFF(second,@start_time,@end_time) as NVARCHAR(20))+ 'seconds';
	PRINT '------------------';

Set @start_time = Getdate();
PRINT '>> TRUNCATING TABLE:[Silver].[erp_loc_a101] ';
TRUNCATE TABLE [Silver].[erp_loc_a101];
PRINT '>> Inserting Data Into : [Silver].[erp_loc_a101]';	

INSERT INTO  [Silver].[erp_loc_a101] (cid,cntry)
select
replace(cid,'-','') as cid,
case when TRIM(cntry)='DE' then 'Denamrk' 
		when TRIM(cntry) IN ('US','USA') then 'United states'
		when TRIM(cntry) =' ' and TRIM(cntry) is null then  'N/A'
else TRIM(cntry)
end as cntry
from [Bronze].[erp_loc_a101]
set @end_time = Getdate(); 
PRINT' >> LOAD DURATION: '+cast(DATEDIFF(second,@start_time,@end_time) as NVARCHAR(20))+ 'seconds';
	PRINT '------------------';

Set @start_time = Getdate();
PRINT '>> TRUNCATING TABLE:[Silver].[erp_px_cat_g1v2] ';
TRUNCATE TABLE [Silver].[erp_px_cat_g1v2];
PRINT '>> Inserting Data Into : [Silver].[erp_px_cat_g1v2]';	

insert into [Silver].[erp_px_cat_g1v2]
(id,cat,subcat,maintenance)
select 
id,
cat,
subcat,
maintenance
from [Bronze].[erp_px_cat_g1v2]
set @end_time = Getdate(); 
PRINT' >> LOAD DURATION: '+cast(DATEDIFF(second,@start_time,@end_time) as NVARCHAR(20))+ 'seconds';
	PRINT '------------------';

	SET @batch_end_time= Getdate();
	PRINT '=================================================================================';
	PRINT 'loading Silver layer is completed';
	PRINT '  -Total Load Duration: ' + cast(DATEDIFF(Second,@Batch_start_time,@Batch_end_time) as Nvarchar(20))+'Seconds';
	PRINT '=================================================================================';
	End try
	Begin catch
	PRINT '=========================================================================';
	PRINT 'ERROR OCCURED DURING LAODING SILVER LAYER';
	PRINT 'ERROR MESSAGE'+ ERROR_MESSAGE();
	PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR(20));
	PRINT 'ERROR MESSAGE'+ CAST (ERROR_STATE() AS NVARCHAR(20));
	PRINT '=========================================================================';
	end catch
END;

EXEC Silver.load_Silver
