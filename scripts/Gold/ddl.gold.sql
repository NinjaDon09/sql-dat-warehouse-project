/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customer  as
SELECT
     ROW_NUMBER() OVER (ORDER BY cst_id ) as customer_key,
	cst_id as customer_id,
	cst_key as customer_number,
	cst_firstname as first_name,
	cst_lastname as last_name,
	cntry as country,
	cst_marital_status as marital_status,
	CASE when ci.cst_gndr !='n/a' then ci.cst_gndr-----CRM is the master for gendr info
      else coalesce(ca.gen,'n/a')
	  end gender,
	  bdate as Birthdate,
	cst_create_date as create_date
from [Silver].[crm_cust_info] ci
left join [Silver].[erp_cust_az12] ca
on ci.cst_key = ca.cid 
LEFT JOIN [Silver].[erp_loc_a101] la
on ci.cst_key= la.cid;
go;


-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
CREATE VIEW Gold.Dim_Products As
	SELECT
	ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt,pn.prd_key) as Product_key,
	pn.prd_id as Product_id,
	pn.prd_key as Product_number,
	pn.prd_nm as Product_name,
	pn.cat_id as Category_id,
	pc.cat as Category,
	pc.subcat as Subcategory,
	pc.maintenance,
	pn.prd_cost as Cost,
	pn.prd_line as Line ,
	pn.prd_start_dt as Start_date
	from [Silver].[crm_prd_info] pn
	Left join [Silver].[erp_px_cat_g1v2] pc
	on pn.cat_id=pc.id
	where prd_end_dt is Null
GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
CREATE VIEW gold.fact_sales as
select 
sls_ord_num as order_number ,
pr.Product_key,
cd.customer_key ,
sls_order_dt as order_date,
sls_ship_dt as shipping_date,
sls_due_dt as due_date,
sls_sales as sales_amount,
sls_quantity as quantity,
sls_price as price
from [Silver].[crm_sales_details] sd 
LEFT JOIN [Gold].[Dim_Products] pr
on sd.sls_prd_key = pr.product_number
LEFT JOIN [Gold].[dim_customer] cd
on sd.sls_cust_id = cd.customer_id

