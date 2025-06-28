/*
============================================================================================
DDL Script: Create Gold Views
============================================================================================
Script Purpose:
    THis script creates views for the gold layer in the data warehouse.
    THe gold layer represents the final dimension and fact tables(Star Schema)
    Each view performs transformations and combines data from the silver layer
    to produce a clean and business ready dataset.
============================================================================================
*/


--joining three tables
SELECT 
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_marital_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON    ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON    ci.cst_key = la.cid

--checking for duplicate customer IDs (cst_id) after joining multiple tables
SELECT cst_id, COUNT(*) FROM
	(SELECT 
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_marital_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
	ON    ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 la
	ON    ci.cst_key = la.cid
)t GROUP BY cst_id
HAVING COUNT(*) > 1
--result: none, which means no duplicates after joing the three tables

--data integration
--there are two columns for gender
SELECT 
		ci.cst_gndr,
		ca.gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON    ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON    ci.cst_key = la.cid
ORDER BY 1,2
--result: some are not matching each other
--fix them
SELECT 
		ci.cst_gndr,
		ca.gen,
		CASE WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr --CRM is the Master for gender Info
			ELSE COALESCE(ca.gen, 'Unknown')
		END AS new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON    ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON    ci.cst_key = la.cid
ORDER BY 1,2

--combine it to the whole query
SELECT 
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_marital_status,
		CASE WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr --CRM is the Master for gender Info
			ELSE COALESCE(ca.gen, 'Unknown')
		END AS new_gen,
		ci.cst_create_date,
		ca.bdate,
		la.cntry
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON    ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON    ci.cst_key = la.cid

--Rename columns to friendly, meaningful names
SELECT 
		ci.cst_id AS customer_id,
		ci.cst_key AS customer_number,
		ci.cst_firstname AS first_name,
		ci.cst_lastname AS last_name,
		la.cntry AS country,
		ci.cst_marital_status AS marital_status,
		CASE WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr --CRM is the Master for gender Info
			ELSE COALESCE(ca.gen, 'Unknown')
		END AS gender,
		ca.bdate AS birthdate,
		ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON    ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON    ci.cst_key = la.cid

--make customer_key
SELECT 
		ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
		ci.cst_id AS customer_id,
		ci.cst_key AS customer_number,
		ci.cst_firstname AS first_name,
		ci.cst_lastname AS last_name,
		la.cntry AS country,
		ci.cst_marital_status AS marital_status,
		CASE WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr --CRM is the Master for gender Info
			ELSE COALESCE(ca.gen, 'Unknown')
		END AS gender,
		ca.bdate AS birthdate,
		ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON    ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON    ci.cst_key = la.cid

--make object
CREATE VIEW gold.dim_customers AS
SELECT 
		ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
		ci.cst_id AS customer_id,
		ci.cst_key AS customer_number,
		ci.cst_firstname AS first_name,
		ci.cst_lastname AS last_name,
		la.cntry AS country,
		ci.cst_marital_status AS marital_status,
		CASE WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr --CRM is the Master for gender Info
			ELSE COALESCE(ca.gen, 'Unknown')
		END AS gender,
		ca.bdate AS birthdate,
		ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON    ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON    ci.cst_key = la.cid

--quality check of the gold table
SELECT * FROM gold.dim_customers

SELECT DISTINCT gender FROM gold.dim_customers

---------------------------------------------------------------
--Second Object
---------------------------------------------------------------
SELECT 
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pn.prd_end_dt
FROM silver.crm_prd_info pn

--pick the pre_end_dt is NULL which is currently processing(is latest)
SELECT 
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pn.prd_end_dt
FROM silver.crm_prd_info pn
WHERE prd_end_dt IS NULL --filter out all historical data

--joining the tables
SELECT 
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pn.prd_end_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL

--check duplicates for prd_key
SELECT prd_key, COUNT(*) FROM (
SELECT 
	pn.prd_id,
	pn.prd_key,
	pn.prd_nm,
	pn.cat_id,
	pc.cat,
	pc.subcat,
	pc.maintenance,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pn.prd_end_dt
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL
)t GROUP BY prd_key
HAVING COUNT(*) >1
--no duplicate

--give friendly names
SELECT 
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL

--make primary key
SELECT 
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL

--creat view
CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL

----------------------------------------------------------------
--create fact scales
----------------------------------------------------------------
SELECT
sd.sls_ord_num,
sd.sls_prd_key,
sd.sls_cust_id,
sd.sls_order_dt,
sd.sls_ship_dt,
sd.sls_due_dt,
sd.sls_sales,
sd.sls_quantity,
sd.sls_price,
FROM silver.crm_sales_details sd

--get surrogate keys by joining( make fact table)
SELECT
sd.sls_ord_num,
pr.product_key,
cu.customer_key,
sd.sls_order_dt,
sd.sls_ship_dt,
sd.sls_due_dt,
sd.sls_sales,
sd.sls_quantity,
sd.sls_price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id

--give friendly names
SELECT
sd.sls_ord_num AS order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id


--create vies
CREATE VIEW gold.fact_sales AS
SELECT
sd.sls_ord_num,
pr.product_key,
cu.customer_key,
sd.sls_order_dt,
sd.sls_ship_dt,
sd.sls_due_dt,
sd.sls_sales,
sd.sls_quantity,
sd.sls_price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id




--Foreign key integraity(dimensions)
SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
WHERE c.customer_key IS NULL


SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL





