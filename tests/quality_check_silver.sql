/*
=========================================================================================
Quality Checks
=========================================================================================
Script Purpose:
  This script performs various quality checks for data consistency, accuracy,
  and standardization across the 'silver' schema. It includes checks for: 
  - Null or duplicate primary keys. 
  - Unwarfed spaces in string fields.
  - Datalsandardization and consistency. 
  - Invalid date ranges and orders. 
  - Data consistency between related fields.

Usage Notes: 
  - Run these checks after data loading Silver Layer. 
  - Investigate and resolve any discrepancies found during the checks.
=========================================================================================
*/

--=======================================================================================
-- Checking 'silver.crm_cust_info' 
--=======================================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results

SELECT
cst_id,
COUNT(*)
FROM silver.crm_crust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- Check for Unwanted Spaces
-- Expectation: No Result
SELECT
cst_firstname
FROM silver.crm_crust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT
cst_lastname
FROM silver.crm_crust_info
WHERE cst_lastname != TRIM(cst_lastname)

SELECT
cst_gndr
FROM silver.crm_crust_info
WHERE cst_gndr != TRIM(cst_gndr)

-- data standardzation & consistency

SELECT DISTINCT cst_gndr
FROM silver.crm_crust_info

SELECT * FROM silver.crm_crust_info

---------------------------- crm_prd_info ---------------------------------------

-- Check For Nulls or Duplicates in Primary Key 
-- Expectation: No Result

SELECT
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1

----------------------------------------------------

SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN
(SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2)

---------------------------------------------------------

SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) NOT IN 

(SELECT sls_prd_key FROM bronze.crm_sales_details)

-- Check for Unwanted Spaces
-- Expectation: No Result

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for NULLs or Negative numbers
-- Expected: no Results

SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- data standardzation & consistency

SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

-- Check for Invalid Data Orders
SELECT
*
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt
----------------------------------------------------------------------------------

SELECT
	prd_id,
	prd_key,
	prd_nm,
	prd_start_dt,
	prd_end_dt,
	LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_tast
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')

-- Data quality check for silver table

SELECT
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- Check for Unwanted Spaces
-- Expectation: No Result

SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for NULLs or Negative numbers
-- Expected: no Results

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- data standardzation & consistency

SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- Check for Invalid Data Orders
SELECT
*
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

SELECT * FROM silver.crm_prd_info

-------------------------------------------------------------------------------

SELECT
NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101

SELECT
NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0
OR LEN(sls_ship_dt) != 8
OR sls_ship_dt > 20500101
OR sls_ship_dt < 19000101

SELECT
NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0
OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101

-- checking for invalid date order

SELECT
*
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
OR sls_order_dt > sls_due_dt

--Check Data Consistency: Between Sales, Quantity, and Price 
-- >> Sales = Quantity * Price 
-- >> Values must not be NULL, zero, or negative.

SELECT DISTINCT
	sls_sales AS old_sls_sales,
	sls_quantity,
	sls_price AS old_sls_price,
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,

	CASE WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales / NULLIF (sls_quantity, 0)
	ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0 
ORDER BY sls_sales, sls_quantity, sls_price


-- checking for invalid date order

SELECT
*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
OR sls_order_dt > sls_due_dt

SELECT DISTINCT
	sls_sales,
	sls_quantity,
	sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0 
ORDER BY sls_sales, sls_quantity, sls_price

SELECT * FROM silver.crm_sales_details

----------------------------------------------------------------------------------

SELECT DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- Date Standardization & consistency

SELECT DISTINCT
    gen,
    CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Felame'
         WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen_label
FROM bronze.erp_cust_az12;

-- CHECKING

-- Identify Out-of-Range Dates
SELECT DISTINCT 
bdate 
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' 
OR bdate > GETDATE()

-- Date Standardization & consistency

SELECT DISTINCT
gen
FROM silver.erp_cust_az12

----------------------------------------------------------------------------------

SELECT DISTINCT
	cntry,
	CASE WHEN TRIM(cntry) = 'DE' THEN  'Germany'
		 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		 ELSE TRIM(cntry)
	END AS cntry

FROM bronze.erp_loc_a101

-------------------------------------------------------------------------------------
SELECT
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2
WHERE TRIM(cat) != cat
OR TRIM(subcat) != subcat
OR TRIM(maintenance) != maintenance

SELECT DISTINCT
cat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT
subcat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT
maintenance
FROM bronze.erp_px_cat_g1v2
