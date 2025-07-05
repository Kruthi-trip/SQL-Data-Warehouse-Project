-- check for nulls or duplicates in primary key


SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING count(*) > 1 OR cst_id IS NULL


-- check for unwanted spaces

SELECT cst_key
FROM bronze.crm_cust_info
WHERE cst_key != TRIM(cst_key);



-- Data Standardization & Consistency
SELECT DISTINCT 
    cst_marital_status 
FROM silver.crm_cust_info;

