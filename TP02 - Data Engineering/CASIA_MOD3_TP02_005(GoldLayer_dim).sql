-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Loading the Dim tables in the Gold layer 
-- MAGIC ## Connecting to the Gold layer (Target)

-- COMMAND ----------

USE CATALOG ngow_lakehouse;
USE SCHEMA gold;

DECLARE OR REPLACE load_date = current_timestamp();
VALUES load_date;

-- COMMAND ----------

MERGE INTO gold.dim_geography AS tgt
USING (
    SELECT
        CAST(address_id AS INT) AS geo_address_id,
        COALESCE(TRY_CAST(address_line1 AS STRING), 'N/A') AS geo_address_line_1,
        COALESCE(TRY_CAST(address_line2 AS STRING), 'N/A') AS geo_address_line_2,
        COALESCE(TRY_CAST(city AS STRING), 'N/A') AS geo_city,
        COALESCE(TRY_CAST(state_province AS STRING), 'N/A') AS geo_state_province,
        COALESCE(TRY_CAST(country_region AS STRING), 'N/A') AS geo_country_region,
        COALESCE(TRY_CAST(postal_code AS STRING), 'N/A') AS geo_postal_code
    FROM silver.address
    WHERE _tf_valid_to IS NULL
) AS src
ON tgt.geo_address_id = src.geo_address_id

-- 1) Update existing records when a difference is detected
WHEN MATCHED AND (
    tgt.geo_address_line_1 != src.geo_address_line_1 OR 
    tgt.geo_address_line_2 != src.geo_address_line_2 OR 
    tgt.geo_city != src.geo_city OR
    tgt.geo_state_province != src.geo_state_province OR
    tgt.geo_country_region != src.geo_country_region OR
    tgt.geo_postal_code != src.geo_postal_code
) THEN 
  
  UPDATE SET 
    tgt.geo_address_line_1 = src.geo_address_line_1,
    tgt.geo_address_line_2 = src.geo_address_line_2,
    tgt.geo_city = src.geo_city,
    tgt.geo_state_province = src.geo_state_province,
    tgt.geo_country_region = src.geo_country_region,
    tgt.geo_postal_code = src.geo_postal_code,
    tgt._tf_update_date = load_date

-- 2) Insert new records
WHEN NOT MATCHED THEN
  
  INSERT (
    geo_address_id,
    geo_address_line_1,
    geo_address_line_2,
    geo_city,
    geo_state_province,
    geo_country_region,
    geo_postal_code,
    _tf_create_date,
    _tf_update_date
  )
  VALUES (
    src.geo_address_id,
    src.geo_address_line_1,
    src.geo_address_line_2,
    src.geo_city,
    src.geo_state_province,
    src.geo_country_region,
    src.geo_postal_code,
    load_date,        -- _tf_create_date
    load_date         -- _tf_update_date
  )

-- COMMAND ----------

MERGE INTO gold.dim_customer AS tgt
USING (
    SELECT
        CAST(customer_id AS INT) AS cust_customer_id,
        COALESCE(TRY_CAST(title AS STRING), 'N/A') AS cust_title,
        COALESCE(TRY_CAST(first_name AS STRING), 'N/A') AS cust_first_name,
        COALESCE(TRY_CAST(middle_name AS STRING), 'N/A') AS cust_middle_name,
        COALESCE(TRY_CAST(last_name AS STRING), 'N/A') AS cust_last_name,
        COALESCE(TRY_CAST(suffix AS STRING), 'N/A') AS cust_suffix,
        COALESCE(TRY_CAST(company_name AS STRING), 'N/A') AS cust_company_name,
        COALESCE(TRY_CAST(sales_person AS STRING), 'N/A') AS cust_sales_person,
        COALESCE(TRY_CAST(email_address AS STRING), 'N/A') AS cust_email_address,
        COALESCE(TRY_CAST(phone AS STRING), 'N/A') AS cust_phone
    FROM silver.customer
    WHERE _tf_valid_to IS NULL
) AS src
ON tgt.cust_customer_id = src.cust_customer_id

-- 1) Update existing records when a difference is detected
WHEN MATCHED AND (
    tgt.cust_title != src.cust_title OR
    tgt.cust_first_name != src.cust_first_name OR
    tgt.cust_middle_name != src.cust_middle_name OR
    tgt.cust_last_name != src.cust_last_name OR
    tgt.cust_suffix != src.cust_suffix OR
    tgt.cust_company_name != src.cust_company_name OR
    tgt.cust_sales_person != src.cust_sales_person OR
    tgt.cust_email_address != src.cust_email_address OR
    tgt.cust_phone != src.cust_phone
) THEN 
  
  UPDATE SET 
    tgt.cust_title = src.cust_title,
    tgt.cust_first_name = src.cust_first_name,
    tgt.cust_middle_name = src.cust_middle_name,
    tgt.cust_last_name = src.cust_last_name,
    tgt.cust_suffix = src.cust_suffix,
    tgt.cust_company_name = src.cust_company_name,
    tgt.cust_sales_person = src.cust_sales_person,
    tgt.cust_email_address = src.cust_email_address,
    tgt.cust_phone = src.cust_phone,
    tgt._tf_update_date = load_date

-- 2) Insert new records
WHEN NOT MATCHED THEN
  
  INSERT (
    cust_customer_id,
    cust_title,
    cust_first_name,
    cust_middle_name,
    cust_last_name,
    cust_suffix,
    cust_company_name,
    cust_sales_person,
    cust_email_address,
    cust_phone,
    _tf_create_date,
    _tf_update_date
  )
  VALUES (
    src.cust_customer_id,
    src.cust_title,
    src.cust_first_name,
    src.cust_middle_name,
    src.cust_last_name,
    src.cust_suffix,
    src.cust_company_name,
    src.cust_sales_person,
    src.cust_email_address,
    src.cust_phone,
    load_date,        -- _tf_create_date
    load_date         -- _tf_update_date
  )

-- COMMAND ----------

-- DBTITLE 1,Cell 5
-- 9. Création de la dim_product + exemple de liaison sur la fact_sales
MERGE INTO gold.dim_product AS tgt
USING (
    SELECT
        p.product_id AS prod_product_id,
        COALESCE(p.name, 'N/A') AS prod_name,
        COALESCE(p.product_number, 'N/A') AS prod_product_number,
        COALESCE(p.color, 'N/A') AS prod_color,
        COALESCE(CAST(p.standard_cost AS DOUBLE), 0.0) AS prod_standard_cost,
        COALESCE(CAST(p.list_price AS DOUBLE), 0.0) AS prod_list_price,
        COALESCE(p.size, 'N/A') AS prod_size,
        COALESCE(CAST(p.weight AS DOUBLE), 0.0) AS prod_weight,
        COALESCE(p.product_category_id, -1) AS prod_product_category_id,
        dpc._tf_dim_product_category_id,
        COALESCE(p.product_model_id, -1) AS prod_product_model_id,
        COALESCE(pm.name, 'N/A') AS prod_model_name,
        p.sell_start_date AS prod_sell_start_date,
        p.sell_end_date AS prod_sell_end_date,
        p.discontinued_date AS prod_discontinued_date
    FROM silver.product p
    LEFT JOIN silver.productmodel pm
        ON p.product_model_id = pm.product_model_id
    LEFT JOIN gold.dim_product_category dpc
        ON p.product_category_id = dpc.prod_product_category_id
    WHERE p._tf_valid_to IS NULL
) AS src
ON tgt.prod_product_id = src.prod_product_id

-- 1) Update existing records when a difference is detected
WHEN MATCHED AND (
    tgt.prod_name != src.prod_name OR
    tgt.prod_product_number != src.prod_product_number OR
    tgt.prod_color != src.prod_color OR
    tgt.prod_standard_cost != src.prod_standard_cost OR
    tgt.prod_list_price != src.prod_list_price OR
    tgt.prod_size != src.prod_size OR
    tgt.prod_weight != src.prod_weight OR
    tgt.prod_product_category_id != src.prod_product_category_id OR
    tgt._tf_dim_product_category_id != src._tf_dim_product_category_id OR
    tgt.prod_product_model_id != src.prod_product_model_id OR
    tgt.prod_model_name != src.prod_model_name OR
    tgt.prod_sell_start_date != src.prod_sell_start_date OR
    tgt.prod_sell_end_date != src.prod_sell_end_date OR
    tgt.prod_discontinued_date != src.prod_discontinued_date
) THEN

  UPDATE SET
    tgt.prod_name = src.prod_name,
    tgt.prod_product_number = src.prod_product_number,
    tgt.prod_color = src.prod_color,
    tgt.prod_standard_cost = src.prod_standard_cost,
    tgt.prod_list_price = src.prod_list_price,
    tgt.prod_size = src.prod_size,
    tgt.prod_weight = src.prod_weight,
    tgt.prod_product_category_id = src.prod_product_category_id,
    tgt._tf_dim_product_category_id = src._tf_dim_product_category_id,
    tgt.prod_product_model_id = src.prod_product_model_id,
    tgt.prod_model_name = src.prod_model_name,
    tgt.prod_sell_start_date = src.prod_sell_start_date,
    tgt.prod_sell_end_date = src.prod_sell_end_date,
    tgt.prod_discontinued_date = src.prod_discontinued_date,
    tgt._tf_update_date = load_date

-- 2) Insert new records
WHEN NOT MATCHED THEN

  INSERT (
    prod_product_id,
    prod_name,
    prod_product_number,
    prod_color,
    prod_standard_cost,
    prod_list_price,
    prod_size,
    prod_weight,
    prod_product_category_id,
    _tf_dim_product_category_id,
    prod_product_model_id,
    prod_model_name,
    prod_sell_start_date,
    prod_sell_end_date,
    prod_discontinued_date,
    _tf_create_date,
    _tf_update_date
  )
  VALUES (
    src.prod_product_id,
    src.prod_name,
    src.prod_product_number,
    src.prod_color,
    src.prod_standard_cost,
    src.prod_list_price,
    src.prod_size,
    src.prod_weight,
    src.prod_product_category_id,
    src._tf_dim_product_category_id,
    src.prod_product_model_id,
    src.prod_model_name,
    src.prod_sell_start_date,
    src.prod_sell_end_date,
    src.prod_discontinued_date,
    load_date,        -- _tf_create_date
    load_date         -- _tf_update_date
  );

-- Exemple de jointure de la fact_sales à la dim_product
-- SELECT f.*, d.prod_product_id
-- FROM gold.fact_sales f
-- JOIN gold.dim_product d ON f.product_id = d.prod_product_id

-- COMMAND ----------

-- 10. Création de la dim_product_category
MERGE INTO gold.dim_product_category AS tgt
USING (
    SELECT
        pc.product_category_id AS prod_product_category_id,
        COALESCE(pc.name, 'N/A') AS prod_product_category_name,
        COALESCE(pc.parent_product_category_id, -1) AS prod_parent_product_category_id,
        COALESCE(ppc.name, 'N/A') AS prod_parent_product_category_name,
        COALESCE(pppc.product_category_id, -1) AS prod_grandparent_product_category_id,
        COALESCE(pppc.name, 'N/A') AS prod_grandparent_product_category_name
    FROM silver.productcategory pc
    LEFT JOIN silver.productcategory ppc
        ON pc.parent_product_category_id = ppc.product_category_id
    LEFT JOIN silver.productcategory pppc
        ON ppc.parent_product_category_id = pppc.product_category_id
    WHERE pc._tf_valid_to IS NULL
) AS src
ON tgt.prod_product_category_id = src.prod_product_category_id

-- 1) Update existing records when a difference is detected
WHEN MATCHED AND (
    tgt.prod_product_category_name != src.prod_product_category_name OR
    tgt.prod_parent_product_category_id != src.prod_parent_product_category_id OR
    tgt.prod_parent_product_category_name != src.prod_parent_product_category_name OR
    tgt.prod_grandparent_product_category_id != src.prod_grandparent_product_category_id OR
    tgt.prod_grandparent_product_category_name != src.prod_grandparent_product_category_name
) THEN

  UPDATE SET
    tgt.prod_product_category_name = src.prod_product_category_name,
    tgt.prod_parent_product_category_id = src.prod_parent_product_category_id,
    tgt.prod_parent_product_category_name = src.prod_parent_product_category_name,
    tgt.prod_grandparent_product_category_id = src.prod_grandparent_product_category_id,
    tgt.prod_grandparent_product_category_name = src.prod_grandparent_product_category_name,
    tgt._tf_update_date = load_date

-- 2) Insert new records
WHEN NOT MATCHED THEN

  INSERT (
    prod_product_category_id,
    prod_product_category_name,
    prod_parent_product_category_id,
    prod_parent_product_category_name,
    prod_grandparent_product_category_id,
    prod_grandparent_product_category_name,
    _tf_create_date,
    _tf_update_date
  )
  VALUES (
    src.prod_product_category_id,
    src.prod_product_category_name,
    src.prod_parent_product_category_id,
    src.prod_parent_product_category_name,
    src.prod_grandparent_product_category_id,
    src.prod_grandparent_product_category_name,
    load_date,        -- _tf_create_date
    load_date         -- _tf_update_date
  );
