-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ingestion in the Bronze layer
-- MAGIC
-- MAGIC ## Connecting to the bronze layer (Target)

-- COMMAND ----------

USE CATALOG ngow_lakehouse;
USE DATABASE bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Load data into bronze layer of the Lakehouse

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of Address
-- MAGIC
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE Address 
AS SELECT * FROM adventureworks.saleslt.Address;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of Customer

-- COMMAND ----------

CREATE OR REPLACE TABLE Customer 
AS SELECT * FROM adventureworks.saleslt.Customer;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of CustomerAdress

-- COMMAND ----------

CREATE OR REPLACE TABLE CustomerAdress
AS SELECT * FROM adventureworks.saleslt.customeraddress;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of Product

-- COMMAND ----------

CREATE OR REPLACE TABLE Product
AS SELECT * FROM adventureworks.saleslt.Product;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of ProductCategory

-- COMMAND ----------

CREATE OR REPLACE TABLE ProductCategory
AS SELECT * FROM adventureworks.saleslt.ProductCategory;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of ProductDescription

-- COMMAND ----------

CREATE OR REPLACE TABLE ProductDescription
AS SELECT * FROM adventureworks.saleslt.productdescription;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of ProductModel

-- COMMAND ----------

CREATE OR REPLACE TABLE ProductModel
AS SELECT * FROM adventureworks.saleslt.productmodel;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of ProductModelProductDescription

-- COMMAND ----------

CREATE OR REPLACE TABLE ProductModelProductDescription
AS SELECT * FROM adventureworks.saleslt.productmodelproductdescription;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of SalesOrderDetail

-- COMMAND ----------

CREATE OR REPLACE TABLE SalesOrderDetail
AS SELECT * FROM adventureworks.saleslt.SalesOrderDetail;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of SalesOrderHeader

-- COMMAND ----------

CREATE OR REPLACE TABLE SalesOrderHeader
AS SELECT * FROM adventureworks.saleslt.SalesOrderHeader;
