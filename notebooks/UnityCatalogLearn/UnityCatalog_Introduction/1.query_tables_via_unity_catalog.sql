-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####Query data via unity catalog using 3 level namespace

-- COMMAND ----------

use catalog demo_catalog;
use schema demo_schema;

-- COMMAND ----------

-- select * from demo_catalog.demo_schema.circuits

-- COMMAND ----------

select * from circuits

-- COMMAND ----------

show databases

-- COMMAND ----------

show tables

-- COMMAND ----------

