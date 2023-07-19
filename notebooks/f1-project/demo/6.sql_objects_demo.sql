-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Lesson Objectives
-- MAGIC ####1. Spark SQL Documentation
-- MAGIC ####2. Create database Demo
-- MAGIC ####3.Data tab in the UI
-- MAGIC ####4.SHOW command
-- MAGIC ####5.DESCRIBE command
-- MAGIC ####6.Find the current database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

describe database demo;

-- COMMAND ----------

describe database extended demo;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Learning Objectives - MANAGED TABLES (when you drop the table it will delete both the data and the metadata)
-- MAGIC ####1.Create managed table using Python
-- MAGIC ####2.Create managed table using SQL
-- MAGIC ####3.Effect of dropping a managed table
-- MAGIC ####4.Describe table
-- MAGIC

-- COMMAND ----------

-- MAGIC %run "../includes/config"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_df.write.format('parquet').saveAsTable('demo.race_results_python')

-- COMMAND ----------

Use demo;
SHOW TABLES;

-- COMMAND ----------

describe race_results_python


-- COMMAND ----------

describe extended race_results_python


-- COMMAND ----------

select * from demo.race_results_python where race_year = 2020;

-- COMMAND ----------

CREATE TABLE demo.race_results_sql
AS
select * from demo.race_results_python where race_year = 2020;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

describe extended demo.race_results_sql

-- COMMAND ----------

drop table demo.race_results_sql

-- COMMAND ----------

SHOW TABLES in demo

-- COMMAND ----------

