-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####time travel
-- MAGIC ####compacting small files and indexing
-- MAGIC ####vacuum

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####time travel
-- MAGIC ##### > audit data changes
-- MAGIC ##### > describe history command
-- MAGIC ##### > we can query older versions of the table (either through timestamp or version number)
-- MAGIC ##### > allows you to rollback thorugh restore table command (timestamp or version number)
-- MAGIC
-- MAGIC ###### RESTORE TABLE my_table TO TIMESTAMP AS OF "2023-05-01"
-- MAGIC ###### RESTORE TABLE my_table TO VERSION AS OF 25

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####compating small files
-- MAGIC ##### > optimize command: 
-- MAGIC ###### OPTIMIZE my_table
-- MAGIC ###### OPTIMIZE my_table ZORDER BY column_name (co-locating and reorganizing column information in the same set of files)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Vacuum a Delta Table
-- MAGIC ####>> Cleaning up unused data file
-- MAGIC ##### > uncomitted files 
-- MAGIC ##### > files that are no longer in the latest table state
-- MAGIC
-- MAGIC ####>> Vacuum command
-- MAGIC ##### > VACUUM table_name [retention period]
-- MAGIC ##### > Default retention period: 7 days
-- MAGIC
-- MAGIC

-- COMMAND ----------

desc history employees

-- COMMAND ----------

select * from employees version as of 3

-- COMMAND ----------

select * from employees @v3

-- COMMAND ----------

delete from employees

-- COMMAND ----------

restore table employees to version as of 3

-- COMMAND ----------

select * from employees

-- COMMAND ----------

truncate table employees

-- COMMAND ----------

select * from employees

-- COMMAND ----------

restore table employees to version as of 3

-- COMMAND ----------

select * from employees

-- COMMAND ----------

describe detail employees

-- COMMAND ----------

OPTIMIZE employees
ZORDER BY (id)
