-- Databricks notebook source
-- MAGIC %sql
-- MAGIC CREATE OR REPLACE TABLE employees
-- MAGIC --using DELTA
-- MAGIC (id INT, name STRING, salary DOUBLE);

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC INSERT INTO employees
-- MAGIC VALUES
-- MAGIC (1, "name1", 2340.5),
-- MAGIC (2, "name2", 5432.5),
-- MAGIC (3, "name3", 23240.5),
-- MAGIC (4, "name4", 234120.5),
-- MAGIC (5, "name5", 84594.5),
-- MAGIC (6, "name6", 1234.5)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.help()

-- COMMAND ----------

describe detail employees

-- COMMAND ----------

describe database default

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/user/hive/warehouse'

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/user/hive/warehouse/employees/'

-- COMMAND ----------

UPDATE employees SET salary = 32000.5 where id = 6

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/user/hive/warehouse/employees/'

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/user/hive/warehouse/employees/_delta_log/'

-- COMMAND ----------

describe detail employees

-- COMMAND ----------

UPDATE employees SET salary = 32000.5 where id = 2

-- COMMAND ----------

select * from employees

-- COMMAND ----------

describe detail employees

-- COMMAND ----------

desc history employees

-- COMMAND ----------

select * from employees version as of 1

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/user/hive/warehouse/employees/_delta_log/'

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC head 'dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000003.json'
