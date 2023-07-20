-- Databricks notebook source
use f1_processed

-- COMMAND ----------

select * from drivers

-- COMMAND ----------

select *, CONCAT (name, '-', code) as new_driver_ref
from drivers

-- COMMAND ----------

select *, 
  SPLIT(name, ' ')[0] forename, 
  SPLIT(name, ' ')[1] surname,
  date_format(dob, 'dd-MM-yyyy') as DOB,
  current_timestamp as ingested_date
from drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC window function

-- COMMAND ----------

select
  nationality,
  name,
  dob,
  rank() over(partition by nationality order by dob desc) as age_rank
from drivers
order by nationality, age_rank 
