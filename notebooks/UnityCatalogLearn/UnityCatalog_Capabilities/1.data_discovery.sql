-- Databricks notebook source
Select * from system.information_schema.tables
where table_name = 'results'

-- COMMAND ----------

Select * from formula1_dev.information_schema.tables
where table_name = 'results'