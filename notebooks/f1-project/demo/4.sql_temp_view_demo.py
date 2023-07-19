# Databricks notebook source
# MAGIC %md
# MAGIC #Access dataframes using SQL
# MAGIC
# MAGIC ##Objectives
# MAGIC ###1.Create temporary views on dataframes
# MAGIC ###2.Access the from from SQL cell
# MAGIC ###3.Access the view from Python cell

# COMMAND ----------

# MAGIC %run "../includes/config"

# COMMAND ----------

race_results = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results)

# COMMAND ----------

race_results.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select * from v_race_results where race_name like 'Canadian%' and driver_name like 'T%'

# COMMAND ----------

param_race_year = 2015

# COMMAND ----------

race_df = spark.sql(f'select * from v_race_results where race_year = {param_race_year}')

# COMMAND ----------

display(race_df)