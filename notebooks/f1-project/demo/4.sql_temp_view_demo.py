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

# COMMAND ----------

# MAGIC %md
# MAGIC #Access dataframes using SQL
# MAGIC
# MAGIC ##Objectives
# MAGIC ###1.Create global temporary views on dataframes
# MAGIC ###2.Access the from from SQL cell
# MAGIC ###3.Access the view from Python cell
# MAGIC ###4.Access the view from another notebook

# COMMAND ----------

race_results.createOrReplaceGlobalTempView("gv_global_race")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_global_race where race_year = 2020

# COMMAND ----------

display(spark.sql("select * from global_temp.gv_global_race where race_year = 2019"))