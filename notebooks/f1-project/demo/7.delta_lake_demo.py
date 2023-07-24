# Databricks notebook source
# MAGIC %md
# MAGIC #####1. Write data to delta lake (managed table)
# MAGIC #####2. Write data to delta lake (external table)
# MAGIC #####3. Read data from delta lake (table)
# MAGIC #####4. Read data from delta lake (file)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists f1_demo
# MAGIC location '/mnt/f1datalakelearn/demo'

# COMMAND ----------

dbutils.widgets.text('param_file_date', "2021-03-21")
v_file_date = dbutils.widgets.get('param_file_date')

# COMMAND ----------

# MAGIC %run "../includes/config"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

results_df = spark.read \
    .option("inferSchema", True) \
    .json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ######save the results_df to a managed table

# COMMAND ----------

results_df.write.format('delta').mode('overwrite').saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ######save the results_df to an external table

# COMMAND ----------

results_df.write.format('delta').mode('overwrite').save('/mnt/f1datalakelearn/demo/results_external')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_demo.results_external
# MAGIC using DELTA
# MAGIC location '/mnt/f1datalakelearn/demo/results_external'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### when I want to read the data from the table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_external

# COMMAND ----------

# MAGIC %md
# MAGIC ##### when I want to read the data from the external source and add it into a dataframe

# COMMAND ----------

result_external_df = spark.read.format('delta').load('/mnt/f1datalakelearn/demo/results_external')

# COMMAND ----------

display(result_external_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### how to write data to partition folders

# COMMAND ----------

results_df.write.format('delta').mode('overwrite').partitionBy('constructorId').saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions f1_demo.results_partitioned

# COMMAND ----------

