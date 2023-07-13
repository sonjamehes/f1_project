# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. set the park config fs.azure.account.key
# MAGIC 1. list files from demo container
# MAGIC 1. read data from circuits.csv file
# MAGIC

# COMMAND ----------

storage_key = dbutils.secrets.get(scope='formula1-scoape', key='formula1-df-accountkey')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.f1datalakelearn.dfs.core.windows.net", 
    storage_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1datalakelearn.dfs.core.windows.net"))

# COMMAND ----------

mydf = spark.read.csv("abfss://demo@f1datalakelearn.dfs.core.windows.net/circuits.csv", header=True)

# COMMAND ----------

display(mydf)