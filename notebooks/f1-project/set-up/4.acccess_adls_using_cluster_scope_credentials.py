# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using cluster scoped credentials
# MAGIC 1. set the park config fs.azure.account.key in the cluster
# MAGIC 2. list files from demo container
# MAGIC 3. read data from circuits.csv file
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1datalakelearn.dfs.core.windows.net"))

# COMMAND ----------

mydf = spark.read.csv("abfss://demo@f1datalakelearn.dfs.core.windows.net/circuits.csv", header=True)

# COMMAND ----------

display(mydf)