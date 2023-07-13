# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token
# MAGIC 1. set the park config for SAS token
# MAGIC 1. list files from demo container
# MAGIC 1. read data from circuits.csv file
# MAGIC

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

display(dbutils.secrets.list(scope="formula1-scoape"))

# COMMAND ----------

dbutils.secrets.get(scope='formula1-scoape', key='formula1-SAStoken2')

# COMMAND ----------

storage_key = dbutils.secrets.get(scope='formula1-scoape', key='formula1-SAStoken2')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.f1datalakelearn.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.f1datalakelearn.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.f1datalakelearn.dfs.core.windows.net", storage_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1datalakelearn.dfs.core.windows.net"))

# COMMAND ----------

mydf = spark.read.csv("abfss://demo@f1datalakelearn.dfs.core.windows.net/circuits.csv", header=True)

# COMMAND ----------

display(mydf)