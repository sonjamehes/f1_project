# Databricks notebook source
# MAGIC %md
# MAGIC ##Explore the capabilities of the dbutils.secrets utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='formula1-scoape')

# COMMAND ----------

dbutils.secrets.get(scope='formula1-scoape', key='formula1-df-accountkey')

# COMMAND ----------

