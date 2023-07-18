# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1. Read the JSON file using the spark dataframe reader API

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Step2. Rename columns and add new columns
# MAGIC 1. driverId renamed to driver_id
# MAGIC 2. driverRef renamed to driver_ref
# MAGIC 3. ingestion date added
# MAGIC 4. name added with concatenation or forename and surname

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3. Drop unwanted columns
# MAGIC 1. name.forname
# MAGIC 2. name.surname
# MAGIC 3. url