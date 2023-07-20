# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_result = dbutils.notebook.run('1.ingest_circuits_file', 0,  {"param_data_source" : "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run('2.ingest_races_file', 0,  {"param_data_source" : "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run('3.ingest_json_constructors_file', 0,  {"param_data_source" : "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run('4. ingest_drivers_file_json_nested', 0,  {"param_data_source" : "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run('5.ingestion_results_json_file', 0,  {"param_data_source" : "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run('6.ingest_pit_stops_file', 0,  {"param_data_source" : "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run('7.ingest_lap_times_files_folder', 0,  {"param_data_source" : "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run('8.ingest_qualifying_files_entire_folder', 0,  {"param_data_source" : "Ergast API"})

# COMMAND ----------

v_result