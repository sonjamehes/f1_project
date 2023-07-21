# Databricks notebook source
# MAGIC %md
# MAGIC create race results 

# COMMAND ----------

dbutils.widgets.text('param_file_date', "2021-03-21")
v_file_date = dbutils.widgets.get('param_file_date')

# COMMAND ----------

# MAGIC %run "../includes/config"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# circuits, drivers, constructors, races, results

# COMMAND ----------

drivers = spark.read.parquet(f'{processed_folder_path}/drivers').withColumnRenamed("name", "driver_name").withColumnRenamed("number", "driver_number").withColumnRenamed("nationality", "driver_nationality")
races = spark.read.parquet(f'{processed_folder_path}/races').withColumnRenamed("race_timestamp", "race_date").withColumnRenamed("name", "race_name")#.filter('race_year = 2020')
circuits = spark.read.parquet(f'{processed_folder_path}/circuits').withColumnRenamed("location", "circuit_location")#.filter('circuit_location = "Abu Dhabi"')
constructors = spark.read.parquet(f'{processed_folder_path}/constructors').withColumnRenamed("name", "team")
results = spark.read.parquet(f'{processed_folder_path}/results').filter(f"file_date = '{v_file_date}'") \
                                                                .withColumnRenamed("time", "race_time") \
                                                                .withColumnRenamed("race_id", "result_race_id") \
                                                                .withColumnRenamed("file_date", "result_file_date") 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

results_driver = results.join(drivers, results.driver_id == drivers.driver_id, 'inner') \
                        .join(races, results.result_race_id == races.race_id, 'inner') \
                        .join(circuits, races.circuit_id == circuits.circuit_id, 'inner') \
                        .join(constructors, results.constructor_id == constructors.constructor_id, 'inner') \
                        .select(races.race_id, races.race_year, races.race_name, races.race_date,circuits.circuit_location, drivers.driver_name, drivers.driver_number, drivers.driver_nationality,constructors.team, results.grid, results.fastest_lap, results.race_time, results.points, results.position, results.result_file_date) \
                        .withColumnRenamed("result_file_date", "file_date")


# COMMAND ----------

display(results_driver)

# COMMAND ----------

results_driver_final = add_ingestion_date(results_driver)

# COMMAND ----------

# results_driver.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.race_results')

# COMMAND ----------

overwrite_partition (results_driver_final, 'f1_presentation', 'race_results', 'race_id')

# COMMAND ----------

# %sql
# select * from f1_presentation.race_results

# COMMAND ----------

# %sql
# describe f1_presentation.race_results

# COMMAND ----------

# display(spark.read.parquet(f"{presentation_folder_path}/race_results"))

# COMMAND ----------

# %sql
# drop table f1_presentation.race_results

# COMMAND ----------

dbutils.notebook.exit('Success')