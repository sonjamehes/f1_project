# Databricks notebook source
# MAGIC %md
# MAGIC create race results 

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
results = spark.read.parquet(f'{processed_folder_path}/results').withColumnRenamed("time", "race_time")

# COMMAND ----------

display(drivers)

# COMMAND ----------

display(constructors)

# COMMAND ----------

display(races)

# COMMAND ----------

display(circuits)

# COMMAND ----------

display(results)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

results_driver = results.join(drivers, results.driver_id == drivers.driver_id, 'inner') \
                        .join(races, results.race_id == races.race_id, 'inner') \
                        .join(circuits, races.circuit_id == circuits.circuit_id, 'inner') \
                        .join(constructors, results.constructor_id == constructors.constructor_id, 'inner') \
                        .select(races.race_year, races.race_name, races.race_date,circuits.circuit_location, drivers.driver_name, drivers.driver_number, drivers.driver_nationality,constructors.team, results.grid, results.fastest_lap, results.race_time, results.points, results.position ) 


# COMMAND ----------

results_driver = add_ingestion_date(results_driver)

# COMMAND ----------

# display(results_driver)

# COMMAND ----------

results_driver.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.race_results')

# COMMAND ----------

# check = spark.read.parquet(f'{presentation_folder_path}/race_results').filter('race_year = 2020').filter('circuit_location = "Abu Dhabi"')

# COMMAND ----------

# display(check.sort(col('points'), ascending = False))

# COMMAND ----------

dbutils.notebook.exit('Success')