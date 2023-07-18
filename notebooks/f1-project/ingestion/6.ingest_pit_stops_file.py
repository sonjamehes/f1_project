# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest pit_stops.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1. Read the JSON file using the spark dataframe reader API

# COMMAND ----------

# MAGIC %md
# MAGIC ####this file is a multiline json, so we will need to set option multiLine to True when reading the file

# COMMAND ----------

# display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %run "../includes/config"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"
# MAGIC

# COMMAND ----------

# %fs
# ls /mnt/f1datalakelearn/raw-bronze

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stop_schema = StructType(fields=[
                                    StructField('raceId', IntegerType(), False),
                                    StructField('driverId', IntegerType(), True),
                                    StructField('stop', StringType(), True),
                                    StructField('lap', IntegerType(), True),
                                    StructField('time', StringType(), True),
                                    StructField('duration', StringType(), True),
                                    StructField('millisecond', IntegerType(), True)
])

# COMMAND ----------

pit_stop_df = spark.read \
    .schema(pit_stop_schema) \
    .option('multiLine', True) \
    .json(f'{raw_folder_path}/pit_stops.json') 

# COMMAND ----------

# display(pit_stop_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 2. Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pit_stop_renamed = pit_stop_df.withColumnRenamed('driverId', 'driver_id') \
                              .withColumnRenamed ('raceId', 'race_id') 


# COMMAND ----------

pit_stop_renamed = add_ingestion_date(pit_stop_renamed)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3. Write to output to processed-silver container in parquet format

# COMMAND ----------

pit_stop_renamed.write.mode('overwrite').parquet(f'{processed_folder_path}/pit_stops')