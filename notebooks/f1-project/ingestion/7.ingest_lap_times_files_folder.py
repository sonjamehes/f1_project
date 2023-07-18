# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest lap_times.json files

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1. Read the CSV files using the spark dataframe reader API

# COMMAND ----------

# MAGIC %run "../includes/config"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"
# MAGIC

# COMMAND ----------

# display(dbutils.fs.mounts())

# COMMAND ----------

# %fs
# ls /mnt/f1datalakelearn/raw-bronze

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields=[
                                    StructField('raceId', IntegerType(), False),
                                    StructField('driverId', IntegerType(), True),
                                    StructField('lap', IntegerType(), True),
                                    StructField('position', IntegerType(), True),
                                    StructField('time', StringType(), True),
                                    StructField('millisecond', IntegerType(), True)
])

# COMMAND ----------

lap_times_df = spark.read \
    .schema(lap_times_schema) \
    .csv(f'{raw_folder_path}/lap_times') 

# COMMAND ----------

# display(lap_times_df)

# COMMAND ----------

# display(lap_times_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 2. Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

lap_times_renamed = lap_times_df.withColumnRenamed('driverId', 'driver_id') \
                              .withColumnRenamed ('raceId', 'race_id') 


# COMMAND ----------

lap_times_renamed = add_ingestion_date(lap_times_renamed)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3. Write to output to processed-silver container in parquet format

# COMMAND ----------

lap_times_renamed.write.mode('overwrite').parquet(f'{processed_folder_path}/lap_times')

# COMMAND ----------

# %fs
# ls /mnt/f1datalakelearn/processed-silver/lap_times