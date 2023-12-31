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

dbutils.widgets.text('param_data_source', "")
v_data_source = dbutils.widgets.get('param_data_source')

# COMMAND ----------

dbutils.widgets.text('param_file_date', "2021-03-28")
v_file_date = dbutils.widgets.get('param_file_date')

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
    .json(f'{raw_folder_path}/{v_file_date}/pit_stops.json') 

# COMMAND ----------

# display(pit_stop_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 2. Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

pit_stop_renamed = pit_stop_df.withColumnRenamed('driverId', 'driver_id') \
                              .withColumnRenamed ('raceId', 'race_id') \
                              .withColumn('data_source', lit(v_data_source))


# COMMAND ----------

pit_stop_renamed = add_ingestion_date(pit_stop_renamed)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3. Write to output to processed-silver container in parquet format

# COMMAND ----------

# overwrite_partition (pit_stop_renamed, 'f1_processed', 'pit_stops', 'race_id')

# COMMAND ----------

merge_condition = 'tgt.race_id = src.race_id  AND tgt.driver_id = src.driver_id  AND tgt.stop = src.stop'
merge_delta_data(pit_stop_renamed, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# pit_stop_renamed.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.pit_stops')

# COMMAND ----------

dbutils.notebook.exit('Success')