# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest qualifying.json files inside the qulifying folder

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

qualifying_schema = StructType(fields=[
                                    StructField('qualifyId', IntegerType(), False),
                                    StructField('raceId', IntegerType(), True),
                                    StructField('driverId', IntegerType(), True),
                                    StructField('constructorId', IntegerType(), True),
                                    StructField('number', IntegerType(), True),
                                    StructField('position', IntegerType(), True),
                                    StructField('q1', StringType(), True),
                                    StructField('q2', StringType(), True),
                                    StructField('q3', StringType(), True),
])

# COMMAND ----------

qualifying_df = spark.read \
    .schema(qualifying_schema) \
    .option('multiLine', True) \
    .json(f'{raw_folder_path}/{v_file_date}/qualifying') 

# COMMAND ----------

# display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 2. Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

qualifying_renamed = qualifying_df.withColumnRenamed('qualifyId', 'qualify_id') \
                                  .withColumnRenamed ('raceId', 'race_id') \
                                  .withColumnRenamed ('constructorId', 'constructor_id') \
                                  .withColumn('data_source', lit(v_data_source))


# COMMAND ----------

qualifying_renamed = add_ingestion_date(qualifying_renamed)

# COMMAND ----------

# display(qualifying_renamed)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3. Write to output to processed-silver container in parquet format

# COMMAND ----------

# qualifying_renamed.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.qualifying')

# COMMAND ----------

overwrite_partition (qualifying_renamed, 'f1_processed', 'qualifying', 'race_id')

# COMMAND ----------

dbutils.notebook.exit('Success')