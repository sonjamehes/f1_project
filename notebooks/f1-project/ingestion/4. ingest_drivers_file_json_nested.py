# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1. Read the JSON file using the spark dataframe reader API

# COMMAND ----------

dbutils.widgets.text('param_data_source', "")
v_data_source = dbutils.widgets.get('param_data_source')

# COMMAND ----------

dbutils.widgets.text('param_file_date', "2021-03-21")
v_file_date = dbutils.widgets.get('param_file_date')

# COMMAND ----------

# MAGIC %run "../includes/config"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

# display(dbutils.fs.mounts())

# COMMAND ----------

# %fs
# ls /mnt/f1datalakelearn/raw-bronze

# COMMAND ----------

# first we create the schema for the nested object
name_schema = StructType(fields= [
                                StructField('forename', StringType(), True),
                                StructField('surname', StringType(), True)
])

# COMMAND ----------

driver_schema = StructType(fields= [
                                StructField('driverId', IntegerType(), False),
                                StructField('driverRef', StringType(), True),
                                StructField('number', IntegerType(), True),
                                StructField('code', StringType(), True),
                                StructField('name', name_schema),
                                StructField('dob', DateType(), True),
                                StructField('nationality', StringType(), True),
                                StructField('url', StringType(), True)

])

# COMMAND ----------

drivers_df = spark.read \
.schema(driver_schema) \
.json(f'{raw_folder_path}/{v_file_date}/drivers.json')


# COMMAND ----------

# drivers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step2. Rename columns and add new columns
# MAGIC 1. driverId renamed to driver_id
# MAGIC 2. driverRef renamed to driver_ref
# MAGIC 3. ingestion date added
# MAGIC 4. name added with concatenation or forename and surname

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit


# COMMAND ----------


drivers_df_renamed = drivers_df.withColumnRenamed('driverId', 'driver_id') \
                               .withColumnRenamed ('driverRef', 'driver_red') \
                               .withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname'))) \
                               .withColumn('data_source', lit(v_data_source)) \
                               .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

drivers_df_renamed = add_ingestion_date(drivers_df_renamed)

# COMMAND ----------

# display(drivers_df_renamed)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3. Drop unwanted columns
# MAGIC 1. name.forname
# MAGIC 2. name.surname
# MAGIC 3. url

# COMMAND ----------

## forename and surname have been dropped when we renamed the column name

final_df = drivers_df_renamed.drop('url')

# COMMAND ----------

# display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 4. Write output to processed-silver container in parquet format

# COMMAND ----------

final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.drivers')

# COMMAND ----------

# %fs
# ls /mnt/f1datalakelearn/processed-silver/drivers

# COMMAND ----------

dbutils.notebook.exit('Success')