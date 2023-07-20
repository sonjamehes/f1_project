# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1. Read the CSV file using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.text('param_data_source', "")
v_data_source = dbutils.widgets.get('param_data_source')

# COMMAND ----------

dbutils.widgets.text('param_file_date', "2021-03-21")
v_file_date = dbutils.widgets.get('param_file_date')

# COMMAND ----------

# MAGIC %run "../includes/config"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# circuits_df = spark.read.csv("dbfs:/mnt/f1datalakelearn/raw-bronze/races.csv", header=True) ## not this for some reason, even though it works
# display(circuits_df)
## or

# circuits_df = spark.read. \
# option("header", True). \
# csv("/mnt/f1datalakelearn/raw-bronze/circuits.csv")

# COMMAND ----------

# display(circuits_df)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType, TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC create schema for df

# COMMAND ----------

# sometimes the data types are mixed up when reading the csv into a dataframe. create a schema to handle those datatypes
# create the schema. define field, datatype, and True is it should allow nulls, False if it shouldn't allow nulls (first field in our schema is a primary key, so it shouldn't allow nulls)
# structType represent the rows, StructFields represent the fields
races_schema = StructType(fields=[
                                    StructField("raceId", IntegerType(), False),
                                    StructField("year", IntegerType(), True),
                                    StructField("round", IntegerType(), True),
                                    StructField("circuitId", IntegerType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("date", DateType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("url", StringType(), True)
                                 
                                ])

# COMMAND ----------

# circuits_df = spark.read.csv("dbfs:/mnt/f1datalakelearn/raw-bronze/races.csv", header=True, schema= races_schema) ## not this for some reason, even though it works. mmh, i think it's correct too

## or

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"{raw_folder_path }/{v_file_date}/races.csv")

# COMMAND ----------

## see all the mounts, search to get to the circuits.csv file
# display(dbutils.fs.mounts())

# COMMAND ----------

# # list everything inside the raw-bronze container so we can copy the file path for circuits.csv and use it in the read.csv method

# %fs
# ls /mnt/f1datalakelearn/raw-bronze

# COMMAND ----------

# display(races_df)

# COMMAND ----------

# races_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step.2 Add ingestion date and race_timestamp to dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, concat, col, to_timestamp

# COMMAND ----------

races_with_timestamp = races_df.withColumn('race_timestamp',to_timestamp(concat(col('date'),lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
                               .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# display(races_with_timestamp)

# COMMAND ----------

races_with_timestamp = add_ingestion_date(races_with_timestamp )

# COMMAND ----------

races_with_timestamp = races_with_timestamp.withColumn('data_source', lit(v_data_source))

# COMMAND ----------

display(races_with_timestamp)

# COMMAND ----------

# display(races_with_timestamp)

# COMMAND ----------

# races_df.printSchema()

# COMMAND ----------

# races_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step3. Select the columns required

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_selected_df = races_with_timestamp.select(col('raceId').alias('race_id'), col('year').alias('race_year'), col('round'), col('circuitId').alias('circuit_id'), col('name'), col('ingestion_date'), col('race_timestamp'))

# COMMAND ----------

# display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 4. Write the output to processed container in parquet format

# COMMAND ----------

# races_selected_df.write.mode('overwrite').parquet('/mnt/f1datalakelearn/processed-silver/races')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 4.2 Add to ADLS in parquet format but with partion 

# COMMAND ----------

races_selected_df.write.mode('overwrite').partitionBy('race_year').format('parquet').saveAsTable('f1_processed.races')

# COMMAND ----------

# display(dbutils.fs.mounts()

# COMMAND ----------

# %fs
# ls /mnt/f1datalakelearn/processed-silver/races

# COMMAND ----------

# display(spark.read.parquet(f'{processed_folder_path}/races'))

# COMMAND ----------

dbutils.notebook.exit('Success')