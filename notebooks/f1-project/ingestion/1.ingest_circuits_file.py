# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1. Read the CSV file using the spark dataframe reader

# COMMAND ----------

# dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text('param_data_source', "")
v_data_source = dbutils.widgets.get('param_data_source')

# COMMAND ----------

dbutils.widgets.text('param_file_date', "2021-03-21")
v_file_date = dbutils.widgets.get('param_file_date')

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run "../includes/config"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# circuits_df = spark.read.csv("dbfs:/mnt/f1datalakelearn/raw-bronze/circuits.csv", header=True) ## not this for some reason, even though it works

## or

# circuits_df = spark.read. \
# option("header", True). \
# csv("/mnt/f1datalakelearn/raw-bronze/circuits.csv")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

# COMMAND ----------

# MAGIC %md
# MAGIC create schema for df

# COMMAND ----------

# sometimes the data types are mixed up when reading the csv into a dataframe. create a schema to handle those datatypes
# create the schema. define field, datatype, and True is it should allow nulls, False if it shouldn't allow nulls (first field in our schema is a primary key, so it shouldn't allow nulls)
# structType represent the rows, StructFields represent the fields
circuit_schema = StructType(fields=[
                                    StructField("circuitId", IntegerType(), False),
                                    StructField("circuitRef", StringType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("location", StringType(), True),
                                    StructField("country", StringType(), True),
                                    StructField("lat", DoubleType(), True),
                                    StructField("lng", DoubleType(), True),
                                    StructField("alt", IntegerType(), True),
                                    StructField("url", StringType(), True)                                    
                                ])

# COMMAND ----------

# circuits_df = spark.read.csv("dbfs:/mnt/f1datalakelearn/raw-bronze/circuits.csv", header=True, schema= circuit_schema) ## not this for some reason, even though it works. mmh, i think it's correct too

## or

circuits_df = spark.read \
.option("header", True) \
.schema(circuit_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

# display(dbutils.fs.mounts())

# COMMAND ----------

# %fs
# ls /mnt/f1datalakelearn/raw-bronze/2021-03-21

# COMMAND ----------

# display(circuits_df)

# COMMAND ----------

# circuits_df.show()

# COMMAND ----------

# circuits_df.printSchema()

# COMMAND ----------

# circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step2. Remove unnecessary columns -> the 'url' column in our case

# COMMAND ----------

# circuits = circuits_df.drop('url')

##or
# circuits_selected_df = circuits_df[['circuitId', 'circuitRef', 'name', 'location', 'country', 'lat', 'lng', 'alt']]
##or

# circuits_selected_df = circuits_df.select('circuitId', 'circuitRef', 'name', 'location', 'country', 'lat', 'lng', 'alt')
 ## or

# circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng,     circuits_df.alt)
 ## or

# circuits_selected_df = circuits_df.select(circuits_df['circuitId'], circuits_df['circuitRef'],circuits_df['name'],circuits_df['location'],circuits_df['country'],circuits_df['lat'],circuits_df['lng'],circuits_df['alt'])

# COMMAND ----------

from pyspark.sql.functions import col,lit

# COMMAND ----------

## here you can also apply renames

circuits_selected_df = circuits_df.select(col('circuitId'), col('circuitRef'), col('name'), col('location'), col('country').alias('race_country'), col('lat'), col('lng'), col('alt'))

# COMMAND ----------

# display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step3. Rename the columns as required

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed('circuitId', 'circuit_id') \
                                            .withColumnRenamed('circuitRef', 'circuit_ref') \
                                            .withColumnRenamed('lat', 'latitude') \
                                            .withColumnRenamed('lng', 'longitude') \
                                            .withColumnRenamed('alt', 'altitude') \
                                            .withColumn('data_source', lit(v_data_source)) \
                                            .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step4. Adding a new column called 'ingestion_date' which will include the current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

#  circuits_final_df = circuits_renamed_df.withColumn('ingestion_date', current_timestamp()) \
#      .withColumn('env', lit('Production'))

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

#  display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 5. Write df to datalake as a Parquet file

# COMMAND ----------

# display(dbutils.fs.mounts())

# COMMAND ----------

# circuits_final_df.write.mode('overwrite').parquet(f'{processed_folder_path}/circuits') ##this will only save the df into ADLS in parquet format

# COMMAND ----------

##this will only save the df into ADLS in parquet format and also save the table under the f1_processsed database that we've created (as a managed table)
# circuits_final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.circuits')

# COMMAND ----------

circuits_final_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.circuits')

# COMMAND ----------

# %fs
# ls /mnt/f1datalakelearn/processed-silver/circuits

# COMMAND ----------

# display(spark.read.delta(f'{processed_folder_path}/circuits'))

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table f1_processed.qualifying
# MAGIC