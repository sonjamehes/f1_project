# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1. Read the CSV file using the spark dataframe reader

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
.csv("/mnt/f1datalakelearn/raw-bronze/circuits.csv")

# COMMAND ----------

## see all the mounts, search to get to the circuits.csv file
# display(dbutils.fs.mounts())

# COMMAND ----------

# # list everything inside the raw-bronze container so we can copy the file path for circuits.csv and use it in the read.csv method

# %fs
# ls /mnt/f1datalakelearn/raw-bronze

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

from pyspark.sql.functions import col

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
                                            .withColumnRenamed('alt', 'altitude') 


# COMMAND ----------

# display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step4. Adding a new column called 'ingestion_date' which will include the current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

 circuits_final_df = circuits_renamed_df.withColumn('ingestion_date', current_timestamp()) \
     .withColumn('env', lit('Production'))

# COMMAND ----------

#  display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 5. Write df to datalake as a Parquet file

# COMMAND ----------

# display(dbutils.fs.mounts())

# COMMAND ----------

circuits_final_df.write.mode('overwrite').parquet('/mnt/f1datalakelearn/processed-silver/circuits')

# COMMAND ----------

# %fs
# ls /mnt/f1datalakelearn/processed-silver/circuits

# COMMAND ----------

# display(spark.read.parquet('/mnt/f1datalakelearn/processed-silver/circuits'))