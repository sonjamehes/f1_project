# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1. Read the CSV file using the spark dataframe reader

# COMMAND ----------

# circuits_df = spark.read.csv("dbfs:/mnt/f1datalakelearn/raw-bronze/races.csv", header=True) ## not this for some reason, even though it works
# display(circuits_df)
## or

# circuits_df = spark.read. \
# option("header", True). \
# csv("/mnt/f1datalakelearn/raw-bronze/circuits.csv")

# COMMAND ----------

display(circuits_df)

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

# circuits_df = spark.read.csv("dbfs:/mnt/f1datalakelearn/raw-bronze/circuits.csv", header=True, schema= circuit_schema) ## not this for some reason, even though it works. mmh, i think it's correct too

## or

races_df = spark.read \
.option("header", True) \
.schema(circuit_schema) \
.csv("/mnt/f1datalakelearn/raw-bronze/races.csv")

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

races_df.printSchema()

# COMMAND ----------

# races_df.describe().show()

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

circuits_renamed_df = circuits_selected_df.withColumnRenamed('raceId', 'race_id') \
                                            .withColumnRenamed('year', 'race_year') \
                                            .withColumnRenamed('circuitId', 'circuit_Id') 



# COMMAND ----------

# display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step4. Adding a new column called 'ingestion_date' which will include the current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, date_format, to_timestamp, concat, col, lit

# COMMAND ----------

 circuits_final_df = circuits_renamed_df.withColumn('ingestion_date', current_timestamp()) \
     .withColumn('race_timestamp',to_timestamp(concat(col('date'),lit(''),col('time')),'yyyy-MM-dd HH:mm:ss'))

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