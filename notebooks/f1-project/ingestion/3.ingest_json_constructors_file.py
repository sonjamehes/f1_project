# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest constructors.json file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType,  StringType

# COMMAND ----------

constructor_schema = StructType(fields=[
                                    StructField("constructorId", IntegerType(), False),
                                    StructField("constructorRef", StringType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True),
                             
                                ])

# COMMAND ----------

# constructor_schema = 'constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING' ## 2nd version, not that used 

# COMMAND ----------

constructor_df = spark.read \
    .schema(constructor_schema) \
    .json('/mnt/f1datalakelearn/raw-bronze/constructors.json')

# COMMAND ----------

# display(constructor_df.printSchema())

# COMMAND ----------

# display(dbutils.fs.mounts())

# COMMAND ----------

# %fs
# ls /mnt/f1datalakelearn/raw-bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ###drop the 'url' column
# MAGIC
# MAGIC

# COMMAND ----------

constructor_df_drop = constructor_df.drop('url')


# COMMAND ----------

# MAGIC %md
# MAGIC ## rename columns and add ingestion date as a new column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp 

# COMMAND ----------

constructor_final_df = constructor_df_drop.withColumnRenamed('constructorId', 'constructor_id')\
                                          .withColumnRenamed('constructorRef', 'constructor_ref')\
                                          .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## write data to parquet

# COMMAND ----------

constructor_final_df.write.mode('overwrite').parquet('/mnt/f1datalakelearn/processed-silver/constructors')

# COMMAND ----------

# display(spark.read.parquet('/mnt/f1datalakelearn/processed-silver/constructors'))