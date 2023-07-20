# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest constructors.json file

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
    .json(f'{raw_folder_path}/{v_file_date}/constructors.json')

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

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructor_final_df = constructor_df_drop.withColumnRenamed('constructorId', 'constructor_id') \
                                          .withColumnRenamed('constructorRef', 'constructor_ref') \
                                          .withColumn('data_source', lit(v_data_source)) \
                                          .withColumn('file_date', lit(v_file_date))
                                         

# COMMAND ----------

 constructor_final_df2 = add_ingestion_date(constructor_final_df)

# COMMAND ----------

# display(constructor_final_df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## write data to parquet

# COMMAND ----------

constructor_final_df2.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.constructors')

# COMMAND ----------

# display(spark.read.parquet('/mnt/f1datalakelearn/processed-silver/constructors'))

# COMMAND ----------

# %fs
# ls /mnt/f1datalakelearn/processed-silver/constructors

# COMMAND ----------

dbutils.notebook.exit('Success')