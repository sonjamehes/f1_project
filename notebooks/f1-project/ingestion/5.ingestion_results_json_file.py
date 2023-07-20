# Databricks notebook source
# display(dbutils.fs.mounts())

# COMMAND ----------

# %fs
# ls /mnt/f1datalakelearn/raw-bronze

# COMMAND ----------

dbutils.widgets.text('param_data_source', "")
v_data_source = dbutils.widgets.get('param_data_source')

# COMMAND ----------

# MAGIC %run "../includes/config"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

schema_results = StructType(fields=[
                              StructField('constructorId', IntegerType(), False),
                              StructField('driverId', IntegerType(), False),
                              StructField('fastestLap', IntegerType(), True),
                              StructField('fastestLapSpeed', StringType(), True),
                              StructField('fastestLapTime', StringType(), True),
                              StructField('grid', IntegerType(), False),
                              StructField('laps', IntegerType(), False),
                              StructField('milliseconds', IntegerType(), True),
                              StructField('number', IntegerType(), True),
                              StructField('points', FloatType(), True),
                              StructField('position', IntegerType(), False),
                              StructField('positionOrder', IntegerType(), False),
                              StructField('positionText', StringType(), False),
                              StructField('raceId', IntegerType(), False), 
                              StructField('rank', IntegerType(), True),
                              StructField('resultId', IntegerType(), False),
                              StructField('statusId', IntegerType(), False),
                              StructField('time', StringType(), False)

                        ])


# COMMAND ----------

results_df = spark.read \
    .schema(schema_results) \
    .json(f'{raw_folder_path}/results.json')

# COMMAND ----------

# results_df.printSchema()

# COMMAND ----------

# display(results_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

results_renamed = results_df.withColumnRenamed('resultId', 'result_id')\
                            .withColumnRenamed('raceId', 'race_id')\
                            .withColumnRenamed('driverId', 'driver_id')\
                            .withColumnRenamed('constructorId', 'constructor_id')\
                            .withColumnRenamed('positionTest', 'position_text')\
                            .withColumnRenamed('positionOrder', 'position_order')\
                            .withColumnRenamed('fastestLap', 'fastest_lap')\
                            .withColumnRenamed('fastestLapTime', 'fastest_lap_time')\
                            .withColumnRenamed('FasttestLapSpeed', 'fastest_lap_speed')\
                            .withColumn('data_source', lit(v_data_source))


# COMMAND ----------

results_renamed = add_ingestion_date(results_renamed )

# COMMAND ----------

final_results_df = results_renamed.drop('statusId')

# COMMAND ----------

# display(final_results_df)

# COMMAND ----------

final_results_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.results')

# COMMAND ----------

# display(spark.read.parquet('/mnt/f1datalakelearn/processed-silver/results'))

# COMMAND ----------

dbutils.notebook.exit('Success')