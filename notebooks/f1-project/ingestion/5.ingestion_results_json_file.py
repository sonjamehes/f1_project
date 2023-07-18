# Databricks notebook source
# display(dbutils.fs.mounts())

# COMMAND ----------

# %fs
# ls /mnt/f1datalakelearn/raw-bronze

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
    .json('/mnt/f1datalakelearn/raw-bronze/results.json')

# COMMAND ----------

# results_df.printSchema()

# COMMAND ----------

# display(results_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

results_renamed = results_df.withColumnRenamed('resultId', 'result_id')\
                            .withColumnRenamed('raceId', 'race_id') \
                            .withColumnRenamed('driverId', 'driver_id')\
                            .withColumnRenamed('constructorId', 'constructor_id')\
                            .withColumnRenamed('positionTest', 'position_text')\
                            .withColumnRenamed('positionOrder', 'position_order')\
                            .withColumnRenamed('fastestLap', 'fastest_lap')\
                            .withColumnRenamed('fastestLapTime', 'fastest_lap_time')\
                            .withColumnRenamed('FasttestLapSpeed', 'fastest_lap_speed')\
                            .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

final_results_df = results_renamed.drop('statusId')

# COMMAND ----------

display(final_results_df)

# COMMAND ----------

final_results_df.write.mode('overwrite').parquet('/mnt/f1datalakelearn/processed-silver/results')

# COMMAND ----------

# display(spark.read.parquet('/mnt/f1datalakelearn/processed-silver/results'))