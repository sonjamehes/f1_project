# Databricks notebook source
# display(dbutils.fs.mounts())

# COMMAND ----------

# %fs
# ls /mnt/f1datalakelearn/raw-bronze

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
    .json(f'{raw_folder_path}/{v_file_date}/results.json')

# COMMAND ----------

# results_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

results_renamed = results_df.withColumnRenamed('resultId', 'result_id')\
                            .withColumnRenamed('raceId', 'race_id')\
                            .withColumnRenamed('driverId', 'driver_id')\
                            .withColumnRenamed('constructorId', 'constructor_id')\
                            .withColumnRenamed('positionText', 'position_text')\
                            .withColumnRenamed('positionOrder', 'position_order')\
                            .withColumnRenamed('fastestLap', 'fastest_lap')\
                            .withColumnRenamed('fastestLapTime', 'fastest_lap_time')\
                            .withColumnRenamed('FasttestLapSpeed', 'fastest_lap_speed')\
                            .withColumn('data_source', lit(v_data_source)) \
                            .withColumn('file_date', lit(v_file_date))


# COMMAND ----------

results_renamed = add_ingestion_date(results_renamed )

# COMMAND ----------

final_results_df = results_renamed.drop('statusId')

# COMMAND ----------

# display(final_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Method 1 for handling incremental load
# MAGIC ######the below code will drop the partitions if they exist and append the data 

# COMMAND ----------

# for race_id_list in final_results_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# final_results_df.write.mode('append').partitionBy('race_id').format('parquet').saveAsTable('f1_processed.results')

# COMMAND ----------

# print(final_results_df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Method 2 for handling incremental load
# MAGIC
# MAGIC ######final_results_df.write.mode('overwrite').partitionBy('race_id').format('parquet').saveAsTable('f1_processed.results') --> will be used for the cut over file
# MAGIC ######final_results_df.write.mode('overwrite').insertInto("f1_processed.results") --> will be used for the rest of the incremental files
# MAGIC ######--> we will need the last column to be race_id, the one with the partition, so need to move it on the last position
# MAGIC ######--> last but not least, don't forget to set the overwrite mode as DYNAMIC, otherwise "final_results_df.write.mode('overwrite').insertInto("f1_processed.results")" will overwrite the data too

# COMMAND ----------

# spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

# final_results_df = final_results_df.select( 'result_id','constructor_id', 'driver_id', 'fastest_lap', 'fastestLapSpeed', 'fastest_lap_time', 'grid', 'laps', 'milliseconds', 'number', 'points', 'position', 'position_order', 'position_Text', 'rank',  'time', 'data_source', 'file_date', 'ingestion_date', 'race_id')
                              

# COMMAND ----------

# if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     final_results_df.write.mode('overwrite').insertInto("f1_processed.results")
# else:
#     final_results_df.write.mode('overwrite').partitionBy('race_id').format('parquet').saveAsTable('f1_processed.results')

# COMMAND ----------

overwrite_partition (final_results_df, 'f1_processed', 'results', 'race_id')

# COMMAND ----------

dbutils.notebook.exit('Success')