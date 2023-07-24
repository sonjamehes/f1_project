# Databricks notebook source
# MAGIC %md
# MAGIC #####1. Write data to delta lake (managed table)
# MAGIC #####2. Write data to delta lake (external table)
# MAGIC #####3. Read data from delta lake (table)
# MAGIC #####4. Read data from delta lake (file)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists f1_demo
# MAGIC location '/mnt/f1datalakelearn/demo'

# COMMAND ----------

dbutils.widgets.text('param_file_date', "2021-03-21")
v_file_date = dbutils.widgets.get('param_file_date')

# COMMAND ----------

# MAGIC %run "../includes/config"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

results_df = spark.read \
    .option("inferSchema", True) \
    .json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ######save the results_df to a managed table

# COMMAND ----------

results_df.write.format('delta').mode('overwrite').saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ######save the results_df to an external table

# COMMAND ----------

results_df.write.format('delta').mode('overwrite').save('/mnt/f1datalakelearn/demo/results_external')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_demo.results_external
# MAGIC using DELTA
# MAGIC location '/mnt/f1datalakelearn/demo/results_external'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### when I want to read the data from the table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_external

# COMMAND ----------

# MAGIC %md
# MAGIC ##### when I want to read the data from the external source and add it into a dataframe

# COMMAND ----------

result_external_df = spark.read.format('delta').load('/mnt/f1datalakelearn/demo/results_external')

# COMMAND ----------

display(result_external_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### how to write data to partition folders

# COMMAND ----------

results_df.write.format('delta').mode('overwrite').partitionBy('constructorId').saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC ###1. Update Delta Table
# MAGIC ###2. Delete from Delta Table

# COMMAND ----------

# MAGIC %md
# MAGIC #delta lake suports Updates, Deletes and Merges

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC update f1_demo.results_managed set points= 11- position where position <= 10

# COMMAND ----------

# MAGIC %md
# MAGIC ##update using the sql way

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ##update using the python way

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/f1datalakelearn/demo/results_managed')
deltaTable.update("position <= 10", {"points" : "21- position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed where constructorId != 131

# COMMAND ----------

# MAGIC %md
# MAGIC ##delete data using the sql way

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC delete from f1_demo.results_managed where constructorId = 131

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ##delete data using the python way

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/f1datalakelearn/demo/results_managed')
deltaTable.delete("points <= 0")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ##upsert using merge

# COMMAND ----------

drivers_day1_df = spark.read \
    .option("inferSchema", True) \
    .json(f"{raw_folder_path}/{v_file_date}/drivers.json") \
    .filter("driverId <= 10") \
    .select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day2_df = spark.read \
    .option("inferSchema", True) \
    .json(f"{raw_folder_path}/{v_file_date}/drivers.json") \
    .filter("driverId between 6 and 15") \
    .select("driverId", "dob", upper("name.forename").alias('forename'), upper("name.surname").alias('surname'))

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day3_df = spark.read \
    .option("inferSchema", True) \
    .json(f"{raw_folder_path}/{v_file_date}/drivers.json") \
    .filter("driverId between 1 and 5 or driverId between 16 and 20") \
    .select("driverId", "dob", upper("name.forename").alias('forename'), upper("name.surname").alias('surname'))

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

display(drivers_day3_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA 

# COMMAND ----------

# MAGIC %md
# MAGIC ###Day 1

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC on tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (driverId, dob, forename,surname,createdDate) VALUES (driverId, dob, forename,surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ###Day 2

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC on tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (driverId, dob, forename,surname,createdDate) VALUES (driverId, dob, forename,surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ##Merge using python syntax

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/f1datalakelearn/demo/drivers_merge')


deltaTable.alias('tgt') \
  .merge(
    drivers_day3_df.alias('upd'),
    'tgt.driverId = upd.driverId'
  ) \
  .whenMatchedUpdate(set =
    {
      "dob": "upd.dob",
      "forename": "upd.forename",
      "surname": "upd.surname",
      "updatedDate": "current_timestamp()"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "upd.driverId",
      "dob": "upd.dob",
      "forename": "upd.forename",
      "surname": "upd.surname",
      "createdDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge