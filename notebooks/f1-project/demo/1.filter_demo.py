# Databricks notebook source
# MAGIC %run "../includes/config"

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races')

# COMMAND ----------

display(races_df)

# COMMAND ----------

## sql way
# races_filtered = races_df.filter('name like "%Grand Prix" and round = 4')
# races_filtered = races_df.where('name like "%Grand Prix" and round = 4')

# COMMAND ----------

## python way
races_filtered = races_df.filter((races_df["race_year"]==2019) & (races_df["round"]== 4 ))

# COMMAND ----------

display(races_filtered)