# Databricks notebook source
# %run "../includes/config"

# COMMAND ----------

# drivers_df = spark.read.parquet(f'{presentation_folder_path}/driver_standings')

# COMMAND ----------

# display(drivers_df)

# COMMAND ----------

# from pyspark.sql.functions import sum, when, col, count

# COMMAND ----------

# constructor_df = drivers_df \
#     .groupBy('race_year','team') \
#     .agg(sum('total_points').alias('total_points'), sum('wins').alias('wins'))

# COMMAND ----------

# from pyspark.sql.functions import desc

# COMMAND ----------

# display(constructor_df.filter('race_year = 2020').orderBy(desc('wins')))

# COMMAND ----------

# constructor_df.mode('overwrite').parquet(f'{presentation_folder_path}/constructor_standings')