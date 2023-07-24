# Databricks notebook source
dbutils.widgets.text('param_file_date', "2021-03-21")
v_file_date = dbutils.widgets.get('param_file_date')

# COMMAND ----------

# MAGIC %run "../includes/config"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC Find race years for which the data needs to be reprocessed by adding into a list (through collect()) all the distinct years

# COMMAND ----------

# display(spark.read.parquet(f'{presentation_folder_path}/race_results').filter(f"file_date='{v_file_date}'" ))

# COMMAND ----------

race_results_df_list = spark.read.parquet(f'{presentation_folder_path}/race_results')\
    .filter(f"file_date='{v_file_date}'" )


# COMMAND ----------

race_year_list = dataframe_columns_to_list(race_results_df_list, 'race_year')

# COMMAND ----------

from pyspark.sql.functions import sum, when, col, count

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')\
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

driver_standings_df = race_results_df \
    .groupBy('race_year', 'driver_name', 'driver_nationality', 'team') \
    .agg(sum('points').alias('total_points'),
         count(when(col('position')== 1, True)).alias('wins'))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

driver_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))
final_df = driver_standings_df.withColumn('rank', rank().over(driver_rank_spec))

# COMMAND ----------

overwrite_partition(final_df, 'f1_presentation', 'driver_standings', 'race_year')

# COMMAND ----------

dbutils.notebook.exit('Success')