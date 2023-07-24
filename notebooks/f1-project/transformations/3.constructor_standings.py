# Databricks notebook source
dbutils.widgets.text('param_file_date', "2021-03-21")
v_file_date = dbutils.widgets.get('param_file_date')

# COMMAND ----------

# MAGIC %run "../includes/config"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

# display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when, col, count

# COMMAND ----------

race_results_df_list = spark.read.parquet(f'{presentation_folder_path}/race_results')\
    .filter(f"file_date='{v_file_date}'" )


# COMMAND ----------

race_year_list = dataframe_columns_to_list(race_results_df_list, 'race_year')

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')\
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

constructor_standings_df = race_results_df \
    .groupBy('race_year','team') \
    .agg(sum('points').alias('total_points'),
         count(when(col('position')== 1, True)).alias('wins'))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

constructors_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))
final_df = constructor_standings_df.withColumn('rank', rank().over(constructors_rank_spec))

# COMMAND ----------


overwrite_partition(final_df, 'f1_presentation', 'constructor_standings', 'race_year')

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# %sql
# drop table f1_presentation.driver_standings