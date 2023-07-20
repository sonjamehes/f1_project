# Databricks notebook source
# MAGIC %run "../includes/config"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Aggregate frunctions demo

# COMMAND ----------

# MAGIC %md
# MAGIC ####Built-in Aggregate functions

# COMMAND ----------

race_results_df= spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

race_results_filtered = race_results_df.filter("race_year = 2020")

# COMMAND ----------

display(race_results_filtered)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum, col

# COMMAND ----------

race_results_filtered.select(count('*')).show()

# COMMAND ----------

race_results_filtered.select(count('race_name')).show()

# COMMAND ----------

race_results_filtered.select(countDistinct('race_name')).show()

# COMMAND ----------

race_results_filtered.filter('driver_name = "Lewis Hamilton"').select(sum('points')).show()

# COMMAND ----------

race_results_filtered.filter('driver_name = "Lewis Hamilton"').select(sum('points'), countDistinct('race_name')) \
    .withColumnRenamed('sum(points)', 'total_points') \
    .withColumnRenamed('count( Distinct race_name)', 'number_of_races') \
    .show()

# COMMAND ----------

# ## this won't work if I add .countDistinct('race_name'). instead I should use the agg function below
# race_results_filtered \
#     .groupBy('driver_name') \
#     .sum('points') \
#     .show()

# COMMAND ----------

race_results_filtered \
    .groupBy('driver_name') \
    .agg(sum('points').alias('total_points'), countDistinct('race_name').alias('number_of_races')) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Window Functions

# COMMAND ----------

wind = race_results_df.filter("race_year in (2019, 2020)")

# COMMAND ----------

display(wind)

# COMMAND ----------

demo_wind = wind \
    .groupBy('race_year','driver_name') \
    .agg(sum('points').alias('total_points'), countDistinct('race_name').alias('number_of_races')) 


# COMMAND ----------

display(demo_wind)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank



# COMMAND ----------

driverRankSpec = Window.partitionBy('race_year').orderBy(desc('total_points'))
demo_wind.withColumn('rank', rank().over(driverRankSpec)).show(100)