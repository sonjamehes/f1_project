# Databricks notebook source
# MAGIC %run "../includes/config"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inner, Left, Right, Outer Join

# COMMAND ----------

circuits_df = spark.read.parquet(f'{processed_folder_path}/circuits') \
    .filter("circuit_id < 70") \
    .withColumnRenamed("name", "circuit_name") 

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races').where('race_year=2019') \
    .withColumnRenamed("name", "race_name")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.race_country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.race_country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "outer") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.race_country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Semi Joins
# MAGIC similar to inner join. the difference is that you'll be given the columns from the left side table
# MAGIC kinda useless, you can use the inner and select the columns from the left side dataframe

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.race_country)

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Anti Joins
# MAGIC this will give everything from the left dataframe that is not found in the right dataframe

# COMMAND ----------

race_circuit_df = races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "anti") 


# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Cross Join
# MAGIC will give a a cartesian product = will take every record from the left and join to every record on the right
# MAGIC

# COMMAND ----------

race_circuit_df = races_df.crossJoin(circuits_df)

# COMMAND ----------

display(race_circuit_df.count())