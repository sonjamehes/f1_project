# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_result = dbutils.notebook.run('1.race_results', 0)

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run('2.driver_standings', 0)

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run('3.constructor_standings', 0)

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run('4.constructor_standings_mine', 0)

# COMMAND ----------

v_result