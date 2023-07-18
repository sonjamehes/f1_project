# Databricks notebook source
raw_folder_path = '/mnt/f1datalakelearn/raw-bronze'
processed_folder_path = '/mnt/f1datalakelearn/processed-silver'
presentation_folder_path = '/mnt/f1datalakelearn/presentation-gold'

# COMMAND ----------

## these should be used if you don't have a premium subscription or you don't have access to the DBFS (instead of mount use the abfss protocol)
# raw_folder_path = 'abfss://raw-bronze@f1datalakelearn.dfs.core.windows.net'
# processed_folder_path = 'abfss://processed-silver@f1datalakelearn.dfs.core.windows.net'
# presentation_folder_path = 'abfss://presentation-gold@f1datalakelearn.dfs.core.windows.net'