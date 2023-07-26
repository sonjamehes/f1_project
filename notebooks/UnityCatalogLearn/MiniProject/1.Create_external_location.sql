-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####Create the external locations required for this project
-- MAGIC 1. bronze
-- MAGIC 2. silver
-- MAGIC 3. gold

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### documentation link
-- MAGIC https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/manage-external-locations-and-credentials#--create-an-external-location

-- COMMAND ----------

CREATE EXTERNAL LOCATION  IF NOT EXISTS unitycatalog_dl_ext_bronze
 URL 'abfss://bronze@saunitycatalogextdl.dfs.core.windows.net/'
 WITH (STORAGE CREDENTIAL `unitycatalog-ext-storagecredential`);

-- COMMAND ----------

describe external location unitycatalog_dl_ext_bronze

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'abfss://bronze@saunitycatalogextdl.dfs.core.windows.net/'

-- COMMAND ----------

CREATE EXTERNAL LOCATION  IF NOT EXISTS unitycatalog_dl_ext_silver
 URL 'abfss://silver@saunitycatalogextdl.dfs.core.windows.net/'
 WITH (STORAGE CREDENTIAL `unitycatalog-ext-storagecredential`);

-- COMMAND ----------

CREATE EXTERNAL LOCATION  IF NOT EXISTS unitycatalog_dl_ext_gold
 URL 'abfss://gold@saunitycatalogextdl.dfs.core.windows.net/'
 WITH (STORAGE CREDENTIAL `unitycatalog-ext-storagecredential`);