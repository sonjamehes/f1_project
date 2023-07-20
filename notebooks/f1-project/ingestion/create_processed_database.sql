-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/f1datalakelearn/processed-silver"

-- COMMAND ----------

describe database f1_raw;

-- COMMAND ----------

describe database f1_processed;

-- COMMAND ----------

