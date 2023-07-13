# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using service principal
# MAGIC 1. register to Azure AD Application/Service Principal
# MAGIC 2. Generate a secret/password for the Application
# MAGIC 3. Set Spark Config with App/ClientId, Directory/Tenant id & Secret
# MAGIC 4. Assign role 'Storage Blob Data Contributor' to the Data Lake
# MAGIC

# COMMAND ----------

display(dbutils.secrets.help())

# COMMAND ----------

display(dbutils.secrets.listScopes())

# COMMAND ----------

display(dbutils.secrets.list('formula1-scoape'))

# COMMAND ----------

client_id = dbutils.secrets.get(scope='formula1-scoape', key ='client-id')
tenant_id = dbutils.secrets.get(scope='formula1-scoape', key ='tenant-id')
client_secret = dbutils.secrets.get(scope='formula1-scoape', key ='client-secret')

# COMMAND ----------

display(client_id, tenant_id, client_secret)

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.f1datalakelearn.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.f1datalakelearn.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.f1datalakelearn.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.f1datalakelearn.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.f1datalakelearn.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1datalakelearn.dfs.core.windows.net"))

# COMMAND ----------

mydf = spark.read.csv("abfss://demo@f1datalakelearn.dfs.core.windows.net/circuits.csv", header=True)

# COMMAND ----------

display(mydf)