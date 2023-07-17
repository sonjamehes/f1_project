# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake using service principal
# MAGIC 1. Get client_id, tenant_id, client_secret from Key Vault
# MAGIC 2. Set Spark Config with App/ClientId, Directory/Tenant id & Secret
# MAGIC 3. Call file system utility mount to mount storage
# MAGIC 4. Explore other file system utilities to mount(list all mounts, unmount)
# MAGIC

# COMMAND ----------

client_id = dbutils.secrets.get(scope='formula1-scoape', key ='client-id')
tenant_id = dbutils.secrets.get(scope='formula1-scoape', key ='tenant-id')
client_secret = dbutils.secrets.get(scope='formula1-scoape', key ='client-secret')

# COMMAND ----------

display(client_id, tenant_id, client_secret)

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
    source = "abfss://demo@f1datalakelearn.dfs.core.windows.net/",
    mount_point = "/mnt/f1datalakelearn/demo",
    extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/f1datalakelearn/demo"))

# COMMAND ----------

mydf = spark.read.csv("/mnt/f1datalakelearn/demo/circuits.csv", header=True)

# COMMAND ----------

display(mydf)

# COMMAND ----------

#see all the mounts in the file store
display(dbutils.fs.mounts())

# COMMAND ----------

# see the files inside mnt/f1datalakelearn/demo

files = dbutils.fs.ls('/mnt/f1datalakelearn/demo')
for file in files:
    print(file.path)

# COMMAND ----------

# unmount - when you want to remove mounts that you've either created accidentaly or you just don't need them anymore

dbutils.fs.unmount(f'/mnt/f1datalakelearn/demo')