# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake using service principal
# MAGIC 1. Get client_id, tenant_id, client_secret from Key Vault
# MAGIC 2. Set Spark Config with App/ClientId, Directory/Tenant id & Secret
# MAGIC 3. Call file system utility mount to mount storage
# MAGIC 4. Explore other file system utilities to mount(list all mounts, unmount)
# MAGIC

# COMMAND ----------

def mount_adls(storage_account_name, container_name):

    # get secrets from Key Vault
    client_id = dbutils.secrets.get(scope='formula1-scoape', key ='client-id')
    tenant_id = dbutils.secrets.get(scope='formula1-scoape', key ='tenant-id')
    client_secret = dbutils.secrets.get(scope='formula1-scoape', key ='client-secret')

    #set spark config
    configs = {"fs.azure.account.auth.type": "OAuth",
                "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                "fs.azure.account.oauth2.client.id": client_id,
                "fs.azure.account.oauth2.client.secret": client_secret,
                "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    #if any container is already mount, unmount it so it can be mounted again
    if any (mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    #mount the storage container
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

    display(dbutils.fs.mounts())


# COMMAND ----------

mount_adls("raw-bronze")

# COMMAND ----------

mount_adls("processed-silver")

# COMMAND ----------

mount_adls("presentation-gold")

# COMMAND ----------

mount_adls("demo")

# COMMAND ----------

display(dbutils.fs.mounts())