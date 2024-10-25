# Databricks notebook source
# MAGIC %md
# MAGIC # Mount Azure Data Lake container for project
# MAGIC

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # get secrets from key vault
    client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-id')
    tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-tenant-id')
    client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-secret')

    #setting spark config
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # Unmount if already mounted
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    # Mount the storage account container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC  Mounting raw container

# COMMAND ----------

mount_adls('formula7dl7', 'raw')

# COMMAND ----------

mount_adls('formula7dl7', 'processed')

# COMMAND ----------

mount_adls('formula7dl7', 'presentation')

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula7dl7/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula7dl7/demo"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------
