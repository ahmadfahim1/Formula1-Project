# Databricks notebook source
# MAGIC %md
# MAGIC # Mount Azure Data Lake using service principle
# MAGIC 1. Get client Id,tenant_id,client_secret from key vault
# MAGIC 2. Set spark config with app/ client Id, Directory/ Tenant Id & Secret
# MAGIC 3. Call file suystem utility mount to mount to mount the storage
# MAGIC 4. Explote othert file system utilities related to mount(list all mounts, unmount)

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula7dl7.dfs.core.windows.net/",
  mount_point = "/mnt/formula7dl7/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula7dl7/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula7dl7/demo"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

