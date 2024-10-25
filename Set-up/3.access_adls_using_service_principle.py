# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using service principle
# MAGIC 1. Register Azure AD application/ service principal
# MAGIC 2. Generate a secret/password for the application
# MAGIC 3. Set spark config with app/ client Id, Directory/ Tenant Id & Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the Data Lake

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula7dl7.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula7dl7.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula7dl7.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula7dl7.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula7dl7.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula7dl7.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula7dl7.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

