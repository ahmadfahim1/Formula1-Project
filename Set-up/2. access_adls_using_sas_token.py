# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using sas token
# MAGIC 1. Set the spark config for sas token
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

formula1_sas_token = dbutils.secrets.get(scope = 'formula1-scope', key ='formula1-demo-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula7dl7.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula7dl7.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula7dl7.dfs.core.windows.net",formula1_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula7dl7.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula7dl7.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

