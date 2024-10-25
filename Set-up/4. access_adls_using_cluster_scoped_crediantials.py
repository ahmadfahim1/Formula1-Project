# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using cluster scoped crediantials
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula7dl7.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula7dl7.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

