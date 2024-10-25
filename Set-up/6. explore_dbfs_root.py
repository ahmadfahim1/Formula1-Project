# Databricks notebook source
# MAGIC %md
# MAGIC # Explore DBFS Root
# MAGIC 1. List all the folder in dbfs root
# MAGIC 2. Interact with dbfs file browser
# MAGIC 3. Upload file to dbfs Root

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

display(spark.read.csv('/FileStore/circuits.csv'))

# COMMAND ----------

