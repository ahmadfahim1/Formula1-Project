# Databricks notebook source
v_result = dbutils.notebook.run("1. Ingest_circuit_file", 0, {"p_data_source": "Ergast API","p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2. Ingest Race File",0, {"p_data_source": "Ergast API","p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3. Ingest Constructor file",0, {"p_data_source": "Ergast API","p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4. Ingest Drivers File", 0, {"p_data_source": "Ergast API","p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5. Ingest results file", 0, {"p_data_source": "Ergast API","p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6. Ingest Pitstop file",0, {"p_data_source": "Ergast API","p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7. Ingest Laptime file", 0, {"p_data_source": "Ergast API","p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8. Ingest qualifying file",0, {"p_data_source": "Ergast API","p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1)
# MAGIC from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

