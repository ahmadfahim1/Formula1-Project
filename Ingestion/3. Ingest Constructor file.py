# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_data_file", "2021-03-21")
v_file_date = dbutils.widgets.get("p_data_file")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read the JSON file using the spark dataframe reader

# COMMAND ----------

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read\
    .schema(constructor_schema)\
        .json(f"/mnt/formula7dl7/raw/{v_file_date}/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### step-2 - Drop url comulmn from the dataframe

# COMMAND ----------

constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC #### step-3- Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId","constructor_id")\
    .withColumnRenamed("constructorRef","constructor_ref")\
        .withColumn("ingestion_date", current_timestamp())\
            .withColumn("data_source", lit(v_data_source))\
                .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### step -4- Write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructor")

# COMMAND ----------

dbutils.notebook.exit("Success")