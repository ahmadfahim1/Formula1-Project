# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuit.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_data_file", "2021-03-21")
v_file_date = dbutils.widgets.get("p_data_file")

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-1: Read the CSV file using the s[ark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields = [StructField("circuitID", IntegerType(), False),
                                        StructField("circuitRef", StringType(), True),
                                        StructField("name", StringType(), True),
                                        StructField("location", StringType(), True),
                                        StructField("country", StringType(), True),
                                        StructField("lat", DoubleType(), True),
                                        StructField("lng", DoubleType(), True),
                                        StructField("alt", IntegerType(), True),
                                        StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read\
    .option("header", "true")\
        .schema(circuits_schema)\
            .csv(f"dbfs:/mnt/formula7dl7/raw/{v_file_date}/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Select the required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-3 - Rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id")\
    .withColumnRenamed("circuitRef", "circuit_ref")\
        .withColumnRenamed("lat", "latitude")\
            .withColumnRenamed("lng", "longitude")\
                .withColumnRenamed("alt", "altitude")\
                    .withColumn("data_source", lit(v_data_source))\
                        .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step- 4 - Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step- 5 - Write data in the dataframe

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits