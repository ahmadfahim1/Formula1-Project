# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingest pitstop.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_data_file", "2021-03-28")
v_file_date = dbutils.widgets.get("p_data_file")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                       StructField("driverId", IntegerType(), True),
                                       StructField("stop", IntegerType(), True),
                                       StructField("lap", IntegerType(), True),
                                       StructField("time", StringType(), True),
                                       StructField("duration", StringType(), True),
                                       StructField("milliseconds", IntegerType(), True)])

# COMMAND ----------

pit_stops_df = spark.read\
    .schema(pit_stops_schema)\
        .option("multiline", True)\
        .json(f"/mnt/formula7dl7/raw/{v_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### ranme columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumn("ingestion_date", current_timestamp())\
        .withColumn("data_source", lit(v_data_source))\
            .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-3 - Write to output to processed container in parquet format

# COMMAND ----------

# overwrite_partition(pit_stops_final_df, 'f1_processed', 'pit_stops', 'race_id')

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop"
merge_delta_data(pit_stops_final_df, 'f1_processed','pit_stops', merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1)
# MAGIC from f1_processed.pit_stops
# MAGIC group by race_id
# MAGIC order by race_id DESC;