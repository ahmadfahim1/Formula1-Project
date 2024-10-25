# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_data_file", "2021-03-21")
v_file_date = dbutils.widgets.get("p_data_file")

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType,TimestampType,DateType

# COMMAND ----------

race_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                        StructField("year", IntegerType(), True),
                                        StructField("round", IntegerType(), True),
                                        StructField("circuitId", IntegerType(), True),
                                        StructField("name", StringType(), True),
                                        StructField("date",DateType(), True),
                                        StructField("time", StringType(), True),
                                        StructField("url", StringType(), True)
])

# COMMAND ----------

race_df = spark.read\
    .option("header", "true")\
        .schema(race_schema)\
            .csv(f"dbfs:/mnt/formula7dl7/raw/{v_file_date}/races.csv")

# COMMAND ----------

display(race_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step -2 - Add ingestion and race_timestamp to dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import to_timestamp, concat, col, lit

# COMMAND ----------

races_with_timestamp_df = race_df.withColumn("ingestion_date", current_timestamp())\
    .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step -3 - Select required column

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

race_selected_df = races_with_timestamp_df.select(col("raceId").alias("race_id"), col("year").alias("race_year"),col("round"),col("circuitId").alias("circuit_id"),col("name"),col("ingestion_date"),col("race_timestamp"))

# COMMAND ----------

race_selected_df = race_selected_df\
    .withColumn("data_source", lit(v_data_source))\
        .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(race_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### write the output to processed container in parquet format

# COMMAND ----------

race_selected_df.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

