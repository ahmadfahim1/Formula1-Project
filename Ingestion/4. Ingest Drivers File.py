# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_data_file", "2021-03-21")
v_file_date = dbutils.widgets.get("p_data_file")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingest the drivers JSON file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step -1 - Read the drivers JSON file 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True), 
                                 StructField("surname", StringType(), True)
])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema, True),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read\
    .schema(drivers_schema)\
    .json(f"/mnt/formula7dl7/raw/{v_file_date}/drivers.json")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

drivers_renamed_column_df = drivers_df.withColumnRenamed("driverId", "driver_id")\
    .withColumnRenamed("driverRef", "driver_ref")\
        .withColumn("ingestion_date", current_timestamp())\
            .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))\
                .withColumn("data_source", lit(v_data_source))\
                    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3- drop unwanted column
# MAGIC 1. Forename
# MAGIC 2. Surname
# MAGIC 3. URL

# COMMAND ----------

drivers_final_df = drivers_renamed_column_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step -4 -  Write to output processed container in parquet format

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")