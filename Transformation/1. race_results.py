# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_file", "2021-03-21")
v_file_date = dbutils.widgets.get("p_data_file")

# COMMAND ----------

drivers_df = spark.read.format("delta").load("/mnt/formula7dl7/processed/drivers")\
    .withColumnRenamed("name", "driver_name")\
    .withColumnRenamed("nationality", "driver_nationality")\
        .withColumnRenamed("number", "driver_number")

# COMMAND ----------

constructors_df = spark.read.format("delta").load("/mnt/formula7dl7/processed/constructor")\
    .withColumnRenamed("name", "team")\
        .withColumnRenamed("nationality", "constructor_nationality")

# COMMAND ----------

circuits_df = spark.read.format("delta").load("/mnt/formula7dl7/processed/circuits")\
    .withColumnRenamed("name", "circuit_name")\
        .withColumnRenamed("location", "circuit_location")


# COMMAND ----------

races_df = spark.read.format("delta").load("/mnt/formula7dl7/processed/races")\
    .withColumnRenamed("name", "race_name")\
        .withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

results_df = spark.read.format("delta").load("/mnt/formula7dl7/processed/results")\
    .filter(f"file_date = '{v_file_date}'")\
        .withColumnRenamed("time","race_time")\
            .withColumnRenamed("race_id", "result_race_id")\
                .withColumnRenamed("file_date", "result_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### join circuit df to race df

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner")\
    .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####join results to all other dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.result_race_id == race_circuits_df.race_id)\
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id)\
        .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

final_df = race_results_df.select("race_id","race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points","position", "result_file_date")\
    .withColumn("created_date", current_timestamp())\
        .withColumnRenamed("result_file_date", "file_date")

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name =='Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

# overwrite_partition(final_df,'f1_presentation', 'results', 'race_id')

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_presentation','results', merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1)
# MAGIC from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id DESC;

# COMMAND ----------

