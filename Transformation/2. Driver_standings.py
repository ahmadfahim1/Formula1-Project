# Databricks notebook source
# MAGIC %md
# MAGIC ## Produce Dreiver Standings

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_file", "2021-03-28")
v_file_date = dbutils.widgets.get("p_data_file")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find the race year for which the data is to be processed

# COMMAND ----------

race_results_list = spark.read.parquet("/mnt/formula7dl7/presentation/results")\
    .filter(f"file_date = '{v_file_date}'")\
        .select("race_year")\
            .distinct()\
                .collect()

# COMMAND ----------

race_results_list

# COMMAND ----------

race_year_list = []
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)


# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.parquet("/mnt/formula7dl7/presentation/results")\
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col

driver_standings_df = race_results_df \
    .groupBy("race_year", "driver_name", "driver_nationality", "team")\
    .agg(sum("points").alias("total_points"),
         count(when(col("position") == 1, True)).alias("wins"))
        

# COMMAND ----------

display(driver_standings_df.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

display(final_df.filter("race_year == 2020"))

# COMMAND ----------

overwrite_partition(final_df,'f1_presentation', 'driver_standings', 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.driver_standings where race_year = 2021

# COMMAND ----------

