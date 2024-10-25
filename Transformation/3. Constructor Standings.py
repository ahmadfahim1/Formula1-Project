# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_file", "2021-03-28")
v_file_date = dbutils.widgets.get("p_data_file")

# COMMAND ----------

race_results_list = spark.read.parquet("/mnt/formula7dl7/presentation/results")\
    .filter(f"file_date = '{v_file_date}'")\
        .select("race_year")\
            .distinct()\
                .collect()

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

from pyspark.sql.functions import *
from pyspark.sql.functions import sum, count, when, col

# COMMAND ----------

constructor_standing_df = race_results_df \
    .groupBy("race_year", "team") \
    .agg(
        sum("points").alias("points"),
        count(when(col("position") == 1, True)).alias("wins")
    )


# COMMAND ----------

display(constructor_standing_df.filter("race_year = 2021"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *

# COMMAND ----------

cons_rank_spec = Window.partitionBy("race_year").orderBy(desc("points"), desc("wins"))
final_df = constructor_standing_df.withColumn("rank", rank().over(cons_rank_spec))

# COMMAND ----------

display(final_df.filter("race_year== 2021"))

# COMMAND ----------

overwrite_partition(final_df,'f1_presentation', 'constructor_standings', 'race_year')

# COMMAND ----------

