# Databricks notebook source
# MAGIC %run "../Includes/configurations"

# COMMAND ----------

from pyspark.sql.functions import col, sum, count, countDistinct, desc, asc, rank, when
from pyspark.sql.window import Window

# COMMAND ----------

constructor_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(constructor_df.filter("race_year == 2020"))

# COMMAND ----------

constructor_standings_df = constructor_df.groupBy("race_year","team").agg(sum('points').alias("total_points"), count(when(col("position")==1 ,True)).alias("wins"))

# COMMAND ----------

display(constructor_standings_df.filter("race_year == 2020"))

# COMMAND ----------

const_window_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(const_window_spec))

# COMMAND ----------

display(final_df.filter("race_year == 2020"))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")

# COMMAND ----------


