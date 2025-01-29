# Databricks notebook source
# MAGIC %run "../Includes/configurations"

# COMMAND ----------

from pyspark.sql.functions import sum, countDistinct, when, count, col, desc, rank
from pyspark.sql.window import Window
race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

grouped_df = race_results_df.groupBy("race_year","driver_name", "driver_nationality", "team").agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(grouped_df.filter("race_year == 2020"))

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = grouped_df.withColumn("rank", rank().over(driver_rank_spec))


# COMMAND ----------

display(final_df.filter("race_year == 2020"))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")

# COMMAND ----------


