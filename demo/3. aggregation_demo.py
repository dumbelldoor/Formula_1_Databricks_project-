# Databricks notebook source
# MAGIC %run "../Includes/configurations"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_result_df)

# COMMAND ----------

demo_df = race_result_df.filter("race_year = 2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

demo_df.select(sum("points")).show()

# COMMAND ----------

demo_df.filter(demo_df.driver_name == "Lewis Hamilton").select(sum("points")).show()

# COMMAND ----------

demo_df.filter(demo_df.driver_name == "Lewis Hamilton").select(sum("points"), countDistinct("race_name")).show()

# COMMAND ----------

group_demo_df = demo_df.groupBy("driver_name").agg(sum("points").alias("total_points"), countDistinct("race_name").alias("total_races"))

# COMMAND ----------

display(group_demo_df)

# COMMAND ----------

demo_df = race_result_df.filter("race_year in (2019, 2020)")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

grouped_df = demo_df.groupBy("race_year", "driver_name").agg(sum("points").alias("total_points"), countDistinct("race_name").alias("total_races"))

# COMMAND ----------

display(grouped_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"))
grouped_df.withColumn("rank", rank().over(driver_rank_spec)).show()

# COMMAND ----------

display(grouped_df)

# COMMAND ----------


