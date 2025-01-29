# Databricks notebook source
# MAGIC %run "../Includes/configurations"

# COMMAND ----------

df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL WAY

# COMMAND ----------

races_filter_df = df.filter("race_year = 2019 and round <= 5").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Pythonic Way

# COMMAND ----------

races_filter_df = df.filter((df["race_year"] == 2019) & (df["round"] <= 5))
display(races_filter_df)

# COMMAND ----------


