# Databricks notebook source
# MAGIC %md
# MAGIC ## Access DataFrames using SQL

# COMMAND ----------

# MAGIC %run "../Includes/configurations"

# COMMAND ----------

race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_result_df.createTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(1)
# MAGIC FROM v_race_results
# MAGIC  WHERE race_year = 2020

# COMMAND ----------

p_race_year = 2020

# COMMAND ----------

race_results_2019_df = spark.sql(f"SELECT * FROM v_race_results WHERE race_year = {p_race_year}")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Global TempView 

# COMMAND ----------

race_result_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM global_temp.gv_race_results;
# MAGIC

# COMMAND ----------


