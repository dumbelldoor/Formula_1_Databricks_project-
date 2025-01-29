# Databricks notebook source
# MAGIC %run "../Includes/configurations"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")
circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")
drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")
Constructors_df = spark.read.parquet(f"{processed_folder_path}/constructor")
result_df = spark.read.parquet(f"{processed_folder_path}/results") 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Races DataFrame

# COMMAND ----------

races_df = races_df.withColumnRenamed("name", "race_name")\
                    .withColumnRenamed("race_timestamp", "race_date")



# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Circuits DataFrame

# COMMAND ----------

circuits_df = circuits_df.withColumnRenamed("location", "circuit_location")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Drivers DataFrame

# COMMAND ----------

drivers_df = drivers_df.withColumnRenamed("name", "driver_name")\
                       .withColumnRenamed("number", "driver_number")\
                       .withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Constructors DataFrame

# COMMAND ----------

Constructors_df = Constructors_df.withColumnRenamed("name", "team")

# COMMAND ----------

display(Constructors_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Rasult DataFrame

# COMMAND ----------

result_df = result_df.withColumnRenamed("time", "race_time")

# COMMAND ----------

display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1st Join 

# COMMAND ----------

races_circuit_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, how='inner')\
    .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

display(races_circuit_df)

# COMMAND ----------

race_result_df = result_df.join(races_circuit_df, result_df.race_id == races_circuit_df.race_id)\
                          .join(drivers_df, result_df.driver_id == drivers_df.driver_id)\
                          .join(Constructors_df, result_df.constructor_id == Constructors_df.constructor_id)
                          

# COMMAND ----------

final_df = race_result_df.select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points", "position")\
    .withColumn("current_date", current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------


