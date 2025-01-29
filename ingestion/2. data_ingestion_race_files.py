# Databricks notebook source
# MAGIC
# MAGIC %run "../Includes/configurations"
# MAGIC

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source" , "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# %python
dbutils.widgets.text("p_file_date" , "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_data_source

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/databricks101course/raw/

# COMMAND ----------

from pyspark.sql.functions import lit, col, to_timestamp, current_timestamp, concat

# COMMAND ----------

df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/races.csv", inferSchema=True, header=True)

# COMMAND ----------

display(df)

# COMMAND ----------

new_df = df.select(col("raceId"),col("year"), col("round"), col("circuitId"),col("name"), col("date"), col("time"))

# COMMAND ----------

display(new_df)

# COMMAND ----------

new_df = add_ingestion_date(new_df)

# COMMAND ----------

display(new_df)

# COMMAND ----------

pre_df = new_df.withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("year", "race_year")\
    .withColumnRenamed("circuitId", "circuit_id")\
    .withColumn("data_source", lit(v_data_source))\
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(pre_df)

# COMMAND ----------

pre_df.printSchema()

# COMMAND ----------

final_df = pre_df.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").partitionBy("race_year").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/databricks101course/processed/races

# COMMAND ----------

spark.read.parquet(f"{processed_folder_path}/races").display()

# COMMAND ----------

dbutils.notebook.exit("Success!")
