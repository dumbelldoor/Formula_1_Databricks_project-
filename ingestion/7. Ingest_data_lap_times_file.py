# Databricks notebook source
# MAGIC
# MAGIC %run "/Workspace/formula1/Includes/configurations"
# MAGIC

# COMMAND ----------

# MAGIC %run "/Workspace/formula1/Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source" , "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

lap_times_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

lap_times_df = spark.read.csv(f"{raw_folder_path}/lap_times", schema=lap_times_schema)

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

new_lap_times_df = lap_times_df.withColumnRenamed("raceId", "race_id")\
                               .withColumnRenamed("driverId", "driver_id")\
                               .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

new_lap_times_df = add_ingestion_date(new_lap_times_df)

# COMMAND ----------

display(new_lap_times_df)

# COMMAND ----------

new_lap_times_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /mnt/databricks101course/processed/lap_times

# COMMAND ----------

dbutils.notebook.exit("Success!")
