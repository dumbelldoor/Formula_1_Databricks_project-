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

pit_stops_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pit_stops_df = spark.read.json(f"{raw_folder_path}/pit_stops.json", schema=pit_stops_schema, multiLine=True)

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

new_pit_stops_df = pit_stops_df.withColumnRenamed("raceId", "race_id")\
                               .withColumnRenamed("driverId", "driver_id")\
                               .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

new_pit_stops_df = add_ingestion_date(new_pit_stops_df)

# COMMAND ----------

new_pit_stops_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /mnt/databricks101course/processed/pit_stops

# COMMAND ----------

dbutils.notebook.exit("Success!")
