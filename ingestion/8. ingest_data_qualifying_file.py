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

qualifying_schema = StructType([StructField("qualifyId", IntegerType(), True),
                                StructField("raceId", IntegerType(), True),
                                StructField("driverId", IntegerType(),True),
                                StructField("constructorId", IntegerType(),True),
                                StructField("number", IntegerType(),True),
                                StructField("position", IntegerType(),True),
                                StructField("q1", StringType(),True),
                                StructField("q2", StringType(),True),
                                StructField("q3", StringType(),True),

])

# COMMAND ----------

qualifying_df = spark.read.json(f"{raw_folder_path}/qualifying", schema=qualifying_schema, multiLine=True)

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

final_qualifying_df = qualifying_df.withColumnRenamed("qualifyId", "qualifying_id")\
                                   .withColumnRenamed("raceId", "race_id")\
                                   .withColumnRenamed("driverId", "driver_id")\
                                   .withColumnRenamed("constructorId", "constructor_id")\
                                   .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

final_qualifying_df = add_ingestion_date(final_qualifying_df)

# COMMAND ----------

display(final_qualifying_df)

# COMMAND ----------

final_qualifying_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/databricks101course/processed/qualifying
# MAGIC

# COMMAND ----------

dbutils.notebook.exit("Success!")
