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

# %python
dbutils.widgets.text("p_file_date" , "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, col, lit, concat

# COMMAND ----------

name_schema = StructType([StructField("forename", StringType(), True),
                          StructField("surname", StringType(), True)
                          ])

# COMMAND ----------

driver_schema = StructType([StructField("driverId", IntegerType(), True),
                            StructField("driverRef", StringType(), True),
                            StructField("number", IntegerType(), True),
                            StructField("code", StringType(), True),
                            StructField("name", name_schema, True),
                            StructField("dob", DateType(), True),
                           StructField("nationality", StringType(), True),
                           StructField("url", StringType(), True)

])

# COMMAND ----------

drivers_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/drivers.json", schema=driver_schema)

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

new_drivers_df = drivers_df.withColumnRenamed("driverId", "driver_id")\
                            .withColumnRenamed("driverRef", "driver_ref")\
                            .withColumn("data_source", lit(v_data_source))\
                            .withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname')))\
                                .withColumn('file_date', lit(v_file_date))
                            


# COMMAND ----------

new_drivers_df = add_ingestion_date(new_drivers_df)

# COMMAND ----------

display(new_drivers_df)

# COMMAND ----------

final_df = new_drivers_df.drop("url")

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/databricks101course/processed/drivers

# COMMAND ----------

dbutils.notebook.exit("Success!")
