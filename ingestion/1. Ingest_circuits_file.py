# Databricks notebook source
# MAGIC %md 
# MAGIC # Ingest circuits.csv File

# COMMAND ----------

# MAGIC %run "../Includes/configurations"
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passing Parameters to Notebooks

# COMMAND ----------

# %python
dbutils.widgets.text("p_data_source" , " ")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# %python
dbutils.widgets.text("p_file_date" , "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

v_data_source

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step1: Read the csv using Spark DataFrame reader

# COMMAND ----------

from pyspark.sql.types import StructType, IntegerType, StringType, DoubleType, StructField
from pyspark.sql.functions import lit

# COMMAND ----------

df = spark.read.csv(f'{raw_folder_path}/{v_file_date}/circuits.csv', inferSchema=True, header=True)

# COMMAND ----------

display(df)

# COMMAND ----------

type(df)

# COMMAND ----------

df.show(n = 10)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df.describe())

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Select Only required Columns

# COMMAND ----------

df = df.select("circuitId","circuitRef","name","location","country","lat","lng","alt")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Another Way!

# COMMAND ----------

df = df.select(df.circuitId, df.circuitRef, df.name, df.location, df.country, df.lat, df.lng, df.alt)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Another Way

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df = df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

df.show()

# COMMAND ----------

col_renamed_df = df.withColumnRenamed("circuitId","circuit_id")\
.withColumnRenamed("circuitRef","circuit_ref")\
.withColumnRenamed("lat","latitude")\
.withColumnRenamed("lng","longitude")\
.withColumnRenamed("alt","altitude")\
.withColumn("data_source", lit(v_data_source))\
.withColumn('file_date', lit(v_file_date))

# COMMAND ----------

display(col_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using Withcolumn

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------



# COMMAND ----------

new_df = add_ingestion_date(col_renamed_df)

# COMMAND ----------

display(new_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DataFrame Writer

# COMMAND ----------

new_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/databricks101course/processed

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/databircks101course/processed/circuits

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

df = spark.read.parquet("/mnt/databircks101course/processed/circuits", mode="overwrite")

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit("Success!")

# COMMAND ----------


