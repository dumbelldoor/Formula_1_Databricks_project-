# Databricks notebook source
spark.read.json("/mnt/databricks101course/raw/2021-03-21/results.json").createOrReplaceTempView("results_cutover")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  raceId, COUNT(1)
# MAGIC FROM results_cutover
# MAGIC GROUP BY raceId
# MAGIC ORDER BY raceId DESC

# COMMAND ----------

spark.read.json("/mnt/databricks101course/raw/2021-03-28/results.json").createOrReplaceTempView("results_w1")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  raceId, COUNT(1)
# MAGIC FROM results_w1
# MAGIC GROUP BY raceId
# MAGIC ORDER BY raceId DESC

# COMMAND ----------

spark.read.json("/mnt/databricks101course/raw/2021-04-18/results.json").createOrReplaceTempView("results_w2")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  raceId, COUNT(1)
# MAGIC FROM results_w2
# MAGIC GROUP BY raceId
# MAGIC ORDER BY raceId DESC

# COMMAND ----------

# MAGIC
# MAGIC %run "/Workspace/formula1/Includes/configurations"
# MAGIC

# COMMAND ----------

# MAGIC %run "/Workspace/formula1/Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source" , "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date" , "2021-4-18")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, concat, lit

# COMMAND ----------

# MAGIC %md
# MAGIC "resultId":1,"raceId":18,"driverId":1,"constructorId":1,"number":22,"grid":1,"position":1,"positionText":1,"positionOrder":1,"points":10,"laps":58,

# COMMAND ----------

# MAGIC %md
# MAGIC "time":"1:34:50.616","milliseconds":5690616,"fastestLap":39,"rank":2,"fastestLapTime":"1:27.452","fastestLapSpeed":218.3,"statusId":1

# COMMAND ----------

result_schema = StructType([StructField("resultId", IntegerType(), True),
                            StructField("raceId", IntegerType(), True),
                            StructField("driverId", IntegerType(), True),
                            StructField("constructorId", IntegerType(), True),
                            StructField("number", IntegerType(), True),
                            StructField("grid", IntegerType(), True),
                            StructField("position", IntegerType(), True),
                            StructField("positionText", StringType(), True),
                            StructField("positionOrder", IntegerType(), True),
                            StructField("points",FloatType() , True),
                            StructField("laps", IntegerType(), True),
                            StructField("time", StringType(), True),
                            StructField("milliseconds", IntegerType(), True),
                            StructField("fastestLap", IntegerType(), True),
                            StructField("rank", IntegerType(), True),
                            StructField("fastestLapTime", StringType(), True),
                            StructField("fastestLapSpeed", FloatType(), True),
                            StructField("statusId", IntegerType(), True)

])

# COMMAND ----------

result_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/results.json", schema=result_schema)

# COMMAND ----------

display(result_df)

# COMMAND ----------

result_df.printSchema()

# COMMAND ----------

new_result_df = result_df.withColumnRenamed("resultId", "result_id")\
                        .withColumnRenamed("raceId", "race_id")\
                        .withColumnRenamed("driverId", "driver_id")\
                        .withColumnRenamed("constructorId", "constructor_id")\
                        .withColumnRenamed("positionText", "position_text")\
                        .withColumnRenamed("positionOrder", "position_order")\
                        .withColumnRenamed("fastestLap", "fastest_lap")\
                        .withColumnRenamed("fastestLapTime", "fastest_lap_time")\
                        .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")\
                        .withColumn("data_source", lit(v_data_source))\
                        .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

new_result_df = add_ingestion_date(new_result_df)

# COMMAND ----------

new_result_df.printSchema()

# COMMAND ----------

final_df = new_result_df.drop("statusId")

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 1

# COMMAND ----------

for race_id_list in final_df.select("race_id").distinct().collect():
    if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
        spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

final_df.write.mode("append").partitionBy("race_id").format('parquet').saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 2

# COMMAND ----------




# COMMAND ----------

dbutils.notebook.exit("Success!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------


