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

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/databricks101course/raw/

# COMMAND ----------

Constructors_schema = "constructorId INT, constructorRef String, name String, nationality String, url String"

# COMMAND ----------

Constructors_df = spark.read.json(f'{raw_folder_path}/{v_file_date}/constructors.json', schema=Constructors_schema)

# COMMAND ----------

Constructors_df.printSchema()

# COMMAND ----------

display(Constructors_df)

# COMMAND ----------

display(Constructors_df.drop('url'))

# COMMAND ----------

new_df = Constructors_df.withColumnRenamed("constructorId", "constructor_id")\
                        .withColumnRenamed("constructorRef", "constructor_ref")\
                        .withColumn("data_source", lit(v_data_source))\
                        .withColumn("file_date", lit(v_file_date))
                        

# COMMAND ----------

new_df = add_ingestion_date(new_df)

# COMMAND ----------

new_df = new_df.drop('url')

# COMMAND ----------

display(new_df)

# COMMAND ----------

new_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructor")

# COMMAND ----------

dbutils.notebook.exit("Success!")
