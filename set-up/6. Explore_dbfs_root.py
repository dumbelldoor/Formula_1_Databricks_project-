# Databricks notebook source
display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

df = spark.read.csv('dbfs:/FileStore/circuits.csv')

# COMMAND ----------

display(df)

# COMMAND ----------


