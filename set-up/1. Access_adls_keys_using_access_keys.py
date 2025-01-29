# Databricks notebook source
fromula1Appkey = dbutils.secrets.get(scope = 'formula1Scope', key = 'formula1-App-key')

# COMMAND ----------

spark.conf.set(
    'fs.azure.account.key.databricks101course.dfs.core.windows.net',
    fromula1Appkey
)

# COMMAND ----------

display(dbutils.fs.ls('abfss://demo@databricks101course.dfs.core.windows.net'))

# COMMAND ----------

df = spark.read.csv("abfss://demo@databricks101course.dfs.core.windows.net")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------


