# Databricks notebook source
spark.conf.set(
    'fs.azure.account.key.databricks101course.dfs.core.windows.net',
    'x1ovI6bOozSSZArohetfAcQ6DKNCKZMoYsTGKhkG29RnJw/PAUT/Gb7VNhFzeZC0tq+cWXNfissX+ASt2pB/gg=='
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


