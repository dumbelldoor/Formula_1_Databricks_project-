# Databricks notebook source
dbutils.secrets.list(scope = "formula1Scope")

# COMMAND ----------

fromula1_key = dbutils.secrets.get(scope = "formula1Scope", key="Formula1SAStoken")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.databricks101course.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.databricks101course.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.databricks101course.dfs.core.windows.net", fromula1_key)

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


