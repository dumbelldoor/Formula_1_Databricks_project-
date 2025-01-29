# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = "formula1Scope")

# COMMAND ----------

formula1_secret_key = dbutils.secrets.get(scope= 'formula1Scope', key = 'formula1-App-key')

# COMMAND ----------


