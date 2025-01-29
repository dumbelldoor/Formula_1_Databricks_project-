# Databricks notebook source
display(dbutils.fs.mounts())

# COMMAND ----------

client_id = dbutils.secrets.get(scope = "formula1Scope", key = "formula1ClientId")

tenent_id = dbutils.secrets.get(scope = "formula1Scope", key = "formula1TenentId")

secret_id = dbutils.secrets.get(scope = "formula1Scope", key = "fromula1SecretId")


# COMMAND ----------

# spark.conf.set("fs.azure.account.auth.type.databricks101course.dfs.core.windows.net", "OAuth")
# spark.conf.set("fs.azure.account.oauth.provider.type.databricks101course.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set("fs.azure.account.oauth2.client.id.databricks101course.dfs.core.windows.net", client_id)
# spark.conf.set("fs.azure.account.oauth2.client.secret.databricks101course.dfs.core.windows.net", secret_id)
# spark.conf.set("fs.azure.account.oauth2.client.endpoint.databricks101course.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenent_id}/oauth2/token")

# COMMAND ----------


configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": secret_id,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenent_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
    source = "abfss://demo@databricks101course.dfs.core.windows.net/",
    mount_point = "/mnt/databricks101course/demo",
    extra_configs = configs
)

# COMMAND ----------

display(dbutils.fs.ls('/mnt/databricks101course/demo'))

# COMMAND ----------

df = spark.read.csv("abfss://demo@databricks101course.dfs.core.windows.net")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------


