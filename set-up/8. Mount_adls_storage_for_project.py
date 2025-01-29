# Databricks notebook source


# COMMAND ----------

def adls_mount(storage_account_name, container_name):
    client_id = dbutils.secrets.get(scope = "formula1Scope", key = "formula1ClientId")

    tenent_id = dbutils.secrets.get(scope = "formula1Scope", key = "formula1TenentId")

    secret_id = dbutils.secrets.get(scope = "formula1Scope", key = "fromula1SecretId")

    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": secret_id,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenent_id}/oauth2/token"}
    
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")


    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    
    display(dbutils.fs.mounts())


# COMMAND ----------

adls_mount('databricks101course','raw')

# COMMAND ----------

adls_mount('databricks101course','processed')

# COMMAND ----------

adls_mount('databricks101course','presentation')

# COMMAND ----------

adls_mount('databricks101course','tokyo-olympic-data')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/databricks101course/

# COMMAND ----------


