# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def overWrite_data(df,results):
    if (df.write.mode("overwrite").inserInto(f"f1_processed.{results}"))
        return df
    else:
        
