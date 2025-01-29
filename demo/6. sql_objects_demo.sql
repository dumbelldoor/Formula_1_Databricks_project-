-- Databricks notebook source
CREATE DATABASE demo;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED default;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Managed Tables

-- COMMAND ----------

-- MAGIC %run "../Includes/configurations"

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("delta").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE demo;
SHOW TABLES

-- COMMAND ----------

DESC EXTENDED race_results_python;

-- COMMAND ----------

Select * FROM demo.race_results_python
WHERE race_year = 2020
ORDER BY points DESC;

-- COMMAND ----------

CREATE TABLE race_results_sql
AS
Select * FROM demo.race_results_python
WHERE race_year = 2020
ORDER BY points DESC;

-- COMMAND ----------

DESC EXTENDED race_results_sql;

-- COMMAND ----------

DROP TABLE IF EXISTS race_results_sql;

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # External Tables

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql(
  race_year INT,
  race_name STRING,
  race_date TIMESTAMP,
  circuit_location STRING,
  driver_name STRING,
  driver_number INT,
  driver_nationality STRING,
  team STRING,
  grid INT,
  fastest_lap INT,
  race_time STRING,
  points FLOAT,
  position INT,
  current_date TIMESTAMP
)
USING parquet
LOCATION '/mnt/databricks101course/presentation/race_results_ext_sql'

-- COMMAND ----------

SET spark.sql.legacy.allowNonEmptyLocationInCTAS = true

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option('path', f'{presentation_folder_path}/race_results_ext_py').saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_py

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py WHERE race_year = 2020;

-- COMMAND ----------

SELECT * FROM demo.race_results_ext_sql;

-- COMMAND ----------

SELECT COUNT(1) FROM demo.race_results_ext_sql

-- COMMAND ----------

SHOW TABLES  IN demo;

-- COMMAND ----------

DROP TABLE IF EXISTS demo.race_results_ext_sql;

-- COMMAND ----------

SHOW TABLES  IN demo;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Views

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW TV_race_results_python
AS 
SELECT * FROM demo.race_results_python 
WHERE race_year = 2019;

-- COMMAND ----------

SELECT * FROM TV_race_results_python;

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS 
SELECT * FROM race_results_python 
WHERE race_year = 2019;

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results;

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_results
AS 
SELECT * FROM race_results_python
WHERE race_year = 2000;

-- COMMAND ----------

DROP VIEW IF EXISTS race_results_python;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

SHOW TABLES IN default;

-- COMMAND ----------


