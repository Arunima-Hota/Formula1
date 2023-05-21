-- Databricks notebook source
CREATE DATABASE demo;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE demo;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

USE demo; 

-- COMMAND ----------

SELECT current_database() ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Learning Objective
-- MAGIC 1. create managed table using python
-- MAGIC 2. create managed table using sql
-- MAGIC 3. Dropping the managed table
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE demo;
SHOW TABLES ;

-- COMMAND ----------

DESC EXTENDED race_results_python ;

-- COMMAND ----------

SELECT * FROM demo.race_results_python WHERE race_year = 2020;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo.race_results_sql
AS
SELECT * 
FROM demo.race_results_python 
WHERE race_year = 2020;

-- COMMAND ----------

SELECT * FROM demo.race_results_sql;

-- COMMAND ----------

DESC EXTENDED demo.race_results_sql;

-- COMMAND ----------

DROP TABLE demo.race_results_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Learning Objective
-- MAGIC 1. create external table using python
-- MAGIC 2. create external table using sql
-- MAGIC 3. Dropping the exteral table effect

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_py;

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(
  race_year INT,
  race_name STRING,
  race_date TIMESTAMP,
  circuit_location STRING,
  driver_name STRING,
  driver_number  INT,
  driver_nationality  STRING,
  team  STRING,
  grid  INT,
  fastest_lap INT,
  race_time STRING,
  points  FLOAT,
  position INT,
  created_date TIMESTAMP
)
USING PARQUET
LOCATION "/mnt/devcicddemodl9/presentation/race_results_ext_sql"

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py WHERE race_year = 2020;

-- COMMAND ----------

SELECT count(1) FROM demo.race_results_ext_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql;



-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Views on table
-- MAGIC 1. create temp view
-- MAGIC 2. create global temp view
-- MAGIC 3. create permanent view

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW v_race_results
AS
SELECT *
 FROM demo.race_results_python
 WHERE race_year = 2018;

-- COMMAND ----------

SELECT * FROM v_race_results;

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW gv_race_results
AS
SELECT *
 FROM demo.race_results_python
 WHERE race_year = 2019;

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results;

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_results
AS
SELECT *
 FROM demo.race_results_python
 WHERE race_year = 2014;

-- COMMAND ----------

SHOW TABLES IN demo;