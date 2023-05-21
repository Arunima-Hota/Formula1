-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Circuits table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits
(
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
using csv
options (path "/mnt/devcicddemodl9/raw1/circuits.csv" , header true)

-- COMMAND ----------

SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races
(
 raceId INT,
 year INT,
 round INT,
 circuitId INT,
 name STRING,
 date DATE,
 time STRING,
 url STRING
)
using csv
options (path "/mnt/devcicddemodl9/raw1/races.csv" , header true)

-- COMMAND ----------

SELECT * FROM f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create table for JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### create constructors table
-- MAGIC . single line JSON
-- MAGIC . Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors
(
 constructorId INT, 
 constructorRef STRING,
 name STRING,
 nationality STRING,
 url STRING
)
using json
options (path "/mnt/devcicddemodl9/raw1/constructors.json" , header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### create drivers table
-- MAGIC ####### simple line,complex structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers
(
 driverId INT,
 driverRef STRING,
 number INT,
 code STRING,
 name STRUCT<forename: STRING, surname: STRING >,
 dob DATE,
 nationality STRING,
 url STRING
)
using json
options (path "/mnt/devcicddemodl9/raw1/drivers.json" , header true)

-- COMMAND ----------

SELECT * FROM f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### create results table
-- MAGIC simple line, simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results
(
resultId Int,
raceId Int,
driverId Int,
constructorId Int,
number Int,
grid Int,
position Int,
positionText String,
positionOrder Int,
points Float,
laps Int,
time String,
milliseconds Int,
fastestLap  Int,
rank Int,
fastestLapTime String,
fastestLapSpeed Float,
statusId String
)
using json
options (path "/mnt/devcicddemodl9/raw1/results.json" , header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### create pit stops table
-- MAGIC multi line, simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops
(
raceId Int,
driverId Int,
stop String,
lap Int,
time String,
duration String,
milliseconds Int
)
using json
options (path "/mnt/devcicddemodl9/raw1/pit_stops.json" , header true , multiLine true)

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create tables for list of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Create lap times table
-- MAGIC 1. CSV FILE
-- MAGIC 2. MULTIPLE FILES

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times
(
raceId Integer,
driverId Integer,
lap Integer,
position Integer,
time String,
milliseconds Integer
)
using json
options (path "/mnt/devcicddemodl9/raw1/lap_times" , header true )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Qualifying table
-- MAGIC 1. JSON FILE
-- MAGIC 2. MULTILINE JSON
-- MAGIC 3. MULTIPLE FILES

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying
(
qualifyId Integer,
raceId Integer,
driverId Integer,
constructorId Integer,
number Integer,
position Integer,
q1 String,
q2 String,
q3 String
)
using json
options (path "/mnt/devcicddemodl9/raw1/qualifying" , header true ,multiLine true )

-- COMMAND ----------

DESC EXTENDED f1_raw.qualifying;