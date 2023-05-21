-- Databricks notebook source
USE f1_presentation;

-- COMMAND ----------

DESC driver_standings

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2018
AS
SELECT race_year,driver_name,driver_nationality,team,total_points,wins,rank
 FROM driver_standings
 WHERE race_year = 2018;

-- COMMAND ----------

SELECT * FROM v_driver_standings_2018;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2020
AS
SELECT race_year,driver_name,driver_nationality,team,total_points,wins,rank
 FROM driver_standings
 WHERE race_year = 2020;

-- COMMAND ----------

SELECT * FROM v_driver_standings_2020;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### inner join

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
INNER JOIN v_driver_standings_2020 d_2020
ON (d_2018.team = d_2020.team);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### left join

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
LEFT JOIN v_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name);

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
RIGHT JOIN v_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### full join

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
FULL JOIN v_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name);

-- COMMAND ----------

-- MAGIC %md semi join (GET DATA FROM LEFT)

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
SEMI JOIN v_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name);

-- COMMAND ----------

-- MAGIC %md anti join

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
ANTI JOIN v_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name);

-- COMMAND ----------

-- MAGIC %md cross join

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
CROSS JOIN v_driver_standings_2020 d_2020


-- COMMAND ----------

-- MAGIC %md we got the inner join, which will join the two tables on the condition and give you one.
-- MAGIC The matching records and left will join to the right, but you'll get everything from the left and the values from the right if they are matched. Otherwise you get nulls.
-- MAGIC But in the case of right, it's the other way round to the left you get everything from right and the matching data from left and 
-- MAGIC full outer join is like a superset of both of them 
-- MAGIC and the semi join is similar to inner join, except that the columns are limited but the records are same as the inner join 
-- MAGIC anti join is where actually you get everything which is in left but not in right.
-- MAGIC And the cross join is everything from left Join to the right and you get a Cartesian product of the two.