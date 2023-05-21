-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

SELECT * , concat(driver_ref,'-',code ) AS new_driver_ref FROM drivers;

-- COMMAND ----------

SELECT * , split(name,' ' )[0] forename ,split(name,' ' )[1] surname FROM drivers;

-- COMMAND ----------

SELECT * , current_timestamp() FROM drivers;

-- COMMAND ----------

SELECT * , date_format(dob,'dd-MM-yyyy' ) FROM drivers;

-- COMMAND ----------

SELECT * , date_add(dob,1 ) FROM drivers;

-- COMMAND ----------

SELECT count(*) FROM drivers;

-- COMMAND ----------

SELECT max(dob) FROM drivers;

-- COMMAND ----------

SELECT *  FROM drivers WHERE dob = '2000-05-11';

-- COMMAND ----------

SELECT count(*) FROM drivers WHERE nationality = 'British';

-- COMMAND ----------

SELECT nationality,count(*) FROM drivers GROUP BY nationality,number ORDER BY nationality;

-- COMMAND ----------

SELECT nationality,count(*) FROM drivers GROUP BY nationality,number having count(*) > 100 ORDER BY nationality;

-- COMMAND ----------


select nationality,name,dob,RANK() OVER(PARTITION BY nationality order by dob desc)as age_rank
from drivers
order by nationality,age_rank;