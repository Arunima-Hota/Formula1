# Databricks notebook source
# MAGIC %md
# MAGIC 1. Write data to delta lake (Managed Table)
# MAGIC 2. Write data to delta lake (External Table)
# MAGIC 3. Read data from delta lake (table)
# MAGIC 4. Read data from delta lake (file)
# MAGIC 5. Create a delta table using partition

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION '/mnt/devcicddemodl9/demo1'

# COMMAND ----------

results_df = spark.read \
    .option("inferSchema",True) \
    .json("/mnt/devcicddemodl9/raw1/2021-03-28/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/mnt/devcicddemodl9/demo1/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.results_external
# MAGIC using DELTA
# MAGIC LOCATION "/mnt/devcicddemodl9/demo1/results_external"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_external;

# COMMAND ----------

# External location to data frame
results_external_df = spark.read.format("delta").load("/mnt/devcicddemodl9/demo1/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo.results_partitioned 

# COMMAND ----------

# MAGIC %md
# MAGIC #####1. Update delta table
# MAGIC #####2. Delete data from delta table

# COMMAND ----------

from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark , "/mnt/devcicddemodl9/demo1/results_managed" )
deltaTable.update("position <= 10",{"points" : "21 - position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC  SET points = 11 - position
# MAGIC WHERE position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC WHERE position > 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_demo.results_managed

# COMMAND ----------

from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark , "/mnt/devcicddemodl9/demo1/results_managed" )
deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC #### Upsert using merge

# COMMAND ----------

drivers_day1_df = spark.read \
    .option("inferSchema",True)\
    .json("/mnt/devcicddemodl9/raw1/2021-03-28/drivers.json")\
    .filter("driverId <= 10") \
    .select("driverId","dob","name.forename","name.surname")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day2_df = spark.read \
    .option("inferSchema",True)\
    .json("/mnt/devcicddemodl9/raw1/2021-03-28/drivers.json")\
    .filter("driverId BETWEEN 6 AND 15") \
    .select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day3_df = spark.read \
    .option("inferSchema",True)\
    .json("/mnt/devcicddemodl9/raw1/2021-03-28/drivers.json")\
    .filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20 ") \
    .select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

display(drivers_day3_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge(
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC #### Day1

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge target
# MAGIC USING drivers_day1 upd
# MAGIC ON target.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.dob = upd.dob,
# MAGIC     target.forename = upd.forename,
# MAGIC     target.surname = upd.surname,
# MAGIC     target.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     createdDate
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     current_timestamp
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Day2

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge target
# MAGIC USING drivers_day2 upd
# MAGIC ON target.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.dob = upd.dob,
# MAGIC     target.forename = upd.forename,
# MAGIC     target.surname = upd.surname,
# MAGIC     target.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     createdDate
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     current_timestamp
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

deltaTableDriver = DeltaTable.forPath(spark, '/mnt/devcicddemodl9/demo1/drivers_merge')

deltaTableDriver.alias('target') \
  .merge(
    drivers_day3_df.alias('upd'),
    'target.driverId = upd.driverId'
  ) \
  .whenMatchedUpdate(set =
    {
      "dob": "upd.dob",
      "forename": "upd.forename",
      "surname": "upd.surname",
      "updatedDate": "current_timestamp()"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "upd.driverId",
      "dob": "upd.dob",
      "forename": "upd.forename",
      "surname": "upd.surname",
      "createdDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge ;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. History and Versioning
# MAGIC #### 2. Time-travel
# MAGIC #### 3. Vacuum

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2023-04-25T12:24:26.000+0000';

# COMMAND ----------

 df = spark.read.format("delta").option("timestampAsOf",'2023-04-25T12:24:26.000+0000').load("/mnt/devcicddemodl9/demo1/drivers_merge")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2023-04-25T12:24:26.000+0000';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_merge WHERE driverId=1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 7;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC using f1_demo.drivers_merge VERSION AS OF 7 src
# MAGIC      on (tgt.driverId = src.driverId)
# MAGIC when not matched then
# MAGIC      INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Transaction logs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_txn(
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId = 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE from f1_demo.drivers_txn
# MAGIC WHERE driverId = 1;

# COMMAND ----------

for driver_id in range(3,20):
    spark.sql(f"""INSERT into f1_demo.drivers_txn
    select * FROM f1_demo.drivers_merge
    WHERE driverId = {driver_id}""")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Convert parquet to delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta(
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta

# COMMAND ----------

df = spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save("/mnt/devcicddemodl9/demo1/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet. `/mnt/devcicddemodl9/demo1/drivers_convert_to_delta_new`