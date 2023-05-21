# Databricks notebook source
spark.read.json("/mnt/devcicddemodl9/raw1/2021-03-21/results.json").createOrReplaceTempView("results_cutover")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT raceId,count(1)
# MAGIC from results_cutover
# MAGIC group by raceId
# MAGIC ORDER BY raceId DESC;

# COMMAND ----------

spark.read.json("/mnt/devcicddemodl9/raw1/2021-03-28/results.json").createOrReplaceTempView("results_w1")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT raceId,count(1)
# MAGIC from results_w1
# MAGIC group by raceId
# MAGIC ORDER BY raceId DESC;

# COMMAND ----------

spark.read.json("/mnt/devcicddemodl9/raw1/2021-04-18/results.json").createOrReplaceTempView("results_w2")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT raceId,count(1)
# MAGIC from results_w2
# MAGIC group by raceId
# MAGIC ORDER BY raceId DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC #### ingest results.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 1: Read the json file to spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType

# COMMAND ----------

results_schema= StructType(fields= [StructField("resultId",IntegerType(),False),
StructField("raceId",IntegerType(),True),
StructField("driverId",IntegerType(),True),
StructField("constructorId",IntegerType(),True),
StructField("number",IntegerType(),True),
StructField("grid",IntegerType(),True),
StructField("position",IntegerType(),True),
StructField("positionText",StringType(),True),
StructField("positionOrder",IntegerType(),True),
StructField("points",FloatType(),True),
StructField("laps",IntegerType(),True),
StructField("time",StringType(),True),
StructField("milliseconds",IntegerType(),True),
StructField("fastestLap",IntegerType(),True),
StructField("rank",IntegerType(),True),
StructField("fastestLapTime",StringType(),True),
StructField("fastestLapSpeed",FloatType(),True),
StructField("statusId",StringType(),True)
])

# COMMAND ----------

results_df = spark.read \
    .schema(results_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 2 : Rename columns and add new column

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

results_with_columns1_df = results_df.withColumnRenamed("resultId","result_id") \
.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverId","driver_id") \
.withColumnRenamed("constructorId","constructor_id") \
.withColumnRenamed("positionText","position_text")\
.withColumnRenamed("positionOrder","position_order") \
.withColumnRenamed("fastestLap","fastest_lap") \
.withColumnRenamed("fastestLapTime","fastest_lap_time") \
.withColumnRenamed("fastestLapSpeed","fastest_lap_speed") \
.withColumn("data_source",lit(v_data_source)) \
.withColumn("file_date",lit(v_file_date))


# COMMAND ----------

results_with_columns_df = add_ingestion_date(results_with_columns1_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3: Drop the unwanted column

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_final_df = results_with_columns_df.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### De-dupe the dataframe

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 : Write the output in parquet format

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 1

# COMMAND ----------

#  for race_id_list in results_final_df.select ("race_id").distinct().collect():
#      if(spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#          spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id ={race_id_list.race_id})")

# COMMAND ----------

#  results_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method 2 Incremental Load

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE f1_processed.results;

# COMMAND ----------

# def rearrange (input_df,partition_column):
# # partition_column='race_id'
# column_list= []
# for column_name in input_df.schema.names:
#     if column_name != partition_column:
#         column_list.append(column_name)
# column_list.append(partition_column)
# output_df=input_df.select(column_list)
# return output_df


# COMMAND ----------

# def partition_overwrite(input_df,db_name,table_name,partition_column):
#     output_df=rearrange(input_df,partition_column)
#     spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
#     if(spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
#         output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
#     else:
#         output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")


# COMMAND ----------

output_df = re_arrange_partition_column(results_final_df,'race_id')

# COMMAND ----------

# overwrite_partition(results_final_df,'f1_processed','results','race_id')

# COMMAND ----------

#  Delta Merge condition becoz partitionOverwriteMode does not support by delta
merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df,'f1_processed','results',processed_folder_path,merge_condition,'race_id')

# COMMAND ----------

# spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

#  results_final_df = results_final_df.select("result_id","driver_id","constructor_id","number","grid","position","position_text","position_order","points","laps","time","milliseconds","fastest_lap","rank","fastest_lap_time","fastest_lap_speed","data_source","file_date","ingestion_date","race_id")

# COMMAND ----------

#  if(spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     results_final_df.write.mode("overwrite").insertInto("f1_processed.results")
# else:
#     results_final_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id,count(1)
# MAGIC FROM f1_processed.results
# MAGIC group by race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(1) from f1_processed.results where file_date ='2021-03-21';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id,driver_id, count(1)
# MAGIC FROM f1_processed.results
# MAGIC group by race_id,driver_id
# MAGIC ORDER BY race_id,driver_id DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_processed.results where race_id = 540 AND driver_id = 229;