# Databricks notebook source
# MAGIC %md
# MAGIC #### ingest drivers.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ###### step 1 : Read the json file using spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ####### inner json object first next the outer one

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename",StringType(),True),
                                StructField("surname",StringType(),True)])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId",IntegerType(),False),
                                    StructField("driverRef",StringType(),True),
                                    StructField("number",IntegerType(),True),
                                    StructField("code",StringType(),True),
                                    StructField("name",name_schema),
                                    StructField("dob",DateType(),True),
                                    StructField("nationality",StringType(),True),
                                    StructField("url",StringType(),True)])

# COMMAND ----------

drivers_df = spark.read \
.schema(drivers_schema) \
.json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### step 2 : Rename the columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import col,concat,lit

# COMMAND ----------

drivers_with_column1_df = drivers_df.withColumnRenamed("driverId","driver_id")\
                                   .withColumnRenamed("driverRef","driver_ref") \
                                   .withColumn("name",concat(col("name.forename"), lit(" "), col("name.surname"))) \
                                    .withColumn("data_source",lit(v_data_source))\
                                    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

drivers_with_column_df = add_ingestion_date(drivers_with_column1_df)

# COMMAND ----------

display(drivers_with_column_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 3: Drop the unwanted column

# COMMAND ----------

drivers_final_df = drivers_with_column_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 4: Write output to parquet file

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("Success")