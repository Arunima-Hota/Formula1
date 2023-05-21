# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
.withColumnRenamed("name","circuit_name") \
.filter("circuit_id < 70")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019") \
.withColumnRenamed("name","race_name")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inner Join

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"inner") \
.select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Outer Joins

# COMMAND ----------

#Left Outer Join
# Find the circuit which do not have the races
# Get all data from circuits_df and where it dont get the matching value simply put null in races_df.
race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"left") \
.select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# Right Outer Join
# It works opposite to the left outer join.What I mean by that, is you're going to have every record from the right side of the join, which is the races dataframe, but the left dataframe, it's going to put nulls where there is no record.So you should see 21 records with some nulls, because we deleted some of the circuits.
race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"right") \
.select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# Full outer join
race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"full") \
.select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Semi Join

# COMMAND ----------

#similar to inner join
#only get data from the left column
race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"semi") \
.select(circuits_df.circuit_name,circuits_df.location,circuits_df.country)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Anti Join

# COMMAND ----------

#Gives the opposite of semi join anti join is going to give you is everything on the left data frame which is not found on the right data frame.
race_circuits_df = races_df.join(circuits_df,circuits_df.circuit_id == races_df.circuit_id,"anti")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cross Join

# COMMAND ----------

#cross joints is basically give you a Cartesian product.What that means is it's going to take every record from the left, joined to the record on the right,and it's going to give you the product of the two.
race_circuits_df = races_df.crossJoin(circuits_df)

# COMMAND ----------

display(race_circuits_df)