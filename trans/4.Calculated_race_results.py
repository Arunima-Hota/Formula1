# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# %sql
# USE f1_processed;

# COMMAND ----------

spark.sql(f"""
               CREATE table if not exists f1_presentation.calculated_race_results
               (
                race_year INT,
                team_name STRING,
                driver_id INT,
                driver_name STRING,
                race_id INT,
                position INT,
                points INT,
                calculated_points INT,
                created_date TIMESTAMP,
                updated_date TIMESTAMP
               )
               using delta
         """)

# COMMAND ----------

spark.sql(f"""
              CREATE OR REPLACE TEMP VIEW race_result_updated
              as
              SELECT drivers.name as driver_name,
              drivers.driver_id,
              constructors.name as team_name,
              races.race_year,
              races.race_id,
              results.position,
              results.points,
              11 - results.position AS calculated_points
              from f1_processed.results
              JOIN f1_processed.drivers ON(results.driver_id = drivers.driver_id)
              JOIN f1_processed.constructors ON(results.constructor_id = constructors.constructor_id)
              JOIN f1_processed.races ON (results.race_id = races.race_id)
              WHERE results.position <=10
              AND results.file_date = '{v_file_date}';
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM race_result_updated;

# COMMAND ----------

spark.sql(f"""
               MERGE INTO f1_presentation.calculated_race_results target
               USING race_result_updated upd
               ON (target.driver_id = upd.driver_id AND target.race_id = upd.race_id)
               WHEN MATCHED THEN
               UPDATE SET
               target.position = upd.position,
               target.points = upd.points,
               target.calculated_points = upd.calculated_points,
               target.updated_date = current_timestamp
               WHEN NOT MATCHED
               THEN INSERT (
                race_year,
                team_name,
                driver_id,
                driver_name,
                race_id,
                position,
                points,
                calculated_points,
                created_date
                )
                VALUES (
                    race_year,
                    team_name,
                    driver_id,
                    driver_name,
                    race_id,
                    position,
                    points,
                    calculated_points,
                    current_timestamp
                    )
    """)

# COMMAND ----------



# COMMAND ----------

# %sql
# CREATE table f1_presentation.calculated_race_results
# using parquet
# as
# SELECT drivers.name as driver_name,
# constructors.name as team_name,
# races.race_year,
# results.position,
# results.points,
# 11 - results.position AS calculated_points
# from results
# JOIN f1_processed.drivers ON(results.driver_id = drivers.driver_id)
# JOIN f1_processed.constructors ON(results.constructor_id = constructors.constructor_id)
# JOIN f1_processed.races ON (results.race_id = races.race_id)
# WHERE results.position <=10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(1) from race_result_updated; 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(1) FROM f1_presentation.calculated_race_results;

# COMMAND ----------

# %sql
# SELECT * FROM f1_presentation.calculated_race_results where race_year  > 2012;