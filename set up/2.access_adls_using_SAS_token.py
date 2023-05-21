# Databricks notebook source
# MAGIC %md
# MAGIC ####Access Azure DataLake using SAS Token
# MAGIC 1. Set the spark config for SAS Token
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.devcicddemodl9.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.devcicddemodl9.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.devcicddemodl9.dfs.core.windows.net", "sp=rl&st=2023-03-15T05:47:06Z&se=2023-03-15T13:47:06Z&spr=https&sv=2021-12-02&sr=c&sig=BXP%2BI1ynsKxzW%2FnbOg4OaqG%2FewvVvIdyTPFXYzRXsxE%3D")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@devcicddemodl9.dfs.core.windows.net/test/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@devcicddemodl9.dfs.core.windows.net/test/circuits.csv"))