# Databricks notebook source
# MAGIC %md
# MAGIC ####Access Azure DataLake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

formula1dl_accountkey = dbutils.secrets.get(scope = 'formula1-scope',key = 'formula1-accesskey')

# COMMAND ----------

spark.conf.set("fs.azure.account.key.devcicddemodl9.dfs.core.windows.net",formula1dl_accountkey)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@devcicddemodl9.dfs.core.windows.net/test/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@devcicddemodl9.dfs.core.windows.net/test/circuits.csv"))