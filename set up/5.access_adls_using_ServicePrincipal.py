# Databricks notebook source
# MAGIC %md
# MAGIC ####Access Azure DataLake using ServicePricipal
# MAGIC 1. Get the client id,secret and directory id from key vault
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'clientid',key = 'clientId')
directory_id = dbutils.secrets.get(scope = 'clientid',key = 'directoryId')
client_secret = dbutils.secrets.get(scope = 'clientsecretNew',key = 'clientsecret-sp')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@devcicddemodl9.dfs.core.windows.net/",
  mount_point = "/mnt/formula1/demo",
  extra_configs = configs
)

# COMMAND ----------

display(dbutils.fs.ls("mnt/formula1/demo/test"))

# COMMAND ----------

dbutils.fs.mounts()