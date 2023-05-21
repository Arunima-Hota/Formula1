# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure data lake container for the project

# COMMAND ----------

def mount_adls(storage_account_name,container_name):
    # get secrets from key vault
    client_id = dbutils.secrets.get(scope = 'clientid',key = 'clientId')
    directory_id = dbutils.secrets.get(scope = 'clientid',key = 'directoryId')
    client_secret = dbutils.secrets.get(scope = 'clientsecretNew',key = 'clientsecret-sp')
    
    #set the spark configuration
    configs = {"fs.azure.account.auth.type": "OAuth",
               "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
               "fs.azure.account.oauth2.client.id": client_id,
               "fs.azure.account.oauth2.client.secret": client_secret,
               "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token"}
    
    #unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
        
    #Mount the storage account container
    dbutils.fs.mount(
                     source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
                     mount_point = f"/mnt/{storage_account_name}/{container_name}",
                     extra_configs = configs
                    )
    
    display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Mount raw container

# COMMAND ----------

mount_adls('devcicddemodl9','raw1')

# COMMAND ----------

mount_adls('devcicddemodl9','processed1')

# COMMAND ----------

mount_adls('devcicddemodl9','presentation')

# COMMAND ----------

dbutils.fs.ls("/mnt/devcicddemodl9/presentation")

# COMMAND ----------

mount_adls('devcicddemodl9','demo1')