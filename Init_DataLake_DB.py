# Databricks notebook source
lakeAccountName = dbutils.secrets.get(scope = "Databricks_Sellout", key = "lakeAccountName")

applicationClientId = dbutils.secrets.get(scope = "Databricks_Sellout", key = "applicationClientId")

directoryTenantId = dbutils.secrets.get(scope = "Databricks_Sellout", key = "directoryTenantId")

secret = dbutils.secrets.get(scope = "Databricks_Sellout", key = "secret")

# COMMAND ----------

mounts = list(map(lambda mount: mount.mountPoint, dbutils.fs.mounts()))

analyticsplatformConfigs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": applicationClientId,
       "fs.azure.account.oauth2.client.secret": secret,
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/"+directoryTenantId+"/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

  #Prelanding Mount

if not "/mnt/prelandingzone/Brazil" in mounts:
  try:
    dbutils.fs.mount(
      source = "abfss://prelandingzone@"+lakeAccountName+".dfs.core.windows.net/Brazil",
      mount_point = "/mnt/prelandingzone/Brazil",
      extra_configs = analyticsplatformConfigs)
  except Exception as error:
    if ('Directory already mounted' not in str(error)):
      raise error
  
  #Landing Mount

if not "/mnt/landingzone/Brazil" in mounts:
  try:
    dbutils.fs.mount(
      source = "abfss://landingzone@"+lakeAccountName+".dfs.core.windows.net/Brazil",
      mount_point = "/mnt/landingzone/Brazil",
      extra_configs = analyticsplatformConfigs)
  except Exception as error:
    if ('Directory already mounted' not in str(error)):
      raise error

  #History Mount

if not "/mnt/historyzone/Brazil" in mounts:
  try:
    dbutils.fs.mount(
      source = "abfss://historyzone@"+lakeAccountName+".dfs.core.windows.net/Brazil",
      mount_point = "/mnt/historyzone/Brazil",
      extra_configs = analyticsplatformConfigs)
  except Exception as error:
    if ('Directory already mounted' not in str(error)):
      raise error

    #Transformed Mount

if not "/mnt/transformedzone/Brazil" in mounts:
  try:
    dbutils.fs.mount(
      source = "abfss://transformedzone@"+lakeAccountName+".dfs.core.windows.net/Brazil",
      mount_point = "/mnt/transformedzone/Brazil",
      extra_configs = analyticsplatformConfigs)
  except Exception as error:
    if ('Directory already mounted' not in str(error)):
      raise error
  
  #Consume Mount

if not "/mnt/consumezone/Brazil" in mounts:
  try:
    dbutils.fs.mount(
      source = "abfss://consumezone@"+lakeAccountName+".dfs.core.windows.net/Brazil",
      mount_point = "/mnt/consumezone/Brazil",
      extra_configs = analyticsplatformConfigs)
  except Exception as error:
    if ('Directory already mounted' not in str(error)):
      raise error
