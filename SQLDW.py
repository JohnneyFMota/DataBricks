# Databricks notebook source
# Configuração do blob temporario do SQL DW
storage_account_name = dbutils.secrets.get(scope = "Databricks_Sellout", key = "SelloutAccountName")
storage_account_key = dbutils.secrets.get(scope = "Databricks_Sellout", key = "SelloutAccessKey")

storage_container_name = "temporario"

temp_dir_url = "wasbs://{}@{}.blob.core.windows.net/".format(storage_container_name, storage_account_name)

spark_config_key = "fs.azure.account.key.{}.blob.core.windows.net".format(storage_account_name)
spark_config_value = storage_account_key

spark.conf.set(spark_config_key, spark_config_value)

# configuração de acesso ao SQL DW
servername = "dwsrv01"
port = "1433"
databasename = "ambevdw"
username = dbutils.secrets.get(scope = "keyVault", key = "dwUserName")
password = dbutils.secrets.get(scope = "keyVault", key = "dwPassword")

sql_dw_connection_string = "jdbc:sqlserver://{}.database.windows.net:{};database={};user={};password={};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;".format(servername, port, databasename, username, password)

# COMMAND ----------

def DWQuery(query):
  return spark.read.format("com.databricks.spark.sqldw").option("url", sql_dw_connection_string).option("forward_spark_azure_storage_credentials", "true").option("tempdir", temp_dir_url).option("query", query).load()