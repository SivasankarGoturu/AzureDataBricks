# Databricks notebook source
# dbutils.fs.mount(source = 'wasbs://inputdata@storagesiva12.blob.core.windows.net',mount_point = '/mnt/sales',extra_configs={'fs.azure.account.key.storagesiva12.blob.core.windows.net':'WwZmPEZ0uWGzYVmYR1WopDB9ybQu4yuwC4pWpwNdbNLax6X/SS0yRkEObN8jEppvocPWRu/yktpJ+ASt01rW9w=='})

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

jdbc_url = "jdbc:sqlserver://learn-server-e.database.windows.net:1433;databaseName=retail"
connection_properties = {
    "user": "siva",
    "password": "Hello@345",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}


# COMMAND ----------

df = spark.read \
    .jdbc(url=jdbc_url, table="SalesLT.Customer", properties=connection_properties)


# COMMAND ----------

display(df)

# COMMAND ----------

df.write.parquet("dbfs:/mnt/sales/landing", mode="overwrite")

# COMMAND ----------


