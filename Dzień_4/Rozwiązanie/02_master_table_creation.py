# Databricks notebook source
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
name = username.split('@')[0].replace('.', '_')
target_catalog= "training_catalog"
target_schema= f"{name}_training"
print(f"Using: '{target_catalog}.{target_schema}' catalog and schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")
spark.sql(f"USE CATALOG {target_catalog}")
spark.sql(f"USE SCHEMA {target_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC Czytanie pliku Users.csv i tworzenie tabeli

# COMMAND ----------

df = (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/Volumes/training_catalog/default/projekt/users.csv"))
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Robię overwrite ponieważ oczekuję że jeżeli ten plik się zaaktualizuje to znaczy ze mamy nową "master date" user'ów. Nie jest to oczywiste

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("users")

# COMMAND ----------

df = (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/Volumes/training_catalog/default/projekt/products.csv"))
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Podobnie jak z Userami...

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("products")

