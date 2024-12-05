# Databricks notebook source
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
name = username.split('@')[0].replace('.', '_')
target_catalog= "training_catalog"
target_schema= f"{name}_training"
print(f"Using: '{target_catalog}.{target_schema}' catalog and schema")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")
spark.sql(f"USE CATALOG {target_catalog}")
spark.sql(f"USE SCHEMA {target_schema}")
table_name = "weather_pl"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wczytanie danych

# COMMAND ----------

path_data = "/Volumes/acxiom_szkolenie_sda/weather/sample_weather_data/weather_data_2022.csv"

data = (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(path_data))

# COMMAND ----------

# MAGIC %md
# MAGIC Wersja w Spark SQL:

# COMMAND ----------

# %sql
# CREATE OR REPLACE TEMPORARY VIEW weather_data
# USING csv
# OPTIONS (
#   path "/Volumes/acxiom_szkolenie_sda/weather/sample_weather_data/weather_data_2022.csv",
#   header "true",
#   inferSchema "true"
# )

# COMMAND ----------

# MAGIC %md
# MAGIC Patrzymy na schemat danych

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schemat Danych

# COMMAND ----------

data.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dodajemy dane do Delty

# COMMAND ----------

data.write.format("delta").saveAsTable(table_name)

# COMMAND ----------

# %sql

# CREATE TABLE IF NOT EXISTS table_name AS
# SELECT * FROM weather_data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dodanie nowych danych

# COMMAND ----------

# a teraz dodajmy nowe dane
new_path_data = "/Volumes/acxiom_szkolenie_sda/weather/sample_weather_data/weather_data_2023.csv"

new_data = (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(new_path_data))

# COMMAND ----------

new_data.write.format("delta").mode("append").saveAsTable(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grupowanie po roku żeby zobaczyć czy wszystko jest ok

# COMMAND ----------

import pyspark.sql.functions as F
df = spark.read.table(table_name)
df_count =  (df.withColumn("Year", F.year(F.col("Timestamp")))
    .groupBy("Year")
    .count()
    .orderBy("Year"))
df_count.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT YEAR(Timestamp) AS Year, COUNT(*) AS count
# MAGIC FROM weather_pl
# MAGIC GROUP BY YEAR(Timestamp)
# MAGIC ORDER BY Year

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sprawdzenie Historii

# COMMAND ----------

hist = spark.sql(f"DESCRIBE HISTORY {table_name}")
display(hist)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY table_name

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wyciągnięcie wersji 0

# COMMAND ----------

# Wyciągnięcie wersji 1
previous_version_df = spark.read.option("versionAsOf", 0).table(table_name)

df_count =  (previous_version_df.withColumn("Year", F.year(F.col("Timestamp")))
    .groupBy("Year")
    .count()
    .orderBy("Year"))
df_count.show()


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     YEAR(Timestamp) AS Year, 
# MAGIC     COUNT(*) AS count
# MAGIC FROM 
# MAGIC     weather_pl VERSION AS OF 0
# MAGIC GROUP BY 
# MAGIC     YEAR(Timestamp)
# MAGIC ORDER BY 
# MAGIC     Year

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dodanie nowej kolumny

# COMMAND ----------

# A co jeżeli chcemy dodać nową kolumnę
fresh_path_data = "/Volumes/acxiom_szkolenie_sda/weather/sample_weather_data/weather_data_2024.csv"

fresh_path_data = (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(fresh_path_data))

fresh_path_data = fresh_path_data.withColumn("Source", F.lit("IMGW"))
display(fresh_path_data)

# COMMAND ----------

# MAGIC %md
# MAGIC Normalne pisanie nie zadziała, ze względu na mismatch schematu

# COMMAND ----------

fresh_path_data.write.mode("append").saveAsTable(table_name)

# COMMAND ----------

fresh_path_data.write.option("mergeSchema", "true").mode("append").saveAsTable(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC  A co z historia?

# COMMAND ----------

hist = spark.sql(f"DESCRIBE HISTORY {table_name}")
display(hist)

# COMMAND ----------

previous_version_df = spark.read.option("versionAsOf", 0).table(table_name)
previous_version_df.show(10)

# COMMAND ----------

previous_version_df = spark.read.option("versionAsOf", 2).table(table_name)
previous_version_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC Stare wersje nie mają w ogóle kolumny Source, obecna wersja ma ją - tam gdzie nie było jej wcześniej mamy NULL'e
