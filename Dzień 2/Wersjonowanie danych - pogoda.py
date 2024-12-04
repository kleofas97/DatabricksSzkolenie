# Databricks notebook source
# MAGIC %md
# MAGIC Stwórzmy tabele `acxiom_szkolenie_sda.weather.pl`. Teraz dodamy dane, które mam w Volumes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tworzymy tabele

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

# MAGIC %sql
# MAGIC DESCRIBE TABLE acxiom_szkolenie_sda.weather.pl

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dodajemy dane do Delty

# COMMAND ----------

data.write.format("delta").saveAsTable("acxiom_szkolenie_sda.weather.pl")

# COMMAND ----------

# %sql

# CREATE TABLE IF NOT EXISTS acxiom_szkolenie_sda.weather.pl AS
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

new_data.write.format("delta").mode("append").saveAsTable("acxiom_szkolenie_sda.weather.pl")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grupowanie po roku żeby zobaczyć czy wszystko jest ok

# COMMAND ----------

import pyspark.sql.functions as F
df = spark.read.table("acxiom_szkolenie_sda.weather.pl")
df_count =  (df.withColumn("Year", F.year(F.col("Timestamp")))
    .groupBy("Year")
    .count()
    .orderBy("Year"))
df_count.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT YEAR(Timestamp) AS Year, COUNT(*) AS count
# MAGIC FROM acxiom_szkolenie_sda.weather.pl
# MAGIC GROUP BY YEAR(Timestamp)
# MAGIC ORDER BY Year

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sprawdzenie Historii

# COMMAND ----------

hist = spark.sql("DESCRIBE HISTORY acxiom_szkolenie_sda.weather.pl")
display(hist)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY acxiom_szkolenie_sda.weather.pl

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wyciągnięcie wersji 0

# COMMAND ----------

# Wyciągnięcie wersji 1
previous_version_df = spark.read.option("versionAsOf", 0).table("acxiom_szkolenie_sda.weather.pl")

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
# MAGIC     acxiom_szkolenie_sda.weather.pl VERSION AS OF 0
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

fresh_path_data.write.mode("append").saveAsTable("acxiom_szkolenie_sda.weather.pl")

# COMMAND ----------

fresh_path_data.write.option("mergeSchema", "true").mode("append").saveAsTable("acxiom_szkolenie_sda.weather.pl")

# COMMAND ----------

# MAGIC %md
# MAGIC  A co z historia?

# COMMAND ----------

hist = spark.sql("DESCRIBE HISTORY acxiom_szkolenie_sda.weather.pl")
display(hist)

# COMMAND ----------

previous_version_df = spark.read.option("versionAsOf", 0).table("acxiom_szkolenie_sda.weather.pl")
previous_version_df.show(10)

# COMMAND ----------

previous_version_df = spark.read.option("versionAsOf", 2).table("acxiom_szkolenie_sda.weather.pl")
previous_version_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC Stare wersje nie mają w ogóle kolumny Source, obecna wersja ma ją - tam gdzie nie było jej wcześniej mamy NULL'e
