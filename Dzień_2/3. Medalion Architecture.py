# Databricks notebook source
# MAGIC %md
# MAGIC Dlaczego używamy Medalion Architecture: https://www.databricks.com/glossary/medallion-architecture

# COMMAND ----------

# MAGIC %md
# MAGIC Ustawienia, aby każdy miał swój schemat w katalogu

# COMMAND ----------

username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
name = username.split('@')[0].replace('.', '_')
target_catalog= "training_catalog"
target_schema= f"{name}_training"
print(f"Using: '{target_catalog}.{target_schema}' catalog and schema")

# COMMAND ----------

# musimy stworzyć najpierw schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")

# COMMAND ----------

# ustawiam, aby spark używał tego katalogu i tego schematu.
spark.sql(f"USE CATALOG {target_catalog}")
spark.sql(f"USE SCHEMA {target_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC Bronze Table
# MAGIC
# MAGIC Wszystkie podróże, których dystans jest > 0

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE taxi_raw_records AS -- piszę tylko tabelę, bo katalog i schemat został ustawiony wcześniej
# MAGIC SELECT *
# MAGIC FROM samples.nyctaxi.trips
# MAGIC WHERE trip_distance > 0.0;

# COMMAND ----------

# MAGIC %md
# MAGIC Silver Table 1
# MAGIC
# MAGIC Podejrzane podróże,  według warunków (OR)
# MAGIC - Kod Pocztowy jest taki sam a zapłacono ponad 50$
# MAGIC - Trip distance jest mniejszy niż 5 mil a zapłacono ponad 50$

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE flagged_rides AS
# MAGIC SELECT
# MAGIC   date_trunc("week", tpep_pickup_datetime) AS week,
# MAGIC   pickup_zip AS zip,
# MAGIC   fare_amount,
# MAGIC   trip_distance
# MAGIC FROM
# MAGIC   taxi_raw_records
# MAGIC WHERE ((pickup_zip = dropoff_zip AND fare_amount > 50) OR
# MAGIC        (trip_distance < 5 AND fare_amount > 50));

# COMMAND ----------

# MAGIC %md
# MAGIC Silver Table 2
# MAGIC
# MAGIC - Tygodniowe zgrupowanie danych ze średnią ceną i dystansem

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE weekly_stats AS
# MAGIC SELECT
# MAGIC   date_trunc("week", tpep_pickup_datetime) AS week,
# MAGIC   AVG(fare_amount) AS avg_amount,
# MAGIC   AVG(trip_distance) AS avg_distance
# MAGIC FROM
# MAGIC   taxi_raw_records
# MAGIC GROUP BY week
# MAGIC ORDER BY week ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC Gold Layer
# MAGIC
# MAGIC Podsumowanie na podstawie dwóch tabel:
# MAGIC Podanie średniej ceny i dla konkretnego tygodnia oraz wartości odstające (w tym przypadku największa cena zapłacona)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 
# MAGIC
# MAGIC CREATE OR REPLACE TABLE top_n AS
# MAGIC SELECT
# MAGIC   ws.week,
# MAGIC   ROUND(ws.avg_amount, 2) AS avg_amount,
# MAGIC   ROUND(ws.avg_distance, 3) AS avg_distance,
# MAGIC   fr.fare_amount,
# MAGIC   fr.trip_distance,
# MAGIC   fr.zip
# MAGIC FROM
# MAGIC   flagged_rides fr
# MAGIC LEFT JOIN weekly_stats ws ON ws.week = fr.week
# MAGIC ORDER BY fr.fare_amount DESC
# MAGIC LIMIT 10;
