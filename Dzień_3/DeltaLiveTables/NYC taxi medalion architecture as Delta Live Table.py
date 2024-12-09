# Databricks notebook source
# MAGIC %md
# MAGIC Te zadanie do rozwiązanie zadania `DatabricksSzkolenie/Dzien_2/3. Medalion Architecture` jako Delta Live Table

# COMMAND ----------

# MAGIC %md
# MAGIC Zadania:
# MAGIC
# MAGIC Napisz Delta Live Table pipeline, która wykona te same kroki co w zadaniu Dzień_2/Medalion Architecture, ale jako Delta Live table.

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

my_catalog = "training_catalog"
my_schema = "default"
my_volume = "nyc_taxi"


volume_path = f"/Volumes/{my_catalog}/{my_schema}/{my_volume}/"

# COMMAND ----------

# MAGIC %md
# MAGIC 1. tabela `dlt_taxi_raw_records` powinna czytać pliki csv ze ścieżki `/Volumes/training_catalog/default/nyc_taxi`. Dodaj expectation, że trip_distance powinien być > 0
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 2. tabela `dlt_flagged_rides` powinna czytać z tabeli `taxi_raw_records` według kodu:
# MAGIC ```
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
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC 3. tabela `dlt_weekly_stats` powinna czytać z tabeli `taxi_raw_records` wedlug kodu:
# MAGIC ```
# MAGIC CREATE OR REPLACE TABLE weekly_stats AS
# MAGIC SELECT
# MAGIC   date_trunc("week", tpep_pickup_datetime) AS week,
# MAGIC   AVG(fare_amount) AS avg_amount,
# MAGIC   AVG(trip_distance) AS avg_distance
# MAGIC FROM
# MAGIC   taxi_raw_records
# MAGIC GROUP BY week
# MAGIC ORDER BY week ASC;
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 4. tabela `dlt_top_n`,  powinna czytać z tabel `dlt_weekly_stats` oraz `dlt_flagged_rides` wedlug kodu:
# MAGIC ```
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
# MAGIC ```
