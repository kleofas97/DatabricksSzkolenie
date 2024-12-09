# Databricks notebook source
# MAGIC %md
# MAGIC Te zadanie do rozwiązanie zadania `DatabricksSzkolenie/Dzien_2/3. Medalion Architecture` jako Delta Live Table

# COMMAND ----------

# MAGIC %md
# MAGIC Zadania:
# MAGIC
# MAGIC Napisz Delta Live Table pipeline, która wykona te same kroki co w zadaniu Dzień_2/Medalion Architecture, ale jako Delta Live table.

# COMMAND ----------

#ustawiamy source z którego będziemy ładować pliki

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


@dlt.table(comment="Taxi Raw Records")
@dlt.expect("trip_distance_higher_than_0","trip_distance > 0")
def dlt_taxi_raw_records():
    df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("inferSchema", True)
    .option("header", True)
    .load(volume_path)
  )
    return df

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

@dlt.table(comment="Flagged Rides table")
def dlt_flagged_rides():
    df = spark.read.table("LIVE.dlt_taxi_raw_records")
    df = df.withColumn("week", F.date_trunc("week", F.col("tpep_pickup_datetime")))
    df = df.filter(
        ((F.col("pickup_zip") == F.col("dropoff_zip")) & (F.col("fare_amount") > 50)) |
        ((F.col("trip_distance") < 5) & (F.col("fare_amount") > 50))
    )
    return df.select("week", "pickup_zip", "fare_amount", "trip_distance")

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

@dlt.table(comment="Weekly Statsdlt table")
def dlt_weekly_stats():
    df = spark.read.table("LIVE.dlt_taxi_raw_records")
    df = df.withColumn("week", F.date_trunc("week", F.col("tpep_pickup_datetime")))
    df = df.groupBy("week").agg(
        F.avg("fare_amount").alias("avg_amount"),
        F.avg("trip_distance").alias("avg_distance")
    )
    return df.orderBy("week", ascending=True)

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

# COMMAND ----------

@dlt.table(comment="Top N Rides table")
def dlt_top_n():
    flagged_rides = spark.read.table("LIVE.dlt_flagged_rides")
    weekly_stats = spark.read.table("LIVE.dlt_weekly_stats")
    
    joined_df = flagged_rides.join(weekly_stats, "week", "left") \
        .select(
            F.col("week"),
            F.round(F.col("avg_amount"), 2).alias("avg_amount"),
            F.round(F.col("avg_distance"), 3).alias("avg_distance"),
            F.col("fare_amount"),
            F.col("trip_distance"),
            F.col("pickup_zip").alias("zip")
        ) \
        .orderBy(F.col("fare_amount").desc()) \
        .limit(10)
    
    return joined_df
