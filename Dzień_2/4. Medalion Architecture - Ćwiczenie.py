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

# COMMAND ----------

# MAGIC %md
# MAGIC Ćwiczenie Medalion architecture.
# MAGIC
# MAGIC 1. Wczytaj tabele `samples.accuweather.historical_daynight_metric`
# MAGIC 2. Zapisz te tabele jako `historical_bronze` wcześniej filtrując wszystkie nulle w kolumnie `city_name`
# MAGIC 3. Wczytaj tabele `samples.accuweather.forecast_daynight_metric`
# MAGIC 4. Stwórz nową tabelę `forecast_bronze` wcześniej filtrując wszystkie nulle w kolumnie `city_name`
# MAGIC 5. Stwórz tabele `historical_tokyo` która jest odpowiadającą tabelą bronze tylko dla city_name == 'tokyo'
# MAGIC 6. Stwórz tabele `forecast_tokyo` która jest odpowiadającą tabelą bronze tylko dla city_name == 'tokyo'
# MAGIC 7. Stwórz tabele `weather_tokyo` gdzie dostaniemy następujące kolumny:
# MAGIC - city_name
# MAGIC - date
# MAGIC - day_flag
# MAGIC - temperature_avg
# MAGIC - temperature_max
# MAGIC - temperature_min
# MAGIC
# MAGIC posortuj tę tabelę po dacie

# COMMAND ----------

# MAGIC %md
# MAGIC # Rozwiązanie

# COMMAND ----------

# zad 1 i 2

historical_daynight_metric_df = spark.read.table("samples.accuweather.historical_daynight_metric")


historical_bronze_df = historical_daynight_metric_df.filter("city_name IS NOT NULL")

historical_bronze_df.write.mode("overwrite").saveAsTable("historical_bronze")


# COMMAND ----------

# zad 3 i 4
forecast_daynight_metric_df = spark.read.table("samples.accuweather.forecast_daynight_metric")

# Filter out rows where city_name is null
forecast_bronze_df = forecast_daynight_metric_df.filter("city_name IS NOT NULL")

# Save the filtered dataframe as a new table
forecast_bronze_df.write.mode("overwrite").saveAsTable("forecast_bronze")

display(forecast_bronze_df)

# COMMAND ----------

# zad 5
historical_tokyo_df = historical_bronze_df.filter("city_name = 'tokyo'")
historical_tokyo_df.write.mode("overwrite").saveAsTable("historical_tokyo")

# zad 6
forecast_tokyo_df = forecast_bronze_df.filter("city_name = 'tokyo'")
forecast_tokyo_df.write.mode("overwrite").saveAsTable("forecast_tokyo")


# COMMAND ----------

# zad 7

weather_tokyo_df = spark.sql("""
    SELECT 
        *
    FROM (
        SELECT 
            city_name,
            date,
            day_flag,
            temperature_avg,
            temperature_max,
            temperature_min 
        FROM historical_tokyo
        UNION ALL
        SELECT 
            city_name,
            date,
            day_flag,
            temperature_avg,
            temperature_max,
            temperature_min 
        FROM forecast_tokyo
    )
    ORDER BY date
""")

weather_tokyo_df.write.mode("overwrite").saveAsTable("weather_tokyo")

display(weather_tokyo_df)

# COMMAND ----------


