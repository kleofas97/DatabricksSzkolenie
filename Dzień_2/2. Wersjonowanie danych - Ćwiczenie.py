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
# MAGIC Cwiczenie - podobnie jak w przykładzie - stwórz tabele do przechowywania danych o pogodzie w USA. Zadania:
# MAGIC 1. Napisz kod, który wczyta dane dla roku 2022 a następnie wpisze je do tabeli `weather_us`.
# MAGIC 2. Następnie napisz kod, który wczyta kolejny plik, dla roku 2023 i dopisze go do istniejącej tabeli.
# MAGIC 3. Zastanów się jak powinniśmy podejść do tematu kiedy chcielibyśmy nadpisać jakiś rok?
# MAGIC 4. (z gwiazdką(!)) Zastanów się jak powinniśmy podejść do tematu, kiedy możemy mieć sytuacje kiedy wiele procesów na raz chce nadpisywać tabele. Np. Jeden proces chce nadpisać tabele dla danego roku,a inny dla innego - jak napisać kod żeby tu umożliwiał?
# MAGIC 5.  Napisz kod, który wczyta kolejny plik, dla roku 2024, i dodaj do niego nową kolumnę. Zapisz kod w taki sposób, aby zmieniał schemat bazy jeśli to konieczne.
# MAGIC 6. (z gwiazdką(!)) Napisz kod, który, potwierdzi że wyciągana historycznie tabela ma taki sam schemat jak teraźniejsza wersja tabeli, a jeśli nie to uzupełni schemat o brakujące kolumny

# COMMAND ----------

table_name = "weather_us"

# COMMAND ----------

path_data = "/Volumes/acxiom_szkolenie_sda/weather/sample_weather_data/weather_data_2022_us.csv"

data = (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(path_data))

# COMMAND ----------

from pyspark.sql.functions import year, col

data = data.withColumn("YEAR", year("timestamp").cast("string"))
display(data)
# dodaje kolumne year osobno -na potrzeby partycjonowania

# COMMAND ----------

data.printSchema()

# COMMAND ----------

data.write.format("delta").saveAsTable(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC Nowe dane za rok 2023

# COMMAND ----------

new_path_data = "/Volumes/acxiom_szkolenie_sda/weather/sample_weather_data/weather_data_2023_us.csv"

new_data = (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(new_path_data))
new_data = new_data.withColumn("YEAR", year("timestamp").cast("string"))

# COMMAND ----------

new_data.write.format("delta").mode("append").saveAsTable(table_name)

# COMMAND ----------

hist = spark.sql(f"DESCRIBE HISTORY {table_name}")
display(hist)


# COMMAND ----------

spark.sql(f"SELECT DISTINCT year FROM {table_name}").show()

# COMMAND ----------

# nadpisanie roku 2023 jeszcze raz
(new_data.write
 .format("delta")
 .mode("overwrite")
 .option("replaceWhere", "YEAR = 2023")
 .saveAsTable(table_name))

# COMMAND ----------

hist = spark.sql(f"DESCRIBE HISTORY {table_name}")
display(hist)

# COMMAND ----------

spark.sql(f"SELECT DISTINCT year FROM {table_name}").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Dodanie do nowej tabeli, gdzie dane już są partycjonowane 

# COMMAND ----------

df = spark.sql(f"SELECT * FROM {table_name}")

(
df.write.format("delta")
.mode("overwrite")
.option("overwriteSchema", "true")
.partitionBy("year")
.saveAsTable(table_name + "_partitioned")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Dodatkowa kolumna

# COMMAND ----------

import pyspark.sql.functions as F
fresh_path = "/Volumes/acxiom_szkolenie_sda/weather/sample_weather_data/weather_data_2024_us.csv"

fresh_path_data = (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(fresh_path))
fresh_path_data = fresh_path_data.withColumn("YEAR", year("timestamp").cast("string"))
fresh_path_data = fresh_path_data.withColumn("Source", F.lit("IMGW"))

# COMMAND ----------

(fresh_path_data.write
 .format("delta")
 .mode("append")
 .option("mergeSchema", "true")
 .partitionBy("YEAR")
 .saveAsTable(table_name + "_partitioned"))

# COMMAND ----------

(fresh_path_data.write
 .format("delta")
 .mode("append")
 .option("mergeSchema", "true")
 .saveAsTable(table_name))

# COMMAND ----------

hist = spark.sql(f"DESCRIBE HISTORY {table_name}")
display(hist)

# COMMAND ----------

version_2 = spark.sql(f"SELECT * FROM {table_name} VERSION AS OF 2")
display(version_2)

# COMMAND ----------

version_2.show()

# COMMAND ----------

spark.read.table(table_name).schema

# COMMAND ----------


current_schema = spark.read.table(table_name).schema
version_2_schema = version_2.schema

# znajdz brakujace schemy
missing_columns = [field for field in current_schema.fields if field.name not in version_2.columns]

# dodanie nulla do tych schem - uwaga withColumn nie jest najszybszą wersją https://kb.databricks.com/en_US/scala/withcolumn-operation-when-using-in-loop-slows-performance
for field in missing_columns:
    version_2 = version_2.withColumn(field.name, F.lit(None).cast(field.dataType))

# Poprawna kolejność
version_2 = version_2.select([field.name for field in current_schema.fields])

display(version_2)

# COMMAND ----------


