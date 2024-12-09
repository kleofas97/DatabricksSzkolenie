# Databricks notebook source
# wersja w SQL'u: https://docs.databricks.com/en/_extras/notebooks/source/dlt-babynames-sql.html

# COMMAND ----------

import dlt # <- dlt = delta live tables
import pyspark.sql.functions as F

#data source
my_catalog = "training_catalog"
my_schema = "default"
my_volume = "ny_babies"


volume_path = f"/Volumes/{my_catalog}/{my_schema}/{my_volume}/"


# syntax jest następujący:
# jako dekorator podajemy informacje o tabeli (samo określenie, że jest to tabela plus ewentualnie komentarze, tagi https://docs.databricks.com/en/delta-live-tables/python-ref.html) można też dodawać zabezpieczenia odnośnie expectation of data
@dlt.table(
  comment="Popular baby first names in New York. This data was ingested from the New York State Department of Health."
)
# nazwa funkcji to tez nazwa przyszłej tabeli
def baby_names_raw():
  df = (spark.readStream # read stream oznacza ze tabela do której piszemy do STREAMING TABLE, czyli tabela do której tylko appendujemy
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("inferSchema", True)
    .option("header", True)
    .load(volume_path)
  )
  df_renamed_column = df.withColumnRenamed("First Name", "First_Name")
  return df_renamed_column # właściwa tabela


# od razu tworzymy kolejną tabelę, która będzie czytać z świeżo co utworzonej tabeli `baby_names_raw`, kiedy piszemy LIVE oznacza tabelę z aktualnego kontekstu
@dlt.table(
  comment="New York popular baby first name data cleaned and prepared for analysis."
)
@dlt.expect("valid_first_name", "First_Name IS NOT NULL") # walidacja,jeżeli nie przejdzie tej walidacji, zostaje "po cichu" usunieta, błąd się nie pojawi i tabela się przeprocesuje
@dlt.expect_or_fail("valid_count", "Count > 0") # jeśli nie przejdzie - cały pipeline się wywali
def baby_names_prepared():
  return (
    spark.read.table("LIVE.baby_names_raw") # samo read i dlt.table oznacza Materalized View, czyli statyczne dan wygenerowane na podstawie querki z poprzedniej tabeli
      .withColumnRenamed("Year", "Year_Of_Birth")
      .select("Year_Of_Birth", "First_Name", "Count")
  )

#kolejny poziom agregacji - tutaj działamy już na kolejnej tabeli.
@dlt.table(
  comment="A table summarizing counts of the top baby names for New York for 2021."
)
def top_baby_names_2021():
  return (
    spark.read.table("LIVE.baby_names_prepared")
      .filter(F.expr("Year_Of_Birth == 2021"))
      .groupBy("First_Name")
      .agg(F.sum("Count").alias("Total_Count"))
      .sort(F.desc("Total_Count"))
      .limit(10)
  )

