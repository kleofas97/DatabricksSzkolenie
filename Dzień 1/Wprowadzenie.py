# Databricks notebook source
# MAGIC %md
# MAGIC Notebooki mają kilka rodzajów komórek, zależnie od tego jakiego języka chcemy używać. To komórka typu Markdown - czyli do pisania tekstu
# MAGIC
# MAGIC Możemy tu formatować tekst na różne sposoby:
# MAGIC
# MAGIC - Wyliczenie
# MAGIC - Kolejne wyliczenie
# MAGIC
# MAGIC # Paragraf
# MAGIC
# MAGIC ## Mniejsze paragraf
# MAGIC
# MAGIC
# MAGIC **Pogrubione**
# MAGIC _Kursywa_
# MAGIC
# MAGIC `zaznaczenie`
# MAGIC

# COMMAND ----------

# Ten notebook domyślnie jest Pythonowy, ale możemy zmienić to za pomocą znaku % na początku komórki

# COMMAND ----------

print("Witaj w Databricks!")

# COMMAND ----------

# MAGIC %md
# MAGIC W SQL'u...

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Witaj w SQL w Databricks!"

# COMMAND ----------

# MAGIC %md
# MAGIC `dbutils` to biblioteka dostępna w Databricks, która ułatwia zarządzanie zasobami w przestrzeni roboczej. Obejmuje operacje związane z plikami, zarządzaniem konfiguracją, przekazywaniem parametrów do notatników i wiele innych. W tym wstępie omówimy najważniejsze funkcje dbutils.

# COMMAND ----------

dbutils.help()

# COMMAND ----------

username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
name = username.split('@')[0].replace('.', '_')
print(name)

# COMMAND ----------

dbutils.fs.ls("/Volumes/acxiom_szkolenie_sda/weather/sample_weather_data/")

# COMMAND ----------

# Tworzenie nowego folderu
dbutils.fs.mkdirs(f"/Volumes/acxiom_szkolenie_sda/weather/fs_training/{name}")

# COMMAND ----------

# spróbujmy to samo w innym miejscu
dbutils.fs.mkdirs(f"/Volumes/acxiom_szkolenie_sda/weather/sample_weather_data/fs_training/{name}")

# COMMAND ----------

# kopiowanie pliku
dbutils.fs.cp("/Volumes/acxiom_szkolenie_sda/weather/sample_weather_data/weather_data_2022.csv", f"/Volumes/acxiom_szkolenie_sda/weather/fs_training/{name}/weather_data_2022.csv")

# COMMAND ----------

#usuwanie pliku
dbutils.fs.rm(f"/Volumes/acxiom_szkolenie_sda/weather/fs_training/{name}/weather_data_2022.csv", True)  

# COMMAND ----------

# MAGIC %md
# MAGIC Widgety

# COMMAND ----------

dbutils.widgets.text("param1", "default_value", "Opis parametru")
dbutils.widgets.dropdown("param2", "opcja1", ["opcja1", "opcja2", "opcja3"], "Wybierz opcję")

# COMMAND ----------

# Pobieranie wartości widżetu
value = dbutils.widgets.get("param1")
print(f"Wartość param1: {value}")

# COMMAND ----------

# Usuwanie widżetu
dbutils.widgets.remove("param1")

# COMMAND ----------

# Usunięcie wszystkich widżetów
dbutils.widgets.removeAll()

# COMMAND ----------


