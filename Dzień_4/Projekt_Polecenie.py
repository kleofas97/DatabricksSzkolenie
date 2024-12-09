# Databricks notebook source
# MAGIC %md
# MAGIC Zadaniem projektu jest przygotowanie pipeline'u, który wczytuje i procesuje dane sprzedażowe, a następnie tworzy wizualizację danych sprzedażowych, z możliwością filtrowania. Pipeline powinien byś automatycznie uruchamiany, kiedy pojawi się nowy plik w landing zone.
# MAGIC
# MAGIC Zadanie podzielone jest na kilka etapów.
# MAGIC
# MAGIC 1. Zrozumienie danych.
# MAGIC w Volumes `/Volumes/training_catalog/default/projekt` znajdują się trzy pliki:
# MAGIC - users.csv - informację o klientach
# MAGIC - products.csv - informacje o produktach
# MAGIC - sales/purchases.csv - sprzedaż produktów.
# MAGIC Przejrzyj je w dowolny sposób, żeby zrozumieć jakie dane się w nich zajmują. Skorzystaj z np. z notebooków.
# MAGIC
# MAGIC 2. Utwórz tabele w swoim katalogu dla danych z plików `users.csv` oraz `products.csv`. Następnie przygotuj DLT pipeline, który będzie czytał pliki z folderu sales/ do STREAMING table RAW_PURCHASES.
# MAGIC 3. W tym samym DLT przygotuj zmaterializowane widoki, gdzie odpowiednio przygotują tabele:
# MAGIC `user_sales` - z kolumnami ID, First_Name, Last_Name, Country, Nb_of_products, total_spent, gdzie nb_of_products oznacza ile produktów kupił dany użytkownik, a total_spent oznacza ile pieniedzy dany klient wydał na produkty.
# MAGIC `country_sales` - z kolumnami Country, Product_Name, Nb_sold, Total_income, gdzie Nb_sold to liczba sprzedanych produktów, a Total_income to suma sprzedaży w danym kraju.
# MAGIC
# MAGIC 4. Przygotuj Dashboard z:
# MAGIC - Totalną sprzedażą per kraj
# MAGIC - Liczbą sprzedanych produktów per kraj - dodaj możliwośc filtrowania po produkcie i kraju który poddany jest analizie
# MAGIC - Tabele user sales, gdzie możemy filtrować po imieniu i nazwisku z aby znaleźć informację ile produktów i za ile kupił dany użytkownik
# MAGIC - Wykres sprzedaży w czasie
# MAGIC
# MAGIC 5. Ustaw job, który automatycznie uruchomi się, kiedy nowy plik sprzedaży pojawi się w lokalizacji `/Volumes/training_catalog/default/projekt/sales`. Job powinien uruchomić DLT, który dodane nowe dane sprzedażowe i przeliczy zmaterializowane Views.
# MAGIC Odśwież Dashboard - największa sprzedaż powinna być dla Meksyku - 5,28k produktów o wartości 1,323,497
# MAGIC
# MAGIC

# COMMAND ----------

username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
name = username.split('@')[0].replace('.', '_')
target_catalog= "training_catalog"
target_schema= f"{name}_training"
print(f"Using: '{target_catalog}.{target_schema}' catalog and schema")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")
spark.sql(f"USE CATALOG {target_catalog}")
spark.sql(f"USE SCHEMA {target_schema}")
