# Databricks notebook source
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
name = username.split('@')[0].replace('.', '_')
table_name = f"acxiom_szkolenie_sda.weather.us_{name}"
print(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC Cwiczenie - podobnie jak w przykładzie - stwórz tabele do przechowywania danych o pogodzie w USA. Zadania:
# MAGIC 1. Napisz kod, który wczyta dane dla roku 2022 a następnie wpisze je do tabeli o nazwie podanej w powyższej komórce.
# MAGIC 2. Następnie napisz kod, który wczyta kolejny plik,dla roku 2023 i dopisze go do istniejącej tabeli.
# MAGIC 3. Zastanów się jak powinniśmy podejść do tematu kiedy chcielibyśmy nadpisać jakiś rok?
# MAGIC 4. (gwiazdką(!)) Zastanów się jak powinniśmy podejść do tematu, kiedy możemy mieć sytuacje kiedy wiele procesów na raz chce nadpisywać tabele. Np. Jeden proces chce nadpisać tabele dla danego roku,a inny dla innego - jak napisać kod żeby tu umożliwiał?
# MAGIC 5.  Napisz kod, który wczyta kolejny plik, dla roku 2024, i dodaj do niego nową kolumnę. Zapisz kod w taki sposób, aby zmieniał schemat bazy jeśli to konieczne.
# MAGIC 6. Napisz kod, który, potwierdzi że wyciągana historycznie tabela ma taki sam schemat jak teraźniejsza wersja tabeli, a jeśli nie to uzupełni schemat o brakujące kolumny

# COMMAND ----------


