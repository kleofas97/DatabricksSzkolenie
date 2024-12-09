# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.get(scope="scope_kursu", key="moje_haslo")

# COMMAND ----------


