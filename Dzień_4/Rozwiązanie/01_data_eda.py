# Databricks notebook source
dbutils.fs.ls("/Volumes/training_catalog/default/projekt")

# COMMAND ----------

import pandas as pd

users = pd.read_csv("/Volumes/training_catalog/default/projekt/users.csv")
users.info()

# COMMAND ----------

users.head()

# COMMAND ----------

products = pd.read_csv("/Volumes/training_catalog/default/projekt/products.csv")
products.info()

# COMMAND ----------

products.head()

# COMMAND ----------

purchases = pd.read_csv("/Volumes/training_catalog/default/projekt/sales/purchases.csv")
purchases.info()

# COMMAND ----------

purchases.head()

# COMMAND ----------


