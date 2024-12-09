# Databricks notebook source
import dlt 
import pyspark.sql.functions as F


my_catalog = "training_catalog"
my_schema = "default"
my_volume = "projekt/sales"

volume_path = f"/Volumes/{my_catalog}/{my_schema}/{my_volume}/"

# COMMAND ----------

@dlt.table(comment="Purchases raw data")
def raw_purchases():
  df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("inferSchema", True)
    .option("header", True)
    .load(volume_path)
  )
  return df

# COMMAND ----------

@dlt.table(comment="User sales data with aggregated number of products and total money spent")
def user_sales():
    df_sales = (spark.read.table("LIVE.raw_purchases")
          .groupBy("User_ID")
          .agg(
              F.count("Product_ID").alias("Nb_of_products"),
              F.sum("Price").alias("total_spent")
          )
    )
    df_user = (spark.read.table("training_catalog.adam_k_mika_training.users").select("ID", "First_Name", "Last_Name", "Country"))
    df = df_user.join(df_sales, df_user.ID == df_sales.User_ID, how="left")
    return df.select("ID", "First_Name", "Last_Name", "Country", "Nb_of_products", "total_spent")

# COMMAND ----------

@dlt.table(comment="Country sales data with number of products sold and total income")
def country_sales():
    # czytanie user table
    df_user = (spark.read.table("training_catalog.adam_k_mika_training.users").select("ID", "Country"))
    # czytanier purchases table
    df_sales = spark.read.table("LIVE.raw_purchases").select("User_ID", "Product_ID", "Price").groupBy("User_ID","Product_ID").agg(F.count("Product_ID").alias("Nb_sold"), F.sum("Price").alias("Total_income"))
    # join sprzedazy z uzytkownikami - zeby miec Country
    df_sales = df_sales.join(df_user, df_sales.User_ID == df_user.ID, how="left")
    # czytanie produktów
    df_products = (spark.read.table("training_catalog.adam_k_mika_training.products").select("ID", "Product_Name"))
    # join z produktami, żeby mieć product name
    df_sales = df_sales.join(df_products, df_sales.Product_ID == df_products.ID, how="left")
    # grupowanie po Product Name i country, sumowanie sprzedaży i liczby sprzedanych egzemplarzy
    df_sales = df_sales.groupBy("Country", "Product_Name").agg(F.sum("Nb_sold").alias("Nb_sold"), F.sum("Total_income").alias("Total_income"))
    return df_sales.select("Country", "Product_Name", "Nb_sold", "Total_income")

# COMMAND ----------


