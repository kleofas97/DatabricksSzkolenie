# Databricks notebook source
# MAGIC %md
# MAGIC Preparing babynames data for training

# COMMAND ----------

my_catalog = "training_catalog"
my_schema = "default"
my_volume = "ny_babies"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {my_catalog}.{my_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {my_catalog}.{my_schema}.{my_volume}")

volume_path = f"/Volumes/{my_catalog}/{my_schema}/{my_volume}/"
download_url = "https://health.data.ny.gov/api/views/jxy9-yhdk/rows.csv"
filename = "babynames.csv"

dbutils.fs.cp(download_url, volume_path + filename)


# COMMAND ----------

import pandas as pd

file_path = volume_path + filename
df = pd.read_csv(file_path)
volume_path = f"/Volumes/{my_catalog}/{my_schema}/{my_volume}/"
# Calculate the number of rows in each split
total_rows = len(df)
split_size = total_rows // 4

# Split the dataframe into 4 parts
df1 = df.iloc[:split_size]
df2 = df.iloc[split_size:2*split_size]
df3 = df.iloc[2*split_size:3*split_size]
df4 = df.iloc[3*split_size:]

# Save the split dataframes to separate files
df1.to_csv(volume_path + "babynames_part1.csv", index=False)
df2.to_csv(volume_path + "babynames_part2.csv", index=False)
df3.to_csv(volume_path + "babynames_part3.csv", index=False)
# df4.to_csv(volume_path + "babynames_part4.csv", index=False)

# COMMAND ----------

df4.to_csv(volume_path + "babynames_part4.csv", index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Preparing NYC taxi table as csv in two parts

# COMMAND ----------

# Read the table into a Spark DataFrame
nyctaxi_df = spark.table("samples.nyctaxi.trips")

# Convert the Spark DataFrame to a Pandas DataFrame
nyctaxi_pd_df = nyctaxi_df.toPandas()

# Calculate the number of rows in each split
total_rows = len(nyctaxi_pd_df)
split_size = total_rows // 2

# Split the dataframe into 2 parts
nyctaxi_pd_df1 = nyctaxi_pd_df.iloc[:split_size]
nyctaxi_pd_df2 = nyctaxi_pd_df.iloc[split_size:]

# Define the volume path
volume_path = f"/Volumes/{my_catalog}/{my_schema}/{my_volume}/"

# Save the split dataframes to separate files
nyctaxi_pd_df1.to_csv(volume_path + "nyctaxi_part1.csv", index=False)
nyctaxi_pd_df2.to_csv(volume_path + "nyctaxi_part2.csv", index=False)

# COMMAND ----------


nyctaxi_pd_df2.to_csv(volume_path + "nyctaxi_part2.csv", index=False)

# COMMAND ----------


