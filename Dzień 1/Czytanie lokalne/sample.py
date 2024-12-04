import os
from databricks import sql
import dotenv

dotenv.load_dotenv()

connection = sql.connect(
    server_hostname="dbc-8491637a-9d4a.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/60bc2e0f71acbb1b",
    access_token=os.environ["DATABRICKS_TOKEN"])

cursor = connection.cursor()
cursor.execute("SELECT * from samples.nyctaxi.trips limit 10")
print(cursor.fetchall())

cursor.close()
connection.close()
