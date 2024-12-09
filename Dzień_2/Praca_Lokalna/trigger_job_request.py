import requests
import os
import dotenv
import json

dotenv.load_dotenv()
job_id = 438904916540065
databricks_instance = "https://dbc-8491637a-9d4a.cloud.databricks.com"
token = os.environ["DATABRICKS_TOKEN"]

headers = {"Authorization": f"Bearer {token}"}

job_param_value = "MojaWartoscZLokalnegoKomputera"

payload = {
    "job_id": job_id,
    "parameters": {
        "job_param_1": job_param_value
    }
}

response = requests.post(
    f"{databricks_instance}/api/2.1/jobs/run-now",
    json=payload,
    headers=headers
)

print(response.text)
