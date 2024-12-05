import time

from databricks_cli.jobs.api import JobsApi
from databricks_cli.runs.api import RunsApi
from databricks_cli.sdk.api_client import ApiClient
import os
import dotenv

dotenv.load_dotenv()

api_client = ApiClient(
    host="https://dbc-8491637a-9d4a.cloud.databricks.com/",
    token=os.environ["DATABRICKS_TOKEN"]
)

jobs_api = JobsApi(api_client)

job_id = "931520163112932"
job_param_value = "MojaWartoscZLokalnegoKomputera"
run_id = jobs_api.run_now(job_id=job_id,
                          notebook_params={"job_param_1": job_param_value},
                          jar_params=None,
                          python_params=None,
                          spark_submit_params=None
                          )
run_api = RunsApi(api_client=api_client)

print(f"Triggered job with run ID: {run_id['run_id']}")
while True:
    status = run_api.get_run(run_id=run_id['run_id'])
    print(f"Running with Status {status['state']['life_cycle_state']}")
    time.sleep(1)
    if status['state']['life_cycle_state'] == 'TERMINATED':
        print(f"Job Finished with status: {status['state']['result_state']}")
        print(f"Full info: {status}")
        break
