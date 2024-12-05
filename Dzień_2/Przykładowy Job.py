# Databricks notebook source
try:
    job_param = dbutils.widgets.get("job_param_1")
    print(f"I got parameter: {job_param}")
except:
    print("NO PARAMS PROVIDED")
    raise Exception("No parameter provided!")

# COMMAND ----------


