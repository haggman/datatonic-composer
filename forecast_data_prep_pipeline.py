import logging
import json
import time
import os
from datetime import timedelta
from google.cloud import bigquery_datatransfer
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery_dts import BigQueryDataTransferServiceStartTransferRunsOperator
from airflow.utils import dates

import urllib
import google.auth.transport.requests
import google.oauth2.id_token

default_dag_args = {
    'start_date': dates.days_ago(0),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        'forecast_data_ingest_pipeline',
        description='Run Forecast data ingestion pipeline',
        schedule_interval=timedelta(hours=1),
        default_args=default_dag_args) as dag:
    def hit_cloud_run():
        service_url = 'https://forecast-data-loader-ldneeqwt7a-nw.a.run.app'
        req = urllib.request.Request(service_url)

        auth_req = google.auth.transport.requests.Request()
        id_token = google.oauth2.id_token.fetch_id_token(auth_req, service_url)
        req.add_header("Authorization", f"Bearer {id_token}")
        response = urllib.request.urlopen(req)
        resp_json = json.load(response)
        print(resp_json)


    cloud_run_load_files_to_gcs = PythonOperator(
        task_id='cloud_run_load_files_to_gcs',
        python_callable=hit_cloud_run)

    bq_projects_data_transfer = BigQueryDataTransferServiceStartTransferRunsOperator(
        task_id="bq_projects_data_transfer",
        transfer_config_id='62d94b42-0000-2a0f-b412-883d24f25d1c',
        requested_run_time={"seconds": int(time.time() + 10)},
        location="europe-west2"
    )

    bq_tasks_data_transfer = BigQueryDataTransferServiceStartTransferRunsOperator(
        task_id="bq_tasks_data_transfer",
        transfer_config_id='62d9852e-0000-2c35-8acd-3c286d418342',
        requested_run_time={"seconds": int(time.time() + 10)},
        location="europe-west2"
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /home/airflow/gcs/dags && dbt'
    )

    cloud_run_load_files_to_gcs >> [bq_tasks_data_transfer, bq_projects_data_transfer] >> dbt_run
