import logging
import json
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

    def get_transfer_configs():
        transfer_client = bigquery_datatransfer.DataTransferServiceClient()

        project_id = "dt-patrick-project-dev"
        parent = transfer_client.common_project_path(project_id)

        configs = transfer_client.list_transfer_configs(parent=parent)
        print(f"Got the following configs: {configs}")
        for config in configs:
            print(f"\tID: {config.name}, Schedule: {config.schedule}, config: {config}")


    cloud_run_load_files_to_gcs = PythonOperator(
        task_id='cloud_run_load_files_to_gcs',
        python_callable=hit_cloud_run)

    check_configs = PythonOperator(
        task_id='check_configs',
        python_callable=get_transfer_configs)

    # run_bq_projects_data_transfer = BigQueryDataTransferServiceStartTransferRunsOperator(
    #     task_id="gcp_bigquery_start_transfer",
    #     transfer_config_id=transfer_config_id,
    #     requested_run_time={"seconds": int(time.time() + 60)},
    # )

    cloud_run_load_files_to_gcs >> check_configs

