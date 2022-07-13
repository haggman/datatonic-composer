import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
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
        logging.info('response=', response)
        logging.info('Message', response.read())


    hello_python = PythonOperator(
        task_id='hello',
        python_callable=hit_cloud_run)

    goodbye_bash = BashOperator(
        task_id='bye',
        bash_command='echo Goodbye.')

    hello_python 

    # def greeting():
    #     logging.info('Hello World!')
    #
    # hello_python = PythonOperator(
    #     task_id='hello',
    #     python_callable=greeting)
    #
    # goodbye_bash = BashOperator(
    #     task_id='bye',
    #     bash_command='echo Goodbye.')
    #
    # hello_python >> goodbye_bash
