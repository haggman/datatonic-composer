from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils import dates

default_dag_args = {
    'start_date': dates.days_ago(0),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        'composer_sample_simple_greeting',
        description='Hello World!',
        schedule_interval=None,
        default_args=default_dag_args) as dag:

    def greeting():
        import logging
        logging.info('Hello World!')

    hello_python = PythonOperator(
        task_id='hello',
        python_callable=greeting)

    goodbye_bash = BashOperator(
        task_id='bye',
        bash_command='echo Goodbye.')

    hello_python >> goodbye_bash
