import datetime

from airflow import models

from airflow.operators import bash_operator
from airflow.operators import python_operator

default_dag_args = {

}

with models.DAG(
        'composer_sample_simple_greeting',
        default_args=default_dag_args) as dag:

    def greeting():
        import logging
        logging.info('Hello World!')

    hello_python = python_operator.PythonOperator(
        task_id='hello',
        python_callable=greeting)

    goodbye_bash = bash_operator.BashOperator(
        task_id='bye',
        bash_command='echo Goodbye.')

    hello_python >> goodbye_bash
