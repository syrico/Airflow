from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.sensors.external_task_sensor import ExternalTaskMarker, ExternalTaskSensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator

import pprint
import json

path_config = '/home/syrico/airflow/dags/config_dag/'

from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": 2,
    "start_date": days_ago(1)
}

var_1 = ['ani','budi']
var_2 = {'nama':'ani'}





def push_xcompush(**context):
    context['ti'].xcom_push(key='task_push_xcompush', value = var_2)
    
pull = "{{ ti.xcom_pull(task_ids='push', key='task_push_xcompush') }}"
    
# def pull(**context):
#     ti = context["ti"]
#     var =  ti.xcom_pull(key = 'task_push_xcompush', task_ids = 'xcom_push', dag_id = 'DAG_3', include_prior_dates= True )
#     print(var)
    

with DAG( dag_id = 'DAG_5', 
         default_args = default_args, 
         schedule_interval='*/15 * * * *', 
        catchup=False) as dag_1:
    
    # task_1 = DummyOperator(task_id = 'start')
    # task_2 = DummyOperator(task_id = 'stop')
    
    task_wo_xcompush = PythonOperator(task_id='push',
                                  python_callable = push_xcompush,
                                  provide_context = True )
    
    bash_operator = BashOperator(task_id='bash', bash_command = "echo {{ ti.xcom_pull(task_ids='push', key='task_push_xcompush') }}")    
    # task_xcompush = PythonOperator(task_id='xcom_push',
    #                               python_callable = push_xcompush,
    #                               provide_context = True )

    # task_xcompull = PythonOperator(task_id='xcom_pull',
    #                               python_callable = pull, 
    #                               provide_context = True )

task_wo_xcompush >> bash_operator
    # task_1 >> task_wo_xcompush >> task_xcompush >> task_xcompull >> task_2

