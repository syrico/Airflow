from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
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

def kalimat():
    return 'kamlimat'
    print('ini kalimat')
    
def show_context(**context):
    print(context)
    
    with open(path_config+'file_config.txt','w') as f:
        f.write(context['prev_execution_date'].to_datetime_string())
    return context['dag_run']

def show_conf(**context):
    print(context)
    return context

with DAG( dag_id = 'DAG_1', 
         default_args = default_args, 
         schedule_interval='*/15 * * * *', 
        catchup=False) as dag_1:
    
    
    task_show_context = PythonOperator(task_id='show_context',
                                  python_callable = show_context,
                                  provide_context = True, )

    task_3 = DummyOperator(task_id = 'start_trigger')
    task_trigger = TriggerDagRunOperator(task_id='trigger_task',
                                         trigger_dag_id='DAG_2'
                                         ) 
    
    task_show_context >> task_3 >> task_trigger
    

with DAG(dag_id= 'DAG_2',
         default_args = default_args,
         schedule_interval=None, 
         catchup=False) as dag_2 :
    
    task_d2_1 = DummyOperator(task_id='start_d2')
    task_d2_2 = DummyOperator(task_id='stop_d2')
    task_show_config = PythonOperator(task_id='show_config',
                                  python_callable = show_conf,
                                  provide_context = True
                                  )
    
    task_show_config >> task_d2_2 >> task_d2_1
