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

var_1 = ['ani','budi']
var_2 = {'nama':'ani'}



def push_wo_xcompush(**context):
    return var_1

def push_xcompush(**context):
    context['ti'].xcom_push(key='task_push_xcompush', value = var_2)
    
def pull(**context):
    ti = context["ti"]
    var =  ti.xcom_pull(key = 'task_push_xcompush', task_ids = 'xcom_push', dag_id = 'DAG_3', include_prior_dates= True )
    print(var)
    

with DAG( dag_id = 'DAG_3', 
         default_args = default_args, 
         schedule_interval='*/15 * * * *', 
        catchup=False) as dag_1:
    
    task_1 = DummyOperator(task_id = 'start')
    task_2 = DummyOperator(task_id = 'stop')
    
    task_wo_xcompush = PythonOperator(task_id='wo_xcom_push',
                                  python_callable = push_wo_xcompush,
                                  provide_context = True )
    
    task_xcompush = PythonOperator(task_id='xcom_push',
                                  python_callable = push_xcompush,
                                  provide_context = True )

    task_xcompull = PythonOperator(task_id='xcom_pull',
                                  python_callable = pull, 
                                  provide_context = True )


    task_1 >> task_wo_xcompush >> task_xcompush >> task_xcompull >> task_2
    
    # task_trigger = TriggerDagRunOperator(task_id='trigger_task',
    #                                      trigger_dag_id='DAG_2'
    #                                      ) 
    
    # task_show_context >> task_3 >> task_trigger
    

with DAG(dag_id= 'DAG_4',
         default_args = default_args,
         schedule_interval=None, 
         catchup=False) as dag_2 :
    

    task_pull = PythonOperator(task_id='pull_another_dag',
                                  python_callable = pull,
                                  provide_context = True
                                  )
    
#     task_show_config >> task_d2_2 >> task_d2_1
