import sys
import os
sys.path.append(os.path.abspath(os.environ["AIRFLOW_HOME"]))
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from elt.extract import extract
from elt.transform import transform
from elt.load import load

# RAPID API details
rapid_hotel_api_host_name = Variable.get('rapid_hotel_api_host_name', default_var = 'booking-com.p.rapidapi.com')
rapid_hotel_api_key = Variable.get('rapid_hotel_api_key')

# Azure details
azure_HB_storage_connection_string = Variable.get('azure_HB_storage_connection_string')
azure_raw_container_name = Variable.get('azure_raw_container_name')

default_args = {
    'owner' : 'abhijit',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'elt_hotel_data',
    description = 'This DAG represent the loading of hotel API in json and loading them into Azure blob storage as csv',
    default_args = default_args,
    start_date = datetime(2024, 4, 10),
    schedule = '0 0 1 * *'
) as dag :


    extract_hotel_api_data_task = PythonOperator(
        task_id =  'extract_hotel_api_data_task',
        python_callable = extract,
        op_kwargs = {'api_host_name': rapid_hotel_api_host_name, 'api_key': rapid_hotel_api_key}
    )

    transform_hotel_api_data_task = PythonOperator(
        task_id =  'transform_hotel_api_data_task',
        python_callable = transform
    )

    load_raw_data_task = PythonOperator(
        task_id = 'load_raw_data_task',
        python_callable = load,
        op_kwargs = {'connection_string': azure_HB_storage_connection_string, 'container_name': azure_raw_container_name}
    )

    # creating a dag
    extract_hotel_api_data_task >> transform_hotel_api_data_task >> load_raw_data_task

