from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from task1 import get_weather_data
from task2 import create_table
from task3 import insert_columns

default_args = {
    "owner": "Mukesh",
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 15),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

dag = DAG("Dag", default_args=default_args, schedule_interval="0 6 * * *")

t1 = PythonOperator(task_id='create_csv', python_callable=get_weather_data, dag=dag)
t2 = PythonOperator(task_id='create_table', python_callable=create_table, dag=dag)
t3 = PythonOperator(task_id='insert_value', python_callable=insert_columns, dag=dag)

t1 >> t2 >> t3
