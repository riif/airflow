from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowFailException

default_args = {
    "owner" : "airflow",
    "start_date" : datetime(2024, 5, 11, 10, 00, 00),
    "retries" : 1,
    "retry_delay" :timedelta(minutes=1)
}

def check_connection_db_postgres():
    try:
        pg_hook = PostgresHook(postgres_conn_id="PostgreSQL")
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        query = "select version()"
        cursor.execute(query)
        
        versions = cursor.fetchall()
        
        return versions
    except (Exception) as e:
        raise AirflowFailException(e)

with DAG('airflows',
         default_args=default_args,
         schedule_interval='@once',
         catchup=False) as dag:
    start = DummyOperator(task_id='start')
    checkConnectionDBPostgreSQL = PythonOperator(task_id = 'check-connection-databases', 
                                                 python_callable=check_connection_db_postgres)
    end = DummyOperator(task_id='end')
    
    start >> checkConnectionDBPostgreSQL >> end