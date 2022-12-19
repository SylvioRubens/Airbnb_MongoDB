from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from tasks.consumo_mongo import consumo_mongo

def leitura_dados_mongo():
    consumo_mongo()

dag = DAG('airbnb', description='ETL airbnb', schedule_interval=None, start_date=datetime(2022, 12, 18), catchup=False)

task_leitura_dados_mongo = PythonOperator(task_id='mongo', python_callable = leitura_dados_mongo, dag=dag)

task_leitura_dados_mongo