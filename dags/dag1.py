from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from include.consumo_mongo import consumo_mongo
from include.mysql import populate_airbnb_trusted

def leitura_dados_mongo():
    consumo_mongo()

def popular_trusted():
    populate_airbnb_trusted()

dag = DAG('airbnb', description='ETL airbnb', schedule_interval=None, start_date=datetime(2022, 12, 18), catchup=False)

task_leitura_dados_mongo = PythonOperator(task_id='leitura_dos_dados_mongo', python_callable = leitura_dados_mongo, dag=dag)
task_popular_tabela_airbnb_trusted = PythonOperator(task_id='Popular_tabela_airbnb_trusted', python_callable = popular_trusted, dag=dag)

task_leitura_dados_mongo >> task_popular_tabela_airbnb_trusted