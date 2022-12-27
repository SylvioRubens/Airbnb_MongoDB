from airflow import DAG
from datetime import datetime
from include.consumo_mongo import consumo_mongo
from include.mysql import populate_airbnb_trusted
from include.mysql import populate_airbnb_refined
from include.mysql import close_connection
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def leitura_dados_mongo():
    consumo_mongo()

def popular_trusted():
    populate_airbnb_trusted()

def popular_refined():
    populate_airbnb_refined()

def close_connection_mysql():
    close_connection()

dag = DAG('airbnb', description='ETL airbnb', schedule_interval=None, start_date=datetime(2022, 12, 18), catchup=False)

inicio = DummyOperator(task_id = "inicio")
task_leitura_dados_mongo = PythonOperator(task_id='Leitura_dos_dados_mongo', python_callable = leitura_dados_mongo, dag=dag)
task_popular_tabela_airbnb_trusted = PythonOperator(task_id='Popular_tabela_airbnb_trusted', python_callable = popular_trusted, dag=dag)
task_popular_tabela_airbnb_refined = PythonOperator(task_id='Popular_tabela_airbnb_refined', python_callable = popular_refined, dag=dag)
task_close_connection_mysql = PythonOperator(task_id='Fechar_conexao_mysql', python_callable = close_connection_mysql, dag=dag)
fim = DummyOperator(task_id = "fim")

inicio >> task_leitura_dados_mongo >> task_popular_tabela_airbnb_trusted >> task_popular_tabela_airbnb_refined >> task_close_connection_mysql >> fim
