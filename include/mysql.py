import mysql.connector
from mysql.connector import Error
import sqlalchemy as db
import pandas as pd
import json

trusted_tbl = "airbnb_trusted"
refined_tbl = "airbnb_refined"

def read_json():

    with open('data.json') as data_file:
        data = json.load(data_file)

    normalized_data = pd.json_normalize(data)

    return normalized_data

def connect_db():
    """ Connect to MySQL database """
    my_conn = None

    config = {'host': 'host.docker.internal',
                'port': 3309,
                'user': 'puc',
                'password': 'root',
                'database': 'airbnb'}

    db_user = config.get('user')
    db_pwd = config.get('password')
    db_host = config.get('host')
    db_port = config.get('port')
    db_name = config.get('database')

    # specify connection string
    connection_str = f'mysql+pymysql://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}'

    my_conn = db.create_engine(connection_str)
    
    return my_conn

def populate_airbnb_trusted():
    data = read_json()
    print("shape of data after normalization: " + str(data.shape))

    # Connect to airbnb database
    conn = connect_db()

    data.to_sql(name=trusted_tbl, con=conn, if_exists='replace', index=False)

    print("Tabela criada")

    # sql = '''
    #     select colunas from trusted
    # '''

def populate_airbnb_refined():
    
    # cursor.execute(sql)
    pass

