import mysql.connector
from mysql.connector import Error
import pandas as pd
import json

trusted_tbl = "airbnb_trusted"
refined_tbl = "airbnb_refined"

def connect():
    """ Connect to MySQL database """
    conn = None
    try:
        conn = mysql.connector.connect(host='host.docker.internal',
                                        database='airbnb',
                                        user='puc',
                                        password='root',
                                        port=3309)
        if conn.is_connected():
            print('Connected to MySQL database')

    except:
        raise

def read_json():

    with open('data.json') as data_file:
        data = json.load(data_file)

    normalized_data = pd.json_normalize(data)

    return normalized_data

def connect_db():
    """ Connect to MySQL database """
    conn = None
    try:
        conn = mysql.connector.connect(host='host.docker.internal',
                                        database='airbnb',
                                        user='puc',
                                        password='root',
                                        port=3309)
        if conn.is_connected():
            print('Connected to MySQL database')
            cursor = conn.cursor()

    except Error as e:
        print(e)
    
    return cursor, conn

def check_table(conn, dataframe):
    # dataframe.to_sql(conn, dataframe)
    pass

def populate_airbnb_trusted():
    data = read_json()
    print("shape of data after normalization: " + str(data.shape))

    # Connect to airbnb database
    cursor, conn = connect_db()

    check_table(conn, data)

    # sql = '''
    #     select colunas from trusted
    # '''

def populate_airbnb_refined():
    
    # cursor.execute(sql)
    pass

