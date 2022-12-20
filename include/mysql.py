# import mysql.connector
# from mysql.connector import Error
import pandas as pd
import json


def connect():
    """ Connect to MySQL database """
    conn = None
    try:
        conn = mysql.connector.connect(host='localhost',
                                       database='db',
                                       user='puc',
                                       password='root')
        if conn.is_connected():
            print('Connected to MySQL database')

    except Error as e:
        print(e)

    finally:
        if conn is not None and conn.is_connected():
            conn.close()

def read_json():

    with open('data.json') as data_file:
        data = json.load(data_file)

    normalized_data = pd.json_normalize(data)

    return normalized_data

def check_database():
    pass

def check_table():
    pass

def populate_airbnb_trusted():
    data = read_json()
    print("shape of data after normalization: " + str(data.shape))

def populate_airbnb_refined():
    pass


