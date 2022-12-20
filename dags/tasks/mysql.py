import mysql.connector
from mysql.connector import Error


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

def check_database():
    pass

def check_table():
    pass

def populate_airbnb_trusted():
    pass

def populate_airbnb_refined():
    pass
