import os
from sqlalchemy import create_engine
from service.util import load_from_file, get_cohort_report
import mysql.connector

user = os.getenv('DB_USER','root')
password = os.getenv('DB_PASSWORD','root')
host = os.getenv('DB_HOST','localhost')
port = int(os.getenv('DB_PORT',32000))
database = os.getenv('DB_DATABASE','enterprise')
config = {
    'user': user,
    'password': password,
    'host': host,
    'port': port,
    'database': database
}

engine = create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'.format(user, password, host, database),
                            connect_args=dict(host=host, port=port))


def clean_db(table_name):
    connection = mysql.connector.connect(**config)
    cursor = connection.cursor()
    cursor.execute('delete FROM ' + table_name)
    connection.commit()
    cursor.close()
    connection.close()


def get_count(table_name):
    connection = mysql.connector.connect(**config)
    cursor = connection.cursor()
    cursor.execute('select * from ' + table_name)
    data = cursor.fetchall()
    count =len(data)
    cursor.close()
    connection.close()
    return count


def test_import_customers():
    assert load_from_file('customer', 'customers.csv', engine) == 'File imported!', 'File imported!'


def test_import_orders():
    assert load_from_file('app_order', 'orders.csv', engine) == 'File imported!', 'File imported!'


def test_import_customers_file_nf():
    assert load_from_file('customer', 'customers1.csv', engine) == 'File not found!', 'File not found'


def test_clean():
    clean_db

def check_reprot():
    result = get_cohort_report(engine)
    assert str(open('output.txt').read()) == str(result), 'Check report'


if __name__ == "__main__":
    clean_db('customer')
    clean_db('app_order')
    test_import_customers()
    test_import_customers_file_nf()
    test_import_orders()
    assert get_count('customer') == 3, 'Customer count'
    assert get_count('app_order') == 5, 'Customer count'
    check_reprot()
    clean_db('customer')
    clean_db('app_order')
    print("Everything passed!")
