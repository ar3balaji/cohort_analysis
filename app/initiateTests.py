import os
from sqlalchemy import create_engine
from service.util import load_from_file, get_cohort_report, get_customers_query, get_orders_query
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


def test_get_customers_pt():
    assert get_customers_query('pt') == 'select id as user_id, date_add(created, interval - 7 hour) as signup_date from customer order by created', 'Received correct query'


def test_get_customers_mt():
    assert get_customers_query('mt') == 'select id as user_id, date_add(created, interval - 6 hour) as signup_date from customer order by created', 'Received correct query'


def test_get_orders_pt():
    assert get_orders_query('pt') == 'select id as order_id, order_number, user_id, date_add(created, interval - 7 hour) as order_date from app_order', 'Received correct query'


def test_get_orders_mt():
    assert get_orders_query('mt') == 'select id as order_id, order_number, user_id, date_add(created, interval - 6 hour) as order_date from app_order', 'Received correct query'


def test_clean():
    clean_db


def check_report():
    result = get_cohort_report(engine, 'pt')
    assert str(open('output.txt').read()) == result, 'Check report'


if __name__ == "__main__":
    clean_db('customer')
    clean_db('app_order')
    test_import_customers()
    test_import_customers_file_nf()
    test_import_orders()
    assert get_count('customer') == 3, 'Customer count'
    assert get_count('app_order') == 5, 'Customer count'
    check_report()
    clean_db('customer')
    clean_db('app_order')
    test_get_customers_pt()
    test_get_customers_mt()
    test_get_orders_pt()
    test_get_orders_mt()
    print("Everything passed!")
