from typing import List, Dict
from flask import Flask, make_response, request
import mysql.connector
import json
import io
import pandas as pd
from sqlalchemy import create_engine

app = Flask(__name__)
user = 'root'
password = 'root'
host = 'localhost'
port = 32000
database = 'enterprise'

def favorite_colors() -> List[Dict]:
    config = {
        'user': 'root',
        'password': 'root',
        'host': 'localhost',
        'port': '32000',
        'database': 'enterprise'
    }
    connection = mysql.connector.connect(**config)
    cursor = connection.cursor()
    cursor.execute('SELECT * FROM customer')
    results = [{id: name} for (id, name) in cursor]
    cursor.close()
    connection.close()
    return results


@app.route('/import/customers')
def customers():
    return """
        <html>
            <body>
                <h1>Import customers csv</h1>

                <form action="/load/customers" method="post" enctype="multipart/form-data">
                    <input type="file" name="data_file" />
                    <input type="submit" />
                </form>
            </body>
        </html>
    """

@app.route('/import/orders')
def orders():
    return """
        <html>
            <body>
                <h1>Import orders csv</h1>

                <form action="/load/orders" method="post" enctype="multipart/form-data">
                    <input type="file" name="data_file" />
                    <input type="submit" />
                </form>
            </body>
        </html>
    """


@app.route('/load/customers', methods=["POST"])
def load_customers():
    input_file = request.files['data_file']
    return load_data('customer', input_file)


@app.route('/load/orders', methods=["POST"])
def load_orders():
    input_file = request.files['data_file']
    return load_data('app_order', input_file)


def load_data(table_name, input_file):
    if not input_file:
        return "File not uploaded"
    stream = io.StringIO(input_file.stream.read().decode("UTF8"), newline=None)
    index_start = 1
    database_connection = create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'.
                                                   format(user, password,
                                                          host, database), connect_args=dict(host=host,port=port))

    for df in pd.read_csv(stream, chunksize=20000, iterator=True, encoding='utf-8'):
        df['created'] = pd.to_datetime(df['created'])
        df.index += index_start
        df.to_sql(con=database_connection, name=table_name, index=False, if_exists='append')
        index_start = df.index[-1] + 1
    return "File imported!"



@app.route('/')
def index() -> str:
    return json.dumps({'customers': 'Hello you made it!'})


if __name__ == '__main__':
    app.run(host='0.0.0.0')