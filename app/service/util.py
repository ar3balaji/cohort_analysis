import io
import pandas as pd
import mysql.connector
from typing import List, Dict
from sqlalchemy import create_engine

user = 'root'
password = 'root'
host = 'localhost'
port = 32000
database = 'enterprise'


def load_data(table_name, input_file):
    if not input_file:
        return "File not uploaded"
    stream = io.StringIO(input_file.stream.read().decode("UTF8"), newline=None)
    index_start = 1
    database_connection = create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'.format(user, password,host, database),
                                        connect_args=dict(host=host,port=port))

    for df in pd.read_csv(stream, chunksize=20000, iterator=True, encoding='utf-8'):
        df['created'] = pd.to_datetime(df['created'])
        df.index += index_start
        df.to_sql(con=database_connection, name=table_name, index=False, if_exists='append')
        index_start = df.index[-1] + 1
    return "File imported!"


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
