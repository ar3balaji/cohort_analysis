import io
import mysql.connector
from datetime import datetime
from datetime import timedelta
import pandas as pd
import math
from typing import List, Dict
from sqlalchemy import create_engine

user = 'root'
password = 'root'
host = 'localhost'
port = 32000
database = 'enterprise'
database_connection = create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'.format(user, password, host, database),
                          connect_args=dict(host=host, port=port))


def load_data(table_name, input_file):
    if not input_file:
        return "File not uploaded"
    stream = io.StringIO(input_file.stream.read().decode("UTF8"), newline=None)
    index_start = 1

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


def get_cohort_report():
    N = 7
    cohort_date_format = "%Y-%m-%d"
    print("Cohort size: " + str(N))
    customers_data = pd.read_sql('select id as user_id, created as signup_date from customer order by created',
                    database_connection)
    dataframe = pd.read_sql('select id as order_id, order_number, user_id, created as order_date from app_order',
                    database_connection)
    dataframe = dataframe.merge(customers_data, on="user_id").sort_values(by=['signup_date'])
    dataframe['first_order'] = 0

    earliest_signup_date = customers_data.iloc[0]['signup_date']
    last_signup_date = customers_data.iloc[len(customers_data)-1]['signup_date']
    cohorts = pd.date_range(start=last_signup_date, end=earliest_signup_date, freq='-'+str(N)+'D')
    print("Earliest signup: ", earliest_signup_date)
    print("Last signup: ", last_signup_date)
    print("No. of cohorts: ", len(cohorts))

    buckets = []
    for index, row in dataframe.iterrows():
        bucket = math.floor((row['order_date'] - row['signup_date']).days/7)
        buckets.append(bucket)

    dataframe['cohort_bucket'] = buckets
    cohorts = list(cohorts.map(lambda x: x.strftime(cohort_date_format)))
    print("Cohorts:", len(cohorts))

    customers = []
    for cohort in cohorts:
        t = datetime.strptime(cohort, cohort_date_format)
        mask = (customers_data['signup_date'] <= t + timedelta(days=1)) & (customers_data['signup_date'] >= t - timedelta(days=N-1))
        customers.append(len(customers_data[mask]))
    print("Customers:", len(customers))

    new_order_indices = dataframe.sort_values(['user_id', 'order_date']).groupby('user_id').head(1).index
    dataframe.loc[new_order_indices, 'first_order'] = 1
    print("First orders", len(dataframe.loc[dataframe['first_order'] == 1]))
    print("Later orders", len(dataframe.loc[dataframe['first_order'] == 0]))

    for cohort in cohorts:
        enddate = datetime.strptime(cohort, cohort_date_format)
        start = enddate - timedelta(days=6)
        t = datetime.strptime(cohort, cohort_date_format)
        mask = (dataframe['signup_date'] <= t + timedelta(days=1)) & (dataframe['signup_date'] >= t - timedelta(days=N-1))
        c_t = datetime.strptime(cohort, cohort_date_format)
        c_mask = (customers_data['signup_date'] <= c_t + timedelta(days=1)) & (
                    customers_data['signup_date'] >= c_t - timedelta(days=N - 1))
        cust_count = len(customers_data[c_mask])
        cohort_orders = dataframe[mask]
        grouped = cohort_orders.groupby('cohort_bucket')
        cohort_result = grouped.agg({'user_id': pd.Series.nunique }).reset_index()
        cohort_result.rename(columns={'user_id': 'unique orderers'}, inplace=True)
        v = cohort_orders.groupby(['cohort_bucket','first_order']).first_order.count().reset_index(name='first_time_orderers_count')
        cohort_result['first_time_orderers'] = v['first_time_orderers_count']
        cohort_result['first_time_orderers_pct'] = cohort_result['first_time_orderers'].map(lambda a: round((a/cust_count)*100,2))
        cohort_result['unique_orderers_pct'] = cohort_result['unique orderers'].map(lambda a: round((a/cust_count)*100,2))
        cohort_result['first_time_orderers_disp'] = cohort_result['first_time_orderers_pct'].map(str) + '% 1st time (' + cohort_result['first_time_orderers'].map(str) + ')'
        cohort_result['unique_orderers_disp'] = cohort_result['unique_orderers_pct'].map(str) + '% orderers (' + cohort_result['unique orderers'].map(str) + ')'
        print("Cohort", start.strftime("%m/%d/%y"), "-", enddate.strftime("%m/%d/%y"))
        print(cohort_result[['first_time_orderers_disp','unique_orderers_disp']])
