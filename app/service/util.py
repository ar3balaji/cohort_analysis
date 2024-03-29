import io
from datetime import datetime
from datetime import timedelta
import pandas as pd
import math


def load_from_file(table_name, input_file, database_connection):
    try:
        return load_data(table_name, open(input_file).read(), database_connection)
    except FileNotFoundError:
        return "File not found!"
    except Exception as e:
        return "Error while processing!"


def load_data(table_name, input_file, database_connection):
    try:
        stream = io.StringIO(input_file, newline=None)
        index_start = 1

        for df in pd.read_csv(stream, chunksize=20000, iterator=True, encoding='utf-8'):
            df['created'] = pd.to_datetime(df['created'])
            df.index += index_start
            df.to_sql(con=database_connection, name=table_name, index=False, if_exists='append')
            index_start = df.index[-1] + 1
        return "File imported!"
    except Exception as e:
        return "Error while processing!"


def get_html_report(frames, headings):
    return pd.concat(frames, keys=headings).to_html()


def get_customers_query(timezone):
    if timezone == 'mt':
        return 'select id as user_id, date_add(created, interval - 6 hour) as signup_date from customer order by created'
    elif timezone == 'ct':
        return 'select id as user_id, date_add(created, interval - 5 hour) as signup_date from customer order by created'
    elif timezone == 'et':
        return 'select id as user_id, date_add(created, interval - 4 hour) as signup_date from customer order by created'
    else:
        return 'select id as user_id, date_add(created, interval - 7 hour) as signup_date from customer order by created'


def get_orders_query(timezone):
    if timezone == 'mt':
        return 'select id as order_id, order_number, user_id, date_add(created, interval - 6 hour) as order_date from app_order'
    elif timezone == 'ct':
        return 'select id as order_id, order_number, user_id, date_add(created, interval - 5 hour) as order_date from app_order'
    elif timezone == 'et':
        return 'select id as order_id, order_number, user_id, date_add(created, interval - 4 hour) as order_date from app_order'
    else:
        return 'select id as order_id, order_number, user_id, date_add(created, interval - 7 hour) as order_date from app_order'


def get_cohort_report(database_connection, timezone):
    try:
        reports = list()
        report_headings= list()
        N = 7
        cohort_date_format = "%Y-%m-%d"
        customers_data = pd.read_sql(get_customers_query(timezone),
                        database_connection)
        dataframe = pd.read_sql(get_orders_query(timezone),
                        database_connection)
        dataframe = dataframe.merge(customers_data, on="user_id").sort_values(by=['signup_date'])
        dataframe['first_order'] = 0

        earliest_signup_date = customers_data.iloc[0]['signup_date']
        last_signup_date = customers_data.iloc[len(customers_data)-1]['signup_date']
        cohorts = pd.date_range(start=last_signup_date, end=earliest_signup_date, freq='-'+str(N)+'D')

        buckets = []
        for index, row in dataframe.iterrows():
            bucket = math.floor((row['order_date'] - row['signup_date']).days/7)
            buckets.append(bucket)

        dataframe['week_number'] = buckets
        cohorts = list(cohorts.map(lambda x: x.strftime(cohort_date_format)))

        new_order_indices = dataframe.sort_values(['user_id', 'order_date']).groupby('user_id').head(1).index
        dataframe.loc[new_order_indices, 'first_order'] = 1

        for cohort in cohorts:
            enddate = datetime.strptime(cohort, cohort_date_format)
            start = enddate - timedelta(days=6)

            t = datetime.strptime(cohort, cohort_date_format)
            mask = (dataframe['signup_date'] <= t + timedelta(days=1)) & (dataframe['signup_date'] >= t - timedelta(days=N-1))

            c_t = datetime.strptime(cohort, cohort_date_format)
            c_mask = (customers_data['signup_date'] <= c_t + timedelta(days=1)) & (customers_data['signup_date'] >= c_t - timedelta(days=N - 1))
            cust_count = len(customers_data[c_mask])

            cohort_orders = dataframe[mask]
            grouped = cohort_orders.groupby('week_number')
            cohort_result = grouped.agg({'user_id': pd.Series.nunique }).reset_index()
            cohort_result.rename(columns={'user_id': 'unique orderers'}, inplace=True)
            v = cohort_orders.loc[cohort_orders['first_order'] == 1].groupby(['week_number']).first_order.count().reset_index(name='first_time_orderers_count')
            cohort_result['first_time_orderers'] = v['first_time_orderers_count']
            cohort_result = cohort_result.fillna(0)
            cohort_result['first_time_orderers_pct'] = cohort_result['first_time_orderers'].map(lambda a: 0 if a==0 else round((a/cust_count)*100,2))
            cohort_result['unique_orderers_pct'] = cohort_result['unique orderers'].map(lambda a: 0 if a==0 else round((a/cust_count)*100,2))
            cohort_result['first_time_orderers_disp'] = cohort_result['first_time_orderers_pct'].map(str) + '% 1st time (' + cohort_result['first_time_orderers'].map(str) + ')'
            cohort_result['unique_orderers_disp'] = cohort_result['unique_orderers_pct'].map(str) + '% orderers (' + cohort_result['unique orderers'].map(str) + ')'
            report_headings.append("Cohort " + start.strftime("%m/%d/%y") + " - " + enddate.strftime("%m/%d/%y"))
            reports.append(cohort_result[['week_number','first_time_orderers_disp','unique_orderers_disp']])
        return get_html_report(reports, report_headings)
    except:
        return "Error while processing!"
