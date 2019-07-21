from flask import Flask, request
from service import util
from sqlalchemy import create_engine
import os
import json

app = Flask(__name__)

user = os.getenv('DB_USER','root')
password = os.getenv('DB_PASSWORD','root')
host = os.getenv('DB_HOST','localhost')
port = int(os.getenv('DB_PORT',32000))
database = os.getenv('DB_DATABASE','enterprise')
database_connection = create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'.format(user, password, host, database),
                          connect_args=dict(host=host, port=port))


@app.route('/')
def index() -> str:
    return json.dumps({'customers': 'Hello you made it!'})


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


@app.route('/cohort/analysis')
def get_report_form():
    return """
        <html>
            <body>
                <h1>Cohort customer order behaviour analysis!</h1>
                <form action="/cohort/report" method="post">
                     <fieldset id="timezone">
                        <input type="radio" value="pt" name="timezone" checked>Pacific time zone</input></br>
                        <input type="radio" value="mt" name="timezone">Mountain time zone</input></br>
                        <input type="radio" value="ct" name="timezone">Central time zone</input></br>
                        <input type="radio" value="et" name="timezone">Eastern time zone</input>
                      </fieldset>
                    <input type="submit" value="Click to Generate Report"/>
                </form>
            </body>
        </html>
    """


@app.route('/load/customers', methods=["POST"])
def load_customers():
    input_file = request.files['data_file']
    return util.load_data('customer', input_file.stream.read().decode("UTF8"), database_connection)


@app.route('/load/orders', methods=["POST"])
def load_orders():
    input_file = request.files['data_file']
    return util.load_data('app_order', input_file.stream.read().decode("UTF8"), database_connection)


@app.route('/cohort/report', methods=["POST"])
def get_report():
    return util.get_cohort_report(database_connection, request.form.get('timezone'))


if __name__ == '__main__':
    app.run(host='0.0.0.0')
