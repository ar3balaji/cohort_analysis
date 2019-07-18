from flask import Flask, request
from service import util
import json
app = Flask(__name__)


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
                    Enter week range: <input type="text" name="last_no_weeks" />
                    <input type="submit" />
                </form>
            </body>
        </html>
    """


@app.route('/load/customers', methods=["POST"])
def load_customers():
    input_file = request.files['data_file']
    return util.load_data('customer', input_file)


@app.route('/load/orders', methods=["POST"])
def load_orders():
    input_file = request.files['data_file']
    return util.load_data('app_order', input_file)


@app.route('/cohort/report', methods=["POST"])
def get_report():
    week_range = request.form.get('last_no_weeks')
    util.get_cohort_report()
    return 'Week Range chosen is: ' + week_range


if __name__ == '__main__':
    app.run(host='0.0.0.0')
