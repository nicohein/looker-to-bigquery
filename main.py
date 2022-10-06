from look2bq.executors import YamlExecuter
from look2bq.jobs import Look2Bq
import os
import logging
from flask import Flask, request
import pandas as pd
from dateutil import parser
from datetime import timedelta, date

logging.basicConfig(level=logging.ERROR, format="%(asctime)s %(message)s")

def get_dates_list(args):
    start_date_str = args.get('start_date')
    end_date_str = args.get('end_date')
    if not start_date_str or start_date_str == "":
        start_date = date.today() - timedelta(days=1)
        end_date = date.today() 
    else:
        start_date = parser.parse(start_date_str)
        end_date = parser.parse(end_date_str)
    dates = [(start_date+timedelta(days=x)).strftime('%Y-%m-%d') for x in range((end_date-start_date).days)]
    return {"dates": dates}

app = Flask(__name__)


@app.route("/")
def health():
    return "running"

@app.route("/execute_config")
def execute_config():
    args = request.args
    yaml_path = f"{args['query_config']}.yaml"
    exe = YamlExecuter(yaml_path)
    transaction_date = args.get('transaction_date', None)
    if parser.parse(transaction_date).date() >= date.today():
        return "date must be before today"
    if transaction_date:
        exe.plan['event_upload_task']['look']['filters'] = {"event.created_date" : transaction_date}
        exe.plan['event_upload_task']['bigquery']['date'] = transaction_date
    response = exe.execute()
    return response

@app.route("/execute_query")
def execute_query():
    args = request.args
    job = Look2Bq()
    job.run_from_plan(args)
    return f"{args['qid']} updated on {args['destination']} successfully"


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
