import pandas as pd
import sys
import pytest
import os
import pandas_gbq

sys.path.append("../")
from look2bq.executors import YamlExecuter
from look2bq.jobs import BigqueryExporter


@pytest.fixture
def e():
    yaml_path = "test_bigquery.yaml"
    return YamlExecuter(yaml_path)


def test_biquery_credentials(e):
    e.enable(BigqueryExporter())
    credentials = e._jobs["bigquery"]._get_impersonated_credentials()
    if os.environ.get("GOOGLE_IMPERSONATE_SERVICE_ACCOUNT") is not None:
        assert credentials is not None
    else:
        assert credentials is None

def test_biquery_data(e):
    data = {
        "destination": "bmg-bigquery-prod.looker_system_activity.looker_bigquery_test",
        'if_exists': 'replace'
    }
    assert e.plan["bqtesttask"]["bigquery"] == data


def test_execute(e):
    e.enable(BigqueryExporter())
    credentials = e._jobs["bigquery"]._get_impersonated_credentials()
    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = os.getenv("GOOGLE_PROJECT", None)
    e.execute()
    
    df = pandas_gbq.read_gbq(
        "SELECT * FROM `bmg-bigquery-prod.looker_system_activity.looker_bigquery_test` LIMIT 10",
        project_id="bmg-bigquery-prod",
    )
    assert type(df) == pd.DataFrame
    assert df.shape[0] > 1
