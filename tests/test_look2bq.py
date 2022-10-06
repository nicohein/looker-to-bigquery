import pandas as pd
import sys
import pytest

sys.path.append("../")
from look2bq.executors import YamlExecuter


@pytest.fixture
def e():
    yaml_path = "test_look2bq.yaml"
    return YamlExecuter(yaml_path)


def test_look_data(e):
    data = {
        "qid": "Rk0daUlQJTRlDwFrAGZN7E",
        "destination": "bmg-bigquery-prod.looker_system_activity.looker_user_data_test",
        "if_exists": "replace",
    }
    assert data == e.plan["looktobqtask"]["look2bq"]


def test_execute(e):
    e.execute()
    assert type(e.store["look2bq"]) == pd.DataFrame
