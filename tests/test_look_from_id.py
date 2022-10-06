import pandas as pd
import sys
import pytest

sys.path.append("../")
from look2bq.executors import YamlExecuter


@pytest.fixture
def e():
    yaml_path = "test_look_from_id.yaml"
    return YamlExecuter(yaml_path)


def test_look_data(e):
    data = {
        "qid": "Rk0daUlQJTRlDwFrAGZN7E",
    }
    assert data == e.plan["lookfromidtask"]["look_from_id"]


def test_execute(e):
    e.execute()
    assert type(e.store["look_from_id"]) == pd.DataFrame
