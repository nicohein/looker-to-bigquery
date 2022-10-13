import pandas as pd
import sys
import pytest

sys.path.append("../")
from look2bq.executors import YamlExecuter


@pytest.fixture
def e():
    yaml_path = "test_looker.yaml"
    return YamlExecuter(yaml_path)


def test_plan_type(e):
    assert type(e.plan) == dict


def test_look_data(e):
    data = {
        "model": "recorded",
        "view": "spotify_dma_report",
        "fields": ["spotify_dma_report.artist_name", "spotify_dma_report.dma_code"],
        "pivots": [],
        "fill_fields": [],
        "filters": {},
        "filter_expression": "",
        "sorts": [],
        "limit": "",
        "column_limit": "",
        "total": False,
        "row_total": "",
        "subtotals": [],
        "vis_config": {},
        "filter_config": {},
        "visible_ui_sections": "",
        "dynamic_fields": "",
        "client_id": "",
        "query_timezone": "",
    }
    assert data == e.plan["lookertesttask"]["look"]


def test_execute(e):
    e.execute()
    assert type(e.store["look"]) == pd.DataFrame
