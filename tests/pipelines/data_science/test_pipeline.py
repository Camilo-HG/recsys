import logging
from typing import Any

import pandas as pd
import pytest
from kedro.io import DataCatalog
from kedro.runner import SequentialRunner

from rec_sys.pipelines.data_science import create_pipeline as create_ds_pipeline
from rec_sys.pipelines.data_science.nodes import split_data


@pytest.fixture
def dummy_data():
    return pd.DataFrame(
        {
            "engines": [1, 2, 3],
            "crew": [4, 5, 6],
            "passenger_capacity": [5, 6, 7],
            "price": [120, 290, 30],
        }
    )


@pytest.fixture
def dummy_parameters():
    parameters = {
        "model_options": {
            "test_size": 0.2,
            "random_state": 3,
            "features": ["engines", "passenger_capacity", "crew"],
        }
    }
    return parameters


def test_split_data(
    dummy_data: pd.DataFrame, dummy_parameters: dict[str, dict[str, Any]]
):
    X_train, X_test, y_train, y_test = split_data(
        dummy_data, dummy_parameters["model_options"]
    )
    expected_len = 2
    assert len(X_train) == expected_len
    assert len(y_train) == expected_len

    expected_len = 1
    assert len(X_test) == expected_len
    assert len(y_test) == expected_len


def test_split_data_missing_price(
    dummy_data: pd.DataFrame, dummy_parameters: dict[str, dict[str, Any]]
):
    dummy_data_missing_price = dummy_data.drop(columns="price")
    with pytest.raises(KeyError) as e_info:
        X_train, X_test, y_train, y_test = split_data(
            dummy_data_missing_price, dummy_parameters["model_options"]
        )

    assert "price" in str(e_info.value)


def test_data_science_pipeline(
    caplog: pytest.LogCaptureFixture,
    dummy_data: pd.DataFrame,
    dummy_parameters: dict[str, dict[str, Any]],
):
    pipeline = (
        create_ds_pipeline()
        .from_nodes("split_data_node")
        .to_nodes("evaluate_model_node")
    )
    catalog = DataCatalog()
    catalog.add_feed_dict(
        {
            "model_input_table": dummy_data,
            "params:model_options": dummy_parameters["model_options"],
        }
    )

    caplog.set_level(logging.DEBUG, logger="kedro")
    successful_run_msg = "Pipeline execution completed successfully."

    SequentialRunner().run(pipeline, catalog)

    assert successful_run_msg in caplog.text
