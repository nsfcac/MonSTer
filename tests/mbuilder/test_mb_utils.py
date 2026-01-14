import pytest
import pandas as pd
from unittest.mock import MagicMock
from mbuilder import mb_utils


@pytest.fixture
def simple_df():
    return pd.DataFrame([
        {"node": "n1", "time": "2024-01-01", "value": 10}
    ])


def test_get_metrics_map():
    cfg = {"fastapi": {"a": 1}}
    assert mb_utils.get_metrics_map(cfg) == {"a": 1}


def test_get_jobs_cpus_jobs():
    row = {
        "jobs_copy": [[1, 2]],
        "cpus": [[4, 8]]
    }
    assert mb_utils.get_jobs_cpus(row, "jobs") == [1, 2]


def test_get_jobs_cpus_cpus():
    row = {
        "jobs_copy": [[1, 2]],
        "cpus": [[4, 8]]
    }
    assert mb_utils.get_jobs_cpus(row, "cpus") == [4, 8]


def test_get_jobs_cpus_invalid_item():
    row = {}
    assert mb_utils.get_jobs_cpus(row, "x") == []


def test_query_db_with_engine_string(mocker, simple_df):
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = MagicMock()

    mocker.patch("sqlalchemy.create_engine", return_value=engine)
    mocker.patch("pandas.read_sql_query", return_value=simple_df)

    result = mb_utils.query_db("db://url", "select", ["n1"])
    assert result[0]["node"] == "n1"
    assert isinstance(result[0]["time"], int)


def test_query_db_jobs_processing(mocker):
    df = pd.DataFrame([
        {
            "node": "n1",
            "time": "2024-01-01",
            "jobs": [[1, 2]],
            "cpus": [[4, 8]]
        }
    ])

    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = MagicMock()
    mocker.patch("pandas.read_sql_query", return_value=df)

    result = mb_utils.query_db(engine, "sql", ["n1"])
    assert result[0]["jobs"] == [1, 2]
    assert result[0]["cpus"] == [4, 8]


def test_query_db_wrapper_slurm_jobs(mocker):
    mocker.patch(
        "mbuilder.mb_sql.generate_slurm_jobs_sql",
        return_value="SQL"
    )

    query_mock = mocker.patch(
        "mbuilder.mb_utils.query_db",
        return_value=[{"a": 1}]
    )

    result = mb_utils.query_db_wrapper(
        "e", "s", "e", "i", "a", ["n1"], "slurm.jobs"
    )

    query_mock.assert_called_once()
    assert result == [{"a": 1}]


def test_query_db_wrapper_slurm_node_jobs(mocker):
    mocker.patch(
        "mbuilder.mb_sql.generate_slurm_node_jobs_sql",
        return_value="SQL"
    )

    query_mock = mocker.patch(
        "mbuilder.mb_utils.query_db",
        return_value=[{"b": 2}]
    )

    result = mb_utils.query_db_wrapper(
        "e", "s", "e", "i", "a", ["n1"], "slurm.node_jobs"
    )

    query_mock.assert_called_once()
    assert result == [{"b": 2}]


def test_query_db_wrapper_slurm_state(mocker):
    mocker.patch(
        "mbuilder.mb_sql.generate_slurm_state_sql",
        return_value="SQL"
    )

    query_mock = mocker.patch(
        "mbuilder.mb_utils.query_db",
        return_value=[{"c": 3}]
    )

    result = mb_utils.query_db_wrapper(
        "e", "s", "e", "i", "a", ["n1"], "slurm.state"
    )

    query_mock.assert_called_once()
    assert result == [{"c": 3}]


def test_query_db_wrapper_idrac_metric(mocker):
    # Patch SQL generator
    mocker.patch(
        "mbuilder.mb_sql.generate_idrac_metric_sql",
        return_value="SQL"
    )

    query_mock = mocker.patch(
        "mbuilder.mb_utils.query_db",
        return_value=[{"d": 4}]
    )

    result = mb_utils.query_db_wrapper(
        "dummy_engine", "start", "end", "interval", "aggregation", ["n1"], "idrac.metric"
    )

    query_mock.assert_called_once()
    assert result == [{"d": 4}]


def test_rename_device_gpu_and_cpu():
    results = {
        "idrac.gpuusage": [{"label": "Video.Slot.31-1", "value": 10}],
        "idrac.temperaturereading": [
            {"label": "iDRAC.Embedded.1#CPU1Temp", "value": 50}
        ],
        "idrac.cpuusage": [{"label": "X", "value": 1}]
    }

    out = mb_utils.rename_device({}, results)

    assert out["idrac.gpuusage"][0]["label"] == "GPU-0"
    assert out["idrac.temperaturereading"][0]["label"] == "CPU-0"
    assert out["idrac.cpuusage"][0]["label"] == "CPU"


def test_reformat_results_basic():
    results = {
        "slurm.jobs": [
            {
                "job_id": 1,
                "nodes": ["n1"],
                "cpus": 4,
                "memory_per_cpu": 2,
                "node_count": 1
            }
        ],
        "idrac.systempowerconsumption": [
            {"node": "n1", "time": 100, "value": 200}
        ]
    }

    out = mb_utils.reformat_results("h100", results)

    assert "nodes" in out
    assert "jobs" in out
    assert out["nodes"][0]["system_power_consumption"] == 200


def test_reformat_results_gpu_usage():
    results = {
        "idrac.systempowerconsumption": [
            {"node": "n1", "time": 1, "value": 100}
        ],
        "idrac.gpuusage": [
            {"node": "n1", "time": 1, "label": "GPU-0", "value": 50}
        ]
    }

    out = mb_utils.reformat_results("h100", results)
    node = out["nodes"][0]

    assert node["gpu_usage"] == [50]
    assert node["gpu_usage_labels"] == ["GPU-0"]


def test_reformat_results_node_time_records():
    results = {
        "slurm.jobs": [
            {
                "job_id": "job1",
                "nodes": ["n1"],
                "cpus": 2,
                "memory_per_cpu": 4,
                "node_count": 1
            },
            {
                "job_id": "job2",
                "nodes": ["n1"],
                "cpus": 2,
                "memory_per_cpu": 2,
                "node_count": 1
            }
        ],
        "idrac.systempowerconsumption": [
            {"node": "n1", "time": 10, "value": 100}
        ],
        "idrac.temperaturereading": [
            {"node": "n1", "time": 10, "label": "CPU-0", "value": 70}
        ],
        "idrac.cpupower": [
            {"node": "n1", "time": 10, "label": "CPU-0", "value": 40}
        ],
        "idrac.powerconsumption": [
            {"node": "n1", "time": 10, "label": "GPU-0", "value": 30}
        ],
        "slurm.node_jobs": [
            {"node": "n1", "time": 10, "jobs": ["job1", "job2"], "cpus": [2, 2]}
        ]
    }

    out = mb_utils.reformat_results("h100", results)

    node = out["nodes"][0]
    assert node["temperature"] == [70]
    assert node["temperature_labels"] == ["CPU-0"]
    assert node["cpu_power_consumption"] == [40]
    assert node["cpu_power_consumption_labels"] == ["CPU-0"]
    assert node["gpu_power_consumption"] == [30]
    assert node["gpu_power_consumption_labels"] == ["GPU-0"]

    jobs = {job["job_id"]: job for job in out["jobs"]}
    assert jobs["job1"]["power"] > 0
    assert jobs["job1"]["cores"] == 2
    assert jobs["job2"]["power"] > 0
    assert jobs["job2"]["cores"] == 2

    for job in jobs.values():
        assert job["power_per_core"] > 0

    for job in jobs.values():
        assert len(job["data"]) == 1
