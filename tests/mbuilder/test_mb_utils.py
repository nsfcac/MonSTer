import pytest
import pandas as pd
from unittest.mock import MagicMock
from mbuilder import mb_utils
from sqlalchemy.engine import Engine


def test_get_metrics_map():
    config = {"fastapi": {"idrac": {"GPU_Usage": "gpu_usage"}}}

    metrics_map = (mb_utils.get_metrics_map(config))

    expected_metrics_map = config['fastapi']

    assert metrics_map == expected_metrics_map


def test_get_jobs_cpus():
    row = {
        "jobs_copy": [["job_1", "job_2"]],
        "cpus": [[4, 8]]
    }

    get_jobs = mb_utils.get_jobs_cpus(row, "jobs")
    get_cpus = mb_utils.get_jobs_cpus(row, "cpus")
    get_unknown = mb_utils.get_jobs_cpus(row, "unknown")

    expected_jobs = ["job_1", "job_2"]
    expected_cpus = [4, 8]

    assert get_jobs == expected_jobs
    assert get_cpus == expected_cpus
    assert get_unknown == []


def test_query_db_with_engine_object(mocker):
    df = pd.DataFrame([
        {
            "node": "node1",
            "time": "2024-01-01 00:00:00",
            "jobs": [["job_1", "job_2"]],
            "cpus": [[4, 8]],
            "value": 100,
            "metric": None
        },
        {
            "node": "node2",
            "time": "2024-01-02 00:00:00",
            "jobs": [["job_3", "job_4"]],
            "cpus": [[12, 16]],
            "value": None,
            "metric": 200
        }
    ])
    engine = MagicMock(spec=Engine)
    engine.connect.return_value.__enter__.return_value = MagicMock()
    mocker.patch("pandas.read_sql_query", return_value=df)

    result = mb_utils.query_db(engine, "SELECT * FROM table", ["node1"])

    expected_result = [{
        "node": "node1",
        "time": 1704067200,
        "jobs": ["job_1", "job_2"],
        "cpus": [4, 8],
        "value": 100,
        "metric": float('-inf')
    }]

    assert result == expected_result
    engine.dispose.assert_not_called()


def test_query_db_empty_dataframe(mocker):
    empty_df = pd.DataFrame()
    engine = MagicMock(spec=Engine)
    engine.connect.return_value.__enter__.return_value = MagicMock()
    mocker.patch("pandas.read_sql_query", return_value=empty_df)

    result = mb_utils.query_db(engine, "SELECT * FROM table", ["n1"])

    assert result == {}


def test_query_db_engine_string(mocker):
    df = pd.DataFrame([{"node": "node1", "value": 100}, {"node": "node2"}])
    mock_engine = MagicMock(spec=Engine)
    mock_engine.connect.return_value.__enter__.return_value = MagicMock()
    mocker.patch("sqlalchemy.create_engine", return_value=mock_engine)
    mocker.patch("pandas.read_sql_query", return_value=df)

    result = mb_utils.query_db("postgresql://host/db", "SELECT *", ["node1", "node2"])

    expected_result = [{
        "node": "node1",
        "value": 100,
    }, {
        "node": "node2",
        "value": float('-inf'),
    }]

    assert result == expected_result
    mock_engine.dispose.assert_called_once()


@pytest.mark.parametrize('table,expected_sql,should_call', [
    ('slurm.jobs', 'SQL_JOBS', True),
    ('slurm.node_jobs', 'SQL_NODE_JOBS', True),
    ('slurm.state', 'SQL_STATE', True),
    ('slurm.cpu', 'SQL_SLURM_METRIC', True),
    ('slurm.memory', 'SQL_SLURM_METRIC', True),
    ('idrac.power', 'SQL_IDRAC_METRIC', True),
    ('idrac.temp', 'SQL_IDRAC_METRIC', True),
    ('unknown.table', None, False),
    ('random', None, False),
])
def test_query_db_wrapper_parameterized(mocker, table, expected_sql, should_call):
    nodelist = ["node1", "node2"]
    engine = MagicMock()
    mocker.patch('mbuilder.mb_sql.generate_slurm_jobs_sql', return_value='SQL_JOBS')
    mocker.patch('mbuilder.mb_sql.generate_slurm_node_jobs_sql', return_value='SQL_NODE_JOBS')
    mocker.patch('mbuilder.mb_sql.generate_slurm_state_sql', return_value='SQL_STATE')
    mocker.patch('mbuilder.mb_sql.generate_slurm_metric_sql', return_value='SQL_SLURM_METRIC')
    mocker.patch('mbuilder.mb_sql.generate_idrac_metric_sql', return_value='SQL_IDRAC_METRIC')
    mock_query_db = mocker.patch("mbuilder.mb_utils.query_db", return_value=[{'data': 'result'}])

    result = mb_utils.query_db_wrapper(
        engine, '2024-01-01', '2024-01-02', '1h', 'avg', nodelist, table
    )

    if should_call:
        mock_query_db.assert_called_once_with(engine, expected_sql, nodelist)
        assert result == [{'data': 'result'}]
    else:
        mock_query_db.assert_not_called()
        assert result == []


def test_rename_device():
    results_input = {
        "idrac.gpuusage": [{"label": "Video.Slot.31-1", "value": 10}],
        "idrac.temperaturereading": [
            {"label": "iDRAC.Embedded.1#CPU1Temp", "value": 50},
            {"label": "iDRAC.Embedded.1#GPUTemp33"}
        ],
        "idrac.cpuusage": [{"label": "X", "value": 1}],
        "idrac.cpupower": [{"label": "CPU.Socket.1", "value": 2}],
        "idrac.systempowerconsumption": [{"value": 2}],
        "idrac.drampwr": [],
        "idrac.memoryusage": [{"label": "ABC"}]
    }

    result = mb_utils.rename_device({}, results_input)

    expected_result = {
        "idrac.gpuusage": [{'label': "GPU-0", "value": 10}],
        "idrac.temperaturereading": [{"label": "CPU-0", "value": 50}, {"label": "GPU-1"}],
        "idrac.cpuusage": [{"label": "CPU", "value": 1}],
        "idrac.cpupower": [{"label": "CPU-0", "value": 2}],
        "idrac.systempowerconsumption": [{"label": "System", "value": 2}],
        "idrac.drampwr": [],
        "idrac.memoryusage": [{"label": "DRAM"}]
    }
    assert result == expected_result


def test_reformat_results_not_h100():
    results_input = {
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
        ],
        "idrac.temperaturereading": [
            {"label": "xyz", "node": "n1", "time": 100, "value": 111}
        ],
        "idrac.memoryusage": [
            {"node": "n1", "time": 100, "value": 222}
        ],
        "idrac.drampwr": [
            {"label": "a", "node": "n1", "time": 100, "value": 123}
        ],
        "slurm.node_jobs": [
            {"label": "l1", "node": "n1", "time": 100, "jobs": [1, 2], "cpus": [3, 4]}
        ]
    }

    result = mb_utils.reformat_results("unknown", results_input)

    expected_result = {
        "job_details": [{
            "job_id": 1,
            "nodes": ["n1"],
            "cpus": 4,
            "memory_per_cpu": 2,
            "node_count": 1
        }],
        "nodes": [
            {'time': 100, 'node': 'n1', 'used_cores': 7, 'jobs': [1, 2], 'cores': [3, 4], 'gpu_usage_labels': [],
             'gpu_usage': [], 'gpu_power_consumption_labels': [], 'gpu_power_consumption': [],
             'gpu_memory_usage_labels': [], 'gpu_memory_usage': [], 'temperature_labels': ["xyz"], 'temperature': [111],
             'cpu_usage': float('-inf'), 'cpu_power_consumption_labels': [], 'cpu_power_consumption': [],
             'dram_usage': 222, 'memory_usage': float('-inf'), 'dram_power_consumption_labels': ["a"],
             'dram_power_consumption': [123], 'system_power_consumption': 200}],
        "jobs": [{"time": 100,
                  "job_id": 1,
                  "data": [{
                      'node': "n1",
                      'power': 85.71,
                      'cores': 3,
                  }],
                  "power": 85.71,
                  "cores": 3,
                  "power_per_core": 12.24,
                  "memory_per_core": 2,
                  "memory_used": 8
                  }, {"time": 100,
                      "job_id": 2,
                      "data": [{
                          'node': "n1",
                          'power': 114.29,
                          'cores': 4,
                      }],
                      "power": 114.29,
                      "cores": 4,
                      "power_per_core": 16.33,
                      "memory_per_core": 0,
                      "memory_used": 0
                      }
                 ]
    }

    assert result == expected_result


def test_reformat_results_h100():
    results_input = {
        "idrac.gpuusage": [
            {"label": "l1", "node": "n1", "time": 123, "value": 1}
        ],
        "idrac.powerconsumption": [
            {"label": "l2", "node": "n1", "time": 123, "value": 2}
        ],
        "idrac.gpumemoryusage": [
            {"label": "l3", "node": "n1", "time": 123, "value": 3}
        ],
        "idrac.cpuusage": [
            {"node": "n1", "time": 123, "value": 4}
        ],
        "idrac.cpupower": [
            {"label": "l1", "node": "n1", "time": 123, "value": 5}
        ]
    }

    result = mb_utils.reformat_results("h100", results_input)

    expected_result = {
        "job_details": [],
        "nodes": [
            {'time': 123, 'node': 'n1', 'used_cores': float('-inf'), 'jobs': [], 'cores': [],
             'gpu_usage_labels': ["l1"],
             'gpu_usage': [1], 'gpu_power_consumption_labels': ["l2"], 'gpu_power_consumption': [2],
             'gpu_memory_usage_labels': ["l3"], 'gpu_memory_usage': [3], 'temperature_labels': [], 'temperature': [],
             'cpu_usage': 4, 'cpu_power_consumption_labels': ["l1"], 'cpu_power_consumption': [5],
             'dram_usage': float('-inf'), 'memory_usage': float('-inf'), 'dram_power_consumption_labels': [],
             'dram_power_consumption': [], 'system_power_consumption': float('-inf')}],
        "jobs": []
    }

    assert result == expected_result
