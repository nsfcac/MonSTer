import json

import pytest
from unittest.mock import MagicMock, patch, mock_open
from monster import slurm


def test_get_slurm_token(mocker):
    slurm_config = {"headnode": "head1"}

    mock_completed = MagicMock()
    mock_completed.stdout = b"SLURM_JWT=abc123\n"

    mocker.patch("monster.slurm.subprocess.run", return_value=mock_completed)
    mocker.patch("monster.slurm.time.time", return_value=1000)
    mocker.patch("builtins.open", mock_open())
    mocker.patch("monster.slurm.json.dump")

    token = slurm.get_slurm_token(slurm_config)

    assert token == "abc123"


def test_read_slurm_token_valid(mocker):
    slurm_config = {}

    token_record = {"time": 900, "token": "cached"}

    mocker.patch("builtins.open", mock_open(read_data=json.dumps(token_record)))
    mocker.patch("monster.slurm.time.time", return_value=1000)

    token = slurm.read_slurm_token(slurm_config)

    assert token == "cached"


def test_read_slurm_token_missing_file(mocker):
    mocker.patch("builtins.open", side_effect=FileNotFoundError)
    mocker.patch("monster.slurm.get_slurm_token", return_value="fresh")

    token = slurm.read_slurm_token({})

    assert token == "fresh"


def test_read_slurm_token_expired(mocker):
    slurm_config = {}

    token_record = {"time": 0, "token": "old"}

    mocker.patch("builtins.open", mock_open(read_data=json.dumps(token_record)))
    mocker.patch("monster.slurm.time.time", return_value=4000)
    mocker.patch("monster.slurm.get_slurm_token", return_value="new")

    token = slurm.read_slurm_token(slurm_config)

    assert token == "new"


def test_call_slurm_api(mocker):
    slurm_config = {"user": "slurmuser"}
    token = "abc"
    url = "http://slurm/api"

    mock_response = MagicMock()
    mock_response.json.return_value = {"jobs": [1, 2]}

    mock_session = MagicMock()
    mock_session.get.return_value = mock_response
    mock_session.__enter__.return_value = mock_session

    mocker.patch("monster.slurm.requests.Session", return_value=mock_session)

    result = slurm.call_slurm_api(slurm_config, token, url)

    assert result == {"jobs": [1, 2]}


def test_get_slurm_jobs_metrics(mocker):
    slurm_config = {
        "ip": "1.1.1.1",
        "port": 80,
        "slurm_jobs": "/jobs"
    }

    jobs = [
        {"job_id": 1, "partition": "debug"},
        {"job_id": 2, "partition": "prod"},
    ]

    mocker.patch("monster.slurm.read_slurm_token", return_value="t")
    mocker.patch("monster.slurm.call_slurm_api", return_value={"jobs": jobs})

    result = slurm.get_slurm_jobs_metrics(slurm_config, "prod")

    assert result == [{"job_id": 2, "partition": "prod"}]


def test_dump_slurm_jobs():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor

    with patch("monster.slurm.sql.job_info_column_names",
               ["job_id", "nodes", "job_state", "user_name",
                "start_time", "end_time", "resize_time",
                "restart_cnt", "exit_code", "derived_exit_code"]), \
            patch("monster.slurm.CopyManager") as mock_mgr:
        cursor.fetchall.return_value = [(1,)]

        jobs_info = [
            (1, "n1", "RUNNING", "u", 0, 0, 0, 0, 0, 0),
            (2, "n2", "PENDING", "u", 0, 0, 0, 0, 0, 0),
        ]

        slurm.dump_slurm_jobs_info(conn, jobs_info)

    mock_mgr.return_value.copy.assert_called_once()
    conn.commit.assert_called()


def test_get_slurm_nodes_metrics(mocker):
    slurm_config = {
        "ip": "1.1.1.1",
        "port": 80,
        "slurm_nodes": "/nodes"
    }

    nodes = [
        {"hostname": "n1"},
        {"hostname": "n2"},
    ]

    mocker.patch("monster.slurm.read_slurm_token", return_value="t")
    mocker.patch("monster.slurm.call_slurm_api", return_value={"nodes": nodes})

    result = slurm.get_slurm_nodes_metrics(slurm_config, ["n2"])

    assert result == [{"hostname": "n2"}]


def test_dump_slurm_nodes_info():
    conn = MagicMock()

    nodes_info = {
        "cpu": [(1, 1, 10)],
        "mem": [(1, 1, 20)],
    }

    with patch("monster.slurm.CopyManager") as mock_mgr:
        slurm.dump_slurm_nodes_info(conn, nodes_info)

    assert mock_mgr.call_count == 2
    assert conn.commit.call_count == 2


def test_dump_slurm_nodes_jobs():
    conn = MagicMock()
    nodes_jobs = [(1, 1, 5, 10)]

    with patch("monster.slurm.CopyManager") as mock_mgr:
        slurm.dump_slurm_nodes_jobs(conn, nodes_jobs)

    mock_mgr.return_value.copy.assert_called_once_with(nodes_jobs)
    conn.commit.assert_called_once()
