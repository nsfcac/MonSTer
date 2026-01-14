import os
import pytest
from unittest.mock import patch, MagicMock, mock_open
from monster import utils
import yaml
import json


def test_parse_config_infra_success(monkeypatch):
    fake_config = {
        "idrac": {"nodelist": ["node[1-2]"]}
    }

    monkeypatch.setattr(
        "argparse.ArgumentParser.parse_args",
        lambda self: type("args", (), {"config": "config.yml"})()
    )

    monkeypatch.setattr(
        "builtins.open",
        mock_open(read_data=yaml.dump(fake_config))
    )

    result = utils.parse_config()
    assert result == fake_config


def test_parse_config_partition_success(monkeypatch):
    valid_config = {
        "partition": "p1",
        "timescaledb": {"host": "h", "port": 5432, "database": "db"},
        "idrac": {"model": "pull", "nodelist": ["node1"]},
        "slurm_rest_api": {
            "ip": "1.1.1.1",
            "port": 123,
            "user": "u",
            "slurm_jobs": "/jobs",
            "slurm_nodes": "/nodes",
        },
        "fastapi": {"idrac": {}, "slurm": {}, "ip": "0.0.0.0", "port": 8000},
    }

    monkeypatch.setattr(
        "argparse.ArgumentParser.parse_args",
        lambda self: type("args", (), {"config": "config.yml"})()
    )

    monkeypatch.setattr(
        "builtins.open",
        mock_open(read_data=yaml.dump(valid_config))
    )

    assert utils.parse_config() == valid_config


def test_parse_config_file_missing(monkeypatch):
    monkeypatch.setattr("argparse.ArgumentParser.parse_args", lambda self: type('args', (), {'config': None})())
    with pytest.raises(SystemExit):
        utils.parse_config()


def test_parse_config_file_invalid(monkeypatch):
    monkeypatch.setattr("argparse.ArgumentParser.parse_args", lambda self: type('args', (), {'config': 'file.yml'})())
    with patch("builtins.open", side_effect=OSError("fail")):
        with pytest.raises(SystemExit):
            utils.parse_config()


def test_init_tsdb_connection_success(monkeypatch):
    config = {
        "timescaledb": {"host": "localhost", "port": 5432, "database": "db"}
    }

    monkeypatch.setenv("tsdb_username", "user")
    monkeypatch.setenv("tsdb_password", "pass")

    conn_str = utils.init_tsdb_connection(config)
    assert conn_str == "postgresql://user:pass@localhost:5432/db"


def test_get_idrac_auth_success(monkeypatch):
    monkeypatch.setenv("idrac_username", "user")
    monkeypatch.setenv("idrac_password", "pass")

    username, password = utils.get_idrac_auth()
    assert username == "user"
    assert password == "pass"


def test_get_idrac_auth_missing(monkeypatch):
    monkeypatch.delenv("idrac_username", raising=False)
    monkeypatch.delenv("idrac_password", raising=False)

    with pytest.raises(SystemExit):
        utils.get_idrac_auth()


def test_get_pdu_auth_success(monkeypatch):
    monkeypatch.setenv("pdu_username", "user")
    monkeypatch.setenv("pdu_password", "pass")

    username, password = utils.get_pdu_auth()
    assert username == "user"
    assert password == "pass"


def test_get_irc_auth_success(monkeypatch):
    monkeypatch.setenv("irc_username", "ircuser")
    assert utils.get_irc_auth() == "ircuser"


def test_get_irc_auth_missing(monkeypatch):
    monkeypatch.delenv("irc_username", raising=False)
    with pytest.raises(SystemExit):
        utils.get_irc_auth()


def test_get_nodelist_success(mocker):
    mocker.patch(
        "monster.utils.hostlist.expand_hostlist",
        return_value=["n1", "n2"]
    )

    config = {"idrac": {"nodelist": ["node[1-2]"]}}
    result = utils.get_nodelist(config)

    assert result == ["n1", "n2"]


def test_get_infra_ip_list_success(mocker):
    mocker.patch(
        "monster.utils.hostlist.expand_hostlist",
        return_value=["10.0.0.1"]
    )

    config = {"pdu": {"ip_list": ["10.0.0.[1]"]}}
    result = utils.get_infra_ip_list(config, "pdu")

    assert result == ["10.0.0.1"]


def test_sort_tuple_list():
    input_list = [(3, "c"), (1, "a"), (2, "b")]
    sorted_list = utils.sort_tuple_list(input_list)
    assert sorted_list == [(1, "a"), (2, "b"), (3, "c")]


def test_get_idrac_api_pull():
    config = {"idrac": {"model": "pull", "api": {"a": "/x"}}}
    assert list(utils.get_idrac_api(config)) == ["/x"]


def test_get_idrac_api_push():
    config = {"idrac": {"model": "push"}}
    assert utils.get_idrac_api(config) is None


def test_get_idrac_metrics_missing():
    assert utils.get_idrac_metrics({"idrac": {}}) == []


def test_get_nodeid_map():
    mock_conn = MagicMock()
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchall.return_value = [(1, "1.1.1.1"), (2, "2.2.2.2")]

    result = utils.get_nodeid_map(mock_conn)
    assert result == {"1.1.1.1": 1, "2.2.2.2": 2}
    mock_cursor.close.assert_called_once()


def test_get_metric_dtype_mapping():
    mock_conn = MagicMock()
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchall.return_value = [("m1", "INT"), ("m2", "REAL")]

    result = utils.get_metric_dtype_mapping(mock_conn)
    assert result == {"m1": "INT", "m2": "REAL"}
    mock_cursor.close.assert_called_once()


def test_get_slurm_config():
    config = {"slurm_rest_api": {"ip": "x"}}
    assert utils.get_slurm_config(config) == {"ip": "x"}


def test_get_ip_hostname_map_from_file(monkeypatch):
    monkeypatch.setattr(utils, "get_partition", lambda _: "p1")

    data = {"1.1.1.1": "node1"}
    monkeypatch.setattr(
        "builtins.open",
        mock_open(read_data=json.dumps(data))
    )

    result = utils.get_ip_hostname_map("conn", {})
    assert result == data


def test_get_ip_hostname_map_from_db(mocker, monkeypatch):
    monkeypatch.setattr(utils, "get_partition", lambda _: "p1")

    mock_conn = mocker.MagicMock()
    mock_conn.__enter__.return_value = mock_conn  # ⭐ key line

    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchall.return_value = [("1.1.1.1", "node1")]

    mocker.patch("psycopg2.connect", return_value=mock_conn)

    monkeypatch.setattr(
        "builtins.open",
        lambda *args, **kwargs: (_ for _ in ()).throw(FileNotFoundError)
    )

    result = utils.get_ip_hostname_map("conn", {})
    assert result == {"1.1.1.1": "node1"}


def test_partition_list_exact_division():
    arr = [1, 2, 3, 4]
    groups = utils.partition_list(arr, 2)
    assert groups == [[1, 2], [3, 4]]


def test_partition_list_with_remainder():
    arr = [1, 2, 3, 4, 5]
    groups = utils.partition_list(arr, 2)
    # Remaining element 5 gets appended to first group
    assert groups == [[1, 2, 5], [3, 4]]


def test_cast_value_type():
    assert utils.cast_value_type("123", "INT") == 123
    assert utils.cast_value_type("12.3", "REAL") == 12.3
    assert utils.cast_value_type("abc", "TEXT") == "abc"


def test_oid_string_to_tuple_valid():
    result = utils.oid_string_to_tuple("MODULE::1.2.3.A")
    assert result == ("MODULE", 1, 2, 3, "A")


def test_oid_string_to_tuple_invalid():
    with pytest.raises(ValueError):
        utils.oid_string_to_tuple("1.2.3")
