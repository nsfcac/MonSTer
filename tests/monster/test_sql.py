import pytest
from unittest.mock import MagicMock, patch
from monster import sql


def test_generate_metadata_table_sql():
    nodes_metadata = [{"HostName": "node1", "IP": "1.1.1.1"}]
    table_name = "nodes"

    expected_sql_str = " CREATE TABLE IF NOT EXISTS nodes \
      ( NodeID SERIAL PRIMARY KEY, HostName TEXT, IP TEXT, UNIQUE (NodeID));"

    sql_str = sql.generate_metadata_table_sql(nodes_metadata, table_name)

    assert sql_str == expected_sql_str


def test_write_nodes_metadata_calls_insert(mocker):
    conn = mocker.Mock()
    nodes_metadata = [{"HostName": "node1"}]

    check_mock = mocker.patch(
        "monster.sql.check_table_exist",
        return_value=False
    )
    insert_mock = mocker.patch("monster.sql.insert_metadata")
    update_mock = mocker.patch("monster.sql.update_metadata")

    sql.write_nodes_metadata(conn, nodes_metadata)

    check_mock.assert_called_once_with(conn, "nodes")
    insert_mock.assert_called_once_with(conn, nodes_metadata)
    update_mock.assert_not_called()


def test_write_nodes_metadata_calls_update(mocker):
    from monster import sql

    conn = mocker.Mock()
    nodes_metadata = [{"HostName": "node1"}]

    check_mock = mocker.patch(
        "monster.sql.check_table_exist",
        return_value=True
    )
    insert_mock = mocker.patch("monster.sql.insert_metadata")
    update_mock = mocker.patch("monster.sql.update_metadata")

    sql.write_nodes_metadata(conn, nodes_metadata)

    check_mock.assert_called_once_with(conn, "nodes")
    update_mock.assert_called_once_with(conn, nodes_metadata, "nodes")
    insert_mock.assert_not_called()


def test_check_table_exist():
    cur = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value = cur

    # Table exists, has data
    cur.fetchall.side_effect = [[(True,)], [(True,)]]
    result = sql.check_table_exist(conn, "nodes")
    assert result is True

    # Table does not exist
    cur.fetchall.side_effect = [[(False,)]]
    result = sql.check_table_exist(conn, "nonexistent")
    assert result is False


@patch("monster.sql.CopyManager")
def test_insert_metadata(mock_copymanager):
    conn = MagicMock()
    nodes_metadata = [{"HostName": "node1", "IP": "1.1.1.1"}]
    sql.insert_metadata(conn, nodes_metadata)
    mock_copymanager.assert_called_once()
    mock_copymanager.return_value.copy.assert_called_once()


@patch("monster.sql.CopyManager")
def test_insert_fqdd_source_metadata_calls_copy(mock_copymgr):
    conn = MagicMock()
    fqdd_metadata = ["F1", "F2"]
    sql.insert_fqdd_source_metadata(conn, fqdd_metadata, "fqdd")
    mock_copymgr.assert_called_once()
    mock_copymgr.return_value.copy.assert_called_once()


def test_update_metadata_infra(mocker):
    conn = mocker.Mock()
    cur = mocker.Mock()
    conn.cursor.return_value = cur

    nodes_metadata = [
        {"HostName": "node1"},
        {"HostName": "node2"}
    ]

    insert_mock = mocker.patch("monster.sql.insert_metadata")

    sql.update_metadata(conn, nodes_metadata, "infra")

    conn.cursor.assert_called_once()

    assert cur.execute.call_count == len(nodes_metadata)

    cur.execute.assert_has_calls([
        mocker.call("TRUNCATE TABLE infra RESTART IDENTITY;"),
        mocker.call("TRUNCATE TABLE infra RESTART IDENTITY;"),
    ])

    assert insert_mock.call_count == len(nodes_metadata)


def test_update_metadata_idrac(mocker):
    from monster.sql import update_metadata

    conn = mocker.Mock()
    cur = mocker.Mock()
    conn.cursor.return_value = cur

    nodes_metadata = [
        {
            "Bmc_Ip_Addr": "10.0.0.1",
            "HostName": "node1",
            "Status": "OK"
        }
    ]

    insert_mock = mocker.patch("monster.sql.insert_metadata")

    sql.update_metadata(conn, nodes_metadata, "nodes")

    conn.cursor.assert_called_once()
    cur.execute.assert_called_once()

    executed_sql = cur.execute.call_args[0][0]

    expected_executed_sql = "UPDATE nodes SET hostname = 'node1', status = 'OK' WHERE bmc_ip_addr = '10.0.0.1';"

    assert executed_sql == expected_executed_sql
    insert_mock.assert_not_called()


def test_generate_source_table_sql():
    sql_str = sql.generate_source_table_sql()

    expected_sql_str = "CREATE TABLE IF NOT EXISTS source \
          (id SERIAL PRIMARY KEY, source TEXT NOT NULL);"

    assert sql_str == expected_sql_str


def test_generate_fqdd_table_sql():
    sql_str = sql.generate_fqdd_table_sql()

    expected_sql_str = "CREATE TABLE IF NOT EXISTS fqdd \
          (id SERIAL PRIMARY KEY, fqdd TEXT NOT NULL);"

    assert sql_str == expected_sql_str


@patch("monster.sql.CopyManager")
def test_write_fqdd_source_metadata(mock_copymgr):
    conn = MagicMock()
    with patch("monster.sql.check_table_exist", return_value=False):
        sql.write_fqdd_source_metadata(conn, ["F1", "F2"], "fqdd")
        mock_copymgr.return_value.copy.assert_called_once()


def test_generate_metric_table_sqls_idrac():
    table_schemas = {
        "cpu": {
            "column_names": ["NodeID", "Value"],
            "column_types": ["INT", "REAL"]
        }
    }
    sqls = sql.generate_metric_table_sqls(table_schemas, "idrac")
    assert "CREATE SCHEMA IF NOT EXISTS idrac" in sqls["schema_sql"]
    assert any("FOREIGN KEY (NodeID)" in tbl for tbl in sqls["tables_sql"])
    assert any("FOREIGN KEY (fqdd)" in tbl for tbl in sqls["tables_sql"])


def test_generate_metric_table_sqls_non_idrac():
    table_schemas = {
        "cpu": {
            "column_names": ["NodeID", "Value"],
            "column_types": ["INT", "REAL"]
        }
    }
    sqls = sql.generate_metric_table_sqls(table_schemas, "non_idrac")
    assert "CREATE SCHEMA IF NOT EXISTS non_idrac" in sqls["schema_sql"]
    assert any("FOREIGN KEY (NodeID)" in tbl for tbl in sqls["tables_sql"])
    assert any("FOREIGN KEY (fqdd)" not in tbl for tbl in sqls["tables_sql"])


def test_generate_slurm_job_table_sql():
    sqls = sql.generate_slurm_job_table_sql("slurm")
    assert "CREATE SCHEMA if NOT EXISTS slurm" in sqls["schema_sql"]
    assert any("jobs" in tbl for tbl in sqls["tables_sql"])


@patch("monster.sql.CopyManager")
def test_write_metric_definitions_push(mock_copymgr):
    conn = MagicMock()
    metric_definitions = [{"Id": "cpu", "Name": "CPU", "Description": "", "MetricType": "Integer",
                           "MetricDataType": "Integer", "Units": "MHz", "Accuracy": 1,
                           "SensingInterval": "1s", "DiscreteValues": []}]
    with patch("monster.sql.check_table_exist", return_value=False):
        sql.write_metric_definitions_push(conn, metric_definitions)
        mock_copymgr.return_value.copy.assert_called_once()


@patch("monster.sql.CopyManager")
def test_write_metric_definitions_pull(mock_copymgr):
    conn = MagicMock()
    metric_definitions = [{"Id": "mem", "MetricDataType": "Integer", "Units": "GB"}]
    with patch("monster.sql.check_table_exist", return_value=False):
        sql.write_metric_definitions_pull(conn, metric_definitions)
        mock_copymgr.return_value.copy.assert_called_once()


@patch("monster.sql.CopyManager")
def test_write_metric_definitions_irc(mock_copymgr):
    conn = MagicMock()
    metric_definitions = [
        {"metric_id": "fans", "metric_name": "Fans", "metric_data_type": "INT", "units": "RPM", "snmp_oid": "1.3.6"}]
    with patch("monster.sql.check_table_exist", return_value=False):
        sql.write_metric_definitions_irc(conn, metric_definitions)
        mock_copymgr.return_value.copy.assert_called_once()
