import pytest
from monster import schema, utils


def test_build_idrac_table_schemas():
    metrics = [
        {'Id': 'FanSpeed', 'MetricDataType': 'INT', 'Units': 'RPM'},
        {'Id': 'NetworkBytes', 'MetricDataType': 'INT', 'Units': 'By'},
        {'Id': 'Temperature', 'MetricDataType': 'REAL', 'Units': 'C'}
    ]

    table_schemas = schema.build_idrac_table_schemas(metrics)

    assert set(table_schemas.keys()) == {'FanSpeed', 'NetworkBytes', 'Temperature'}

    assert table_schemas['FanSpeed']['column_types'][-1] == utils.data_type_mapping.get('INT', 'TEXT')
    assert table_schemas['NetworkBytes']['column_types'][-1] == 'BIGINT'
    assert table_schemas['Temperature']['column_types'][-1] == utils.data_type_mapping.get('REAL', 'TEXT')

    for table in table_schemas.values():
        assert table['column_names'] == ['Timestamp', 'NodeID', 'Source', 'FQDD', 'Value']


def test_build_slurm_table_schemas():
    slurm_schemas = schema.build_slurm_table_schemas()

    expected_tables = ['memoryusage', 'memory_used', 'cpu_load', 'state', 'node_jobs']
    assert set(slurm_schemas.keys()) == set(expected_tables)

    mem_table = slurm_schemas['memoryusage']
    assert mem_table['column_names'] == ['Timestamp', 'NodeID', 'Value']
    assert mem_table['column_types'] == ['TIMESTAMPTZ NOT NULL', 'INT NOT NULL', 'REAL']

    jobs_table = slurm_schemas['node_jobs']
    assert jobs_table['column_names'] == ['Timestamp', 'NodeID', 'Jobs', 'CPUs']
    assert jobs_table['column_types'] == ['TIMESTAMPTZ NOT NULL', 'INT NOT NULL', 'INTEGER[]', 'INTEGER[]']


def test_build_pdu_table_schemas():
    pdu_schemas = schema.build_pdu_table_schemas()
    assert set(pdu_schemas.keys()) == {'pdu'}

    pdu_table = pdu_schemas['pdu']
    assert pdu_table['column_names'] == ['Timestamp', 'NodeID', 'Value']
    assert pdu_table['column_types'] == ['TIMESTAMPTZ NOT NULL', 'INT NOT NULL', 'REAL']


def test_build_irc_table_schemas():
    irc_metrics = [
        {'metric_id': 'TempSensor', 'metric_data_type': 'REAL'},
        {'metric_id': 'FanSensor', 'metric_data_type': 'INT'}
    ]

    irc_schemas = schema.build_irc_table_schemas(irc_metrics)
    assert set(irc_schemas.keys()) == {'TempSensor', 'FanSensor'}

    temp_table = irc_schemas['TempSensor']
    assert temp_table['column_names'] == ['Timestamp', 'NodeID', 'Value']
    assert temp_table['column_types'] == ['TIMESTAMPTZ NOT NULL', 'INT NOT NULL', 'REAL']

    fan_table = irc_schemas['FanSensor']
    assert fan_table['column_types'][-1] == 'INT'
