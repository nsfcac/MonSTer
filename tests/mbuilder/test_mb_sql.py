from mbuilder import mb_sql
from dateutil.parser import parse


def test_generate_slurm_jobs_sql():
    start = "2023-01-01 00:00:00"
    end = "2023-01-02 00:00:00"

    sql_query = mb_sql.generate_slurm_jobs_sql(start, end)

    start_epoch = int(parse(start).timestamp())
    end_epoch = int(parse(end).timestamp())

    expected_sql = f"SELECT * FROM slurm.jobs WHERE start_time < {end_epoch} AND end_time > {start_epoch};"

    assert expected_sql == sql_query


def test_generate_slurm_node_jobs_sql():
    start = "2023-01-01 00:00:00"
    end = "2023-01-02 00:00:00"
    interval = "5 minutes"

    sql_query = mb_sql.generate_slurm_node_jobs_sql(start, end, interval)

    expected_sql = f"SELECT time_bucket_gapfill('{interval}', timestamp) AS time, \
            nodes.hostname as node, jsonb_agg(jobs) AS jobs, jsonb_agg(cpus) AS cpus \
            FROM slurm.node_jobs \
            JOIN nodes \
            ON slurm.node_jobs.nodeid = nodes.nodeid \
            WHERE timestamp >= '{start}' \
            AND timestamp <= '{end}' \
            GROUP BY time, node \
            ORDER BY time;"

    assert expected_sql == sql_query


def test_generate_slurm_state_sql():
    start = "2023-01-01 00:00:00"
    end = "2023-01-02 00:00:00"
    interval = "1 hour"

    sql_query = mb_sql.generate_slurm_state_sql(start, end, interval)

    expected_sql = f"SELECT time_bucket_gapfill('{interval}', timestamp) AS time, \
            nodes.hostname as node, jsonb_agg(value) AS value \
            FROM slurm.state \
            JOIN nodes \
            ON slurm.state.nodeid = nodes.nodeid \
            WHERE timestamp >= '{start}' \
            AND timestamp <= '{end}' \
            GROUP BY time, node \
            ORDER BY time;"

    assert expected_sql == sql_query


def test_generate_idrac_metric_sql():
    table = "fans"
    start = "2023-01-01 00:00:00"
    end = "2023-01-02 00:00:00"
    interval = "1h"
    aggregation = "avg"

    sql_query = mb_sql.generate_idrac_metric_sql(table, start, end, interval, aggregation)

    expected_sql = f"SELECT time_bucket_gapfill('{interval}', timestamp) AS time, \
        nodes.hostname as node, fqdd.fqdd AS label, {aggregation}(value) AS value \
        FROM idrac.{table} \
        JOIN nodes \
        ON idrac.{table}.nodeid = nodes.nodeid \
        JOIN fqdd \
        ON idrac.{table}.fqdd = fqdd.id \
        WHERE timestamp >= '{start}' \
        AND timestamp <= '{end}' \
        GROUP BY time, node, label \
        ORDER BY time;"

    assert expected_sql == sql_query


def test_generate_idrac_metric_raw_sql():
    table = "temp"
    start = "2023-01-01 00:00:00"
    end = "2023-01-02 00:00:00"
    node = "node1"

    sql_query = mb_sql.generate_idrac_metric_raw_sql(table, start, end, node)

    expected_sql = f"SELECT timestamp AS time, \
        nodes.hostname as node, fqdd.fqdd AS label, value \
        FROM idrac.{table} \
        JOIN nodes \
        ON idrac.{table}.nodeid = nodes.nodeid \
        JOIN fqdd \
        ON idrac.{table}.fqdd = fqdd.id \
        WHERE timestamp >= '{start}' \
        AND timestamp <= '{end}' \
        AND nodes.hostname = '{node}' \
        ORDER BY time;"

    assert expected_sql == sql_query


def test_generate_slurm_metric_sql():
    table = "cpu"
    start = "2023-01-01 00:00:00"
    end = "2023-01-02 00:00:00"
    interval = "15m"
    aggregation = "max"

    sql_query = mb_sql.generate_slurm_metric_sql(table, start, end, interval, aggregation)

    expected_sql = f"SELECT time_bucket_gapfill('{interval}', timestamp) AS time, \
            nodes.hostname as node, {aggregation}(value) AS value \
            FROM slurm.{table} \
            JOIN nodes \
            ON slurm.{table}.nodeid = nodes.nodeid \
            WHERE timestamp >= '{start}' \
            AND timestamp <= '{end}' \
            GROUP BY time, node \
            ORDER BY time;"

    assert expected_sql == sql_query
