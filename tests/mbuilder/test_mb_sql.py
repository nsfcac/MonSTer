from mbuilder import mb_sql


def test_generate_slurm_jobs_sql():
    sql_query = mb_sql.generate_slurm_jobs_sql("2023-01-01 00:00:00", "2023-01-02 00:00:00")
    assert "SELECT * FROM slurm.jobs" in sql_query
    assert "start_time" in sql_query
    assert "end_time" in sql_query
    assert "1672552800" in sql_query  # 2023-01-01 00:00:00 UTC epoch
    assert "1672639200" in sql_query  # 2023-01-02 00:00:00 UTC epoch


def test_generate_slurm_node_jobs_sql():
    sql_query = mb_sql.generate_slurm_node_jobs_sql("2023-01-01", "2023-01-02", "5 minutes")
    assert "time_bucket_gapfill('5 minutes'" in sql_query
    assert "jsonb_agg(jobs)" in sql_query
    assert "JOIN nodes" in sql_query


def test_generate_slurm_state_sql():
    sql_query = mb_sql.generate_slurm_state_sql("2023-01-01", "2023-01-02", "1 hour")
    assert "time_bucket_gapfill('1 hour'" in sql_query
    assert "jsonb_agg(value)" in sql_query
    assert "JOIN nodes" in sql_query


def test_generate_idrac_metric_sql():
    sql_query = mb_sql.generate_idrac_metric_sql("fans", "2023-01-01", "2023-01-02", "1h", "AVG")
    assert "idrac.fans" in sql_query
    assert "AVG(value)" in sql_query
    assert "GROUP BY time, node, label" in sql_query


def test_generate_idrac_metric_raw_sql():
    sql_query = mb_sql.generate_idrac_metric_raw_sql("fans", "2023-01-01", "2023-01-02", "node1")
    assert "idrac.fans" in sql_query
    assert "nodes.hostname = 'node1'" in sql_query
    assert "ORDER BY time" in sql_query


def test_generate_slurm_metric_sql():
    sql_query = mb_sql.generate_slurm_metric_sql("cpu_load", "2023-01-01", "2023-01-02", "15m", "MAX")
    assert "slurm.cpu_load" in sql_query
    assert "MAX(value)" in sql_query
    assert "GROUP BY time, node" in sql_query
