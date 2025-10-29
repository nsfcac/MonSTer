from dateutil.parser import parse


def generate_slurm_jobs_sql(start: str, end: str):
    start_epoch = int(parse(start).timestamp())
    end_epoch = int(parse(end).timestamp())
    sql = f"SELECT * FROM slurm.jobs WHERE start_time < {end_epoch} AND end_time > {start_epoch};"
    return sql


def generate_slurm_node_jobs_sql(start: str, end: str, interval: str):
    sql = f"SELECT time_bucket_gapfill('{interval}', timestamp) AS time, \
            nodes.hostname as node, jsonb_agg(jobs) AS jobs, jsonb_agg(cpus) AS cpus \
            FROM slurm.node_jobs \
            JOIN nodes \
            ON slurm.node_jobs.nodeid = nodes.nodeid \
            WHERE timestamp >= '{start}' \
            AND timestamp <= '{end}' \
            GROUP BY time, node \
            ORDER BY time;"
    return sql


def generate_slurm_state_sql(start: str, end: str, interval: str):
    sql = f"SELECT time_bucket_gapfill('{interval}', timestamp) AS time, \
            nodes.hostname as node, jsonb_agg(value) AS value \
            FROM slurm.state \
            JOIN nodes \
            ON slurm.state.nodeid = nodes.nodeid \
            WHERE timestamp >= '{start}' \
            AND timestamp <= '{end}' \
            GROUP BY time, node \
            ORDER BY time;"
    return sql


def generate_idrac_metric_sql(table: str,
                              start: str,
                              end: str,
                              interval: str,
                              aggregation: str):
    sql = f"SELECT time_bucket_gapfill('{interval}', timestamp) AS time, \
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
    return sql


def generate_idrac_metric_raw_sql(table: str,
                                   start: str,
                                   end: str,
                                   node: str):
    sql = f"SELECT timestamp AS time, \
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
    return sql


def generate_slurm_metric_sql(table: str,
                              start: str,
                              end: str,
                              interval: str,
                              aggregation: str):
    sql = f"SELECT time_bucket_gapfill('{interval}', timestamp) AS time, \
            nodes.hostname as node, {aggregation}(value) AS value \
            FROM slurm.{table} \
            JOIN nodes \
            ON slurm.{table}.nodeid = nodes.nodeid \
            WHERE timestamp >= '{start}' \
            AND timestamp <= '{end}' \
            GROUP BY time, node \
            ORDER BY time;"
    return sql
