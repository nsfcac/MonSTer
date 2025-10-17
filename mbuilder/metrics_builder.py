import json
import hostlist
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import create_engine

from mbuilder import mb_utils
from monster import utils


MAX_DB_WORKERS = 4


def metrics_builder(config,
                    start=None,
                    end=None,
                    interval=None,
                    aggregation=None,
                    nodelist=None,
                    metrics=None):
    results = {}
    tables = []

    connection          = utils.init_tsdb_connection(config)
    ip_hostname_mapping = utils.get_ip_hostname_map(connection, config)
    metrics_mapping     = mb_utils.get_metrics_map(config)
    nodelist            = hostlist.expand_hostlist(nodelist)
    partition           = utils.get_partition(config)

    # Convert IPs to hostnames of the nodes
    nodelist = [ip_hostname_mapping[ip] for ip in nodelist]

    # Parse the metrics
    if metrics:
        for metric in metrics:
            if metric in metrics_mapping['idrac'].keys():
                table = metrics_mapping['idrac'][metric]
                if table not in tables:
                    # Add the table only if it is not already in the list
                    # This avoids duplicate queries for the same table
                    tables.append(f'idrac.{table}')
            elif metric in metrics_mapping['slurm'].keys():
                table = metrics_mapping['slurm'][metric]
                tables.append(f'slurm.{table}')
            else:
                return {"ERROR: " : f"Metric '{metric}' is not supported or not found in the configuration."}
    else:
        return {}

    if not tables:
        return {}

    # Parallelize the queries with a bounded thread pool to avoid exhausting DB connections
    engine = create_engine(connection, pool_pre_ping=True, pool_size=MAX_DB_WORKERS, max_overflow=0)

    def fetch_table(table_name):
        return table_name, mb_utils.query_db_wrapper(engine, start, end, interval, aggregation, nodelist, table_name)

    max_workers = min(len(tables), MAX_DB_WORKERS) or 1

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for table, record in executor.map(fetch_table, tables):
                results[table] = record
    finally:
        engine.dispose()

    rename_results = mb_utils.rename_device(metrics_mapping, results)

    # # Write the renamed results to a json file
    # with open(f"./results-{start.split(' ')[0]}-{end.split(' ')[0]}_rename.json", "w") as f:
    #     f.write(json.dumps(rename_results, indent=2))

    # Reformat the results required by the frontend
    reformat_results = mb_utils.reformat_results(partition, rename_results)

    return reformat_results


if __name__ == "__main__":
    config = utils.parse_config()
    # For testing purposes
    start = '2025-10-14 22:00:00-05'
    end = '2025-10-14 23:00:00-05'
    interval = '5m'
    aggregation = 'max'
    nodelist = ""
    metrics = []

    if utils.get_partition(config) == 'h100':
        nodelist = "10.101.93.[1-8]"
        metrics = ['GPU_Usage', 'GPU_PowerConsumption', 'GPU_MemoryUsage', 'GPU_Temperature', \
                'CPU_Usage', 'CPU_PowerConsumption', 'CPU_Temperature', 'Memory_Usage', \
                'DRAM_PowerConsumption', 'System_PowerConsumption', \
                'Jobs_Info', 'NodeJobs_Correlation', 'Nodes_State']
    elif utils.get_partition(config) == 'zen4':
        nodelist = "10.101.91.[1-20],10.101.92.[1-20],10.101.94.[1-20],10.101.95.[1-10],10.101.96.[1-20],10.101.97.[1-20]"
        metrics = ['CPU_Usage', 'CPU_PowerConsumption', 'CPU_Temperature', 'Memory_Usage', \
                'DRAM_PowerConsumption', 'System_PowerConsumption', \
                'Jobs_Info', 'NodeJobs_Correlation', 'Nodes_State']

    if metrics:
        results = metrics_builder(config, start, end, interval, aggregation, nodelist, metrics)
        # Write the results to a file
        with open(f"./results-{start.split(' ')[0]}-{end.split(' ')[0]}_fromat.json", "w") as f:
            f.write(json.dumps(results, indent=2))
