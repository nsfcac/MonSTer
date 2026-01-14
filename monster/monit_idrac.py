import multiprocessing
import time
import psycopg2
import schedule
from pgcopy import CopyManager
from datetime import datetime, timezone

from monster import idrac, utils


def monit_idrac_pull(config):
    cols = ('timestamp', 'nodeid', 'source', 'fqdd', 'value')
    connection         = utils.init_tsdb_connection(config)
    username, password = utils.get_idrac_auth()
    nodelist           = utils.get_nodelist(config)
    idrac_api          = utils.get_idrac_api(config)
    idrac_metrics      = utils.get_idrac_metrics(config)

    with psycopg2.connect(connection) as conn:
        nodeid_map = utils.get_nodeid_map(conn)
        fqdd_map   = utils.get_fqdd_source_map(conn, 'fqdd')
        source_map = utils.get_fqdd_source_map(conn, 'source')
        timestamp  = datetime.now(timezone.utc).replace(microsecond=0)
        processed_records = idrac.get_idrac_metrics_pull(idrac_api, timestamp, idrac_metrics,
                                                         nodelist, username, password,
                                                         nodeid_map, source_map, fqdd_map)
        for tabel, records in processed_records.items():
            mgr = CopyManager(conn, tabel, cols)
            mgr.copy(records)
        conn.commit()


def monit_idrac_push(config):
    connection         = utils.init_tsdb_connection(config)
    username, password = utils.get_idrac_auth()
    nodelist           = utils.get_nodelist(config)
    idrac_metrics      = utils.get_idrac_metrics(config)

    cores = multiprocessing.cpu_count()
    if (len(nodelist) < cores):
        cores = len(nodelist)
    nodelist_chunks = utils.partition_list(nodelist, cores)

    with psycopg2.connect(connection) as conn:
        nodeid_map           = utils.get_nodeid_map(conn)
        fqdd_map             = utils.get_fqdd_source_map(conn, 'fqdd')
        source_map           = utils.get_fqdd_source_map(conn, 'source')
        metric_dtype_mapping = utils.get_metric_dtype_mapping(conn)

    with multiprocessing.Pool(cores) as pool:
        pool.starmap(idrac.get_idrac_metrics_push, [(nodelist, idrac_metrics, username, password,
                                                     connection, nodeid_map, source_map,
                                                     fqdd_map, metric_dtype_mapping)
                                                     for nodelist in nodelist_chunks])


if __name__ == '__main__':
    config = utils.parse_config()
    
    idrac_model = utils.get_idrac_model(config)
    if idrac_model == "pull":
        schedule.every().minutes.at(":00").do(monit_idrac_pull, config)
        while True:
            schedule.run_pending()
            time.sleep(1)
    elif idrac_model == "push":
        monit_idrac_push(config)
