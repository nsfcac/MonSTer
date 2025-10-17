import time
import psycopg2
import schedule
import urllib3
from datetime import datetime, timezone

import process
import slurm
from monster import utils

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def monit_slurm(config):
    timestamp = datetime.now(timezone.utc).replace(microsecond=0)

    connection      = utils.init_tsdb_connection(config)
    nodelist        = utils.get_nodelist(config)
    partition       = utils.get_partition(config)
    slurm_config    = utils.get_slurm_config(config)
    ip_hostname_map = utils.get_ip_hostname_map(connection, config)
    hostname_id_map = utils.get_hostname_id_map(connection, config)
    hostname_list   = [ip_hostname_map[ip] for ip in nodelist]

    jobs_metrics  = slurm.get_slurm_jobs_metrics(slurm_config, partition)
    nodes_metrics = slurm.get_slurm_nodes_metrics(slurm_config, hostname_list)    

    # Extract job information
    jobs_info = process.process_job_metrics_slurm(jobs_metrics)

    # Extract node information
    nodes_info = process.process_node_metrics_slurm(nodes_metrics,
                                                    hostname_id_map,
                                                    timestamp)
    # Extract node-job correlation
    nodes_jobs = process.process_node_job_correlation(jobs_metrics,
                                                      hostname_id_map,
                                                      timestamp)

    with psycopg2.connect(connection) as conn:
        slurm.dump_slurm_jobs_info(conn, jobs_info)
        slurm.dump_slurm_nodes_info(conn, nodes_info)
        slurm.dump_slurm_nodes_jobs(conn, nodes_jobs)


if __name__ == "__main__":
    config = utils.parse_config()
    schedule.every().minutes.at(":00").do(monit_slurm, config)
    while True:
        schedule.run_pending()
        time.sleep(1)
