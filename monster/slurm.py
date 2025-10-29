import json
import subprocess
import time

import requests
from pgcopy import CopyManager
from requests.adapters import HTTPAdapter

from monster import logger, sql

log = logger.get_logger(__name__)


def get_slurm_token(slurm_config: dict):
    while True:
        try:
            # Setting command parameters
            slurm_headnode = slurm_config['headnode']

            # The command used in cli
            command = [f"ssh {slurm_headnode} 'scontrol token lifespan=3600'"]
            # Get the string from command line
            rtn_str = subprocess.run(command, shell=True, stdout=subprocess.PIPE).stdout.decode('utf-8')
            # Get token
            token = rtn_str.splitlines()[0].split('=')[1]
            timestamp = int(time.time())

            token_record = {
                'time': timestamp,
                'token': token
            }

            with open('./token.json', 'w') as f:
                json.dump(token_record, f, indent=4)

            return token
        except Exception as err:
            # Get Slurm token error! Try in 60s.
            time.sleep(60)


def read_slurm_token(slurm_config: dict):
    token = ""
    try:
        with open('./token.json', 'r') as f:
            token_record = json.load(f)
            time_interval = int(time.time()) - token_record['time']
            if time_interval >= 3600:
                token = get_slurm_token(slurm_config)
            else:
                token = token_record['token']
    # Catch the exception when the file does not exist
    except FileNotFoundError:
        token = get_slurm_token(slurm_config)
    return token


def call_slurm_api(slurm_config: dict, token: str, url: str):
    metrics = {}
    headers = {"X-SLURM-USER-NAME": slurm_config['user'],
               "X-SLURM-USER-TOKEN": token}
    adapter = HTTPAdapter(max_retries=3)
    with requests.Session() as session:
        session.mount(url, adapter)
        try:
            response = session.get(url, headers=headers)
            metrics = response.json()
        except Exception as err:
            log.error(f"Fetch slurm metrics error: {err}")
    return metrics


def get_slurm_jobs_metrics(slurm_config: dict, partition: str):
    url = f"http://{slurm_config['ip']}:{slurm_config['port']}{slurm_config['slurm_jobs']}"
    jobs_metric = call_slurm_api(slurm_config, read_slurm_token(slurm_config), url).get('jobs', [])
    # only keep the jobs in the specified partition
    jobs_metric = [job for job in jobs_metric if job['partition'] == partition]
    return jobs_metric


def dump_slurm_jobs_info(conn: object, jobs_info: list):
    target_table = 'slurm.jobs'
    cols = tuple(sql.job_info_column_names)
    all_records = []
    curs = conn.cursor()

    # Get all job ids from the jobs info
    job_ids = [job[cols.index('job_id')] for job in jobs_info]

    # Get all existing job ids from the table
    check_sql = """SELECT job_id FROM slurm.jobs"""
    curs.execute(check_sql)
    all_job_ids = curs.fetchall()
    all_job_ids = [job_id[0] for job_id in all_job_ids]
    all_job_ids = set(all_job_ids)
    job_ids = set(job_ids)

    # Get the job ids that are not in the table
    new_job_ids = job_ids - all_job_ids

    try:
        for job in jobs_info:
            # Append the new job ids to the all records
            if job[cols.index('job_id')] in new_job_ids:
                all_records.append(job)
            else:
                job_id = job[cols.index('job_id')]
                nodes = job[cols.index('nodes')]
                job_state = job[cols.index('job_state')]
                user_name = job[cols.index('user_name')]
                start_time = job[cols.index('start_time')]
                end_time = job[cols.index('end_time')]
                resize_time = job[cols.index('resize_time')]
                restart_cnt = job[cols.index('restart_cnt')]
                exit_code = job[cols.index('exit_code')]
                derived_exit_code = job[cols.index('derived_exit_code')]
                update_sql = """ UPDATE slurm.jobs 
                         SET nodes = %s, job_state = %s, user_name = %s, start_time = %s, end_time = %s, resize_time = %s, restart_cnt = %s, exit_code = %s, derived_exit_code = %s
                         WHERE job_id = %s """
                curs.execute(update_sql, (
                    nodes, job_state, user_name, start_time, end_time, resize_time, restart_cnt, exit_code,
                    derived_exit_code, job_id))

        mgr = CopyManager(conn, target_table, cols)
        mgr.copy(all_records)
        conn.commit()
    except Exception as err:
        curs.execute("ROLLBACK")
        log.error(f"Fail to dump job metrics: {err}")


def get_slurm_nodes_metrics(slurm_config: dict, hostname_list: list):
    url = f"http://{slurm_config['ip']}:{slurm_config['port']}{slurm_config['slurm_nodes']}"
    nodes_metric = call_slurm_api(slurm_config, read_slurm_token(slurm_config), url).get('nodes', [])
    # only keep the nodes in the specified partition
    nodes_metric = [node for node in nodes_metric if node["hostname"] in hostname_list]
    return nodes_metric


def dump_slurm_nodes_info(conn: object, nodes_info: dict):
    schema = 'slurm'
    curs = conn.cursor()
    cols = ('timestamp', 'nodeid', 'value')
    try:
        for table, records in nodes_info.items():
            target_table = f'{schema}.{table}'
            mgr = CopyManager(conn, target_table, cols)
            mgr.copy(records)
            conn.commit()
    except Exception as err:
        curs.execute("ROLLBACK")
        log.error(f"Fail to dump node metrics : {err}")


def dump_slurm_nodes_jobs(conn: object, nodes_jobs: list):
    target_table = 'slurm.node_jobs'
    curs = conn.cursor()
    cols = ('timestamp', 'nodeid', 'jobs', 'cpus')
    try:
        mgr = CopyManager(conn, target_table, cols)
        mgr.copy(nodes_jobs)
        conn.commit()
    except Exception as err:
        curs.execute("ROLLBACK")
        log.error(f"Fail to dump node-jobs correlation : {err}")
