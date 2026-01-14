import json
import asyncio
import multiprocessing
from itertools import repeat

import aiohttp
import hostlist
import requests
import urllib3
from aiohttp_sse_client import client as sse_client
from dateutil.parser import parse
from pgcopy import CopyManager
from requests.adapters import HTTPAdapter

from monster import snmp_irc, utils, logger, sql

log = logger.get_logger(__name__)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


async def base_fetch(url: str, session: aiohttp.ClientSession, max_retries=3, retry_delay=2):
    timeout = aiohttp.ClientTimeout(total=45)

    for attempt in range(max_retries):
        try:
            async with session.get(url, timeout=timeout, verify_ssl=False) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as err:
            if attempt + 1 == max_retries:
                log.error(f"Cannot fetch data from {url} : {err}")
                return {}
            await asyncio.sleep(retry_delay)


async def fetch_all(urls: list, username: str, password: str):
    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(username, password)) as session:
        tasks = []
        for url in urls:
            task = asyncio.create_task(base_fetch(url, session))
            tasks.append(task)
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        return responses


def single_fetch(url: str, username: str, password: str, max_retries=3, retry_delay=2):
    adapter = HTTPAdapter(max_retries, retry_delay)
    try:
        with requests.Session() as session:
            session.mount(url, adapter)
            response = session.get(url, auth=(username, password), verify=False)
            if response.status_code == 200:
                return response.json()
            else:
                return {}
    except Exception as err:
        log.error(f"Cannot fetch data from {url} : {err}")
        return {}


def run_fetch_all(urls: list, username: str, password: str):
    return asyncio.run(fetch_all(urls, username, password))


def run_single_snmp(irc_ip: str, username: str):
    metrics = asyncio.run(snmp_irc.get_irc_metrics(irc_ip, username))
    if metrics:
        return {
            'irc_ip': irc_ip,
            'metrics': metrics
        }
    return {}


def run_snmp_all(irc_list: list, username: str):
    metrics_all = []
    snmp_args = zip(irc_list, repeat(username))
    with multiprocessing.Pool() as pool:
        metrics_all = pool.starmap(run_single_snmp, snmp_args)
    return metrics_all


def extract_metadata(system_info: dict, bmc_info: dict, node: str):
    bmc_ip_addr    = node
    system_metrics = system_info
    bmc_metrics    = bmc_info

    general   = ["UUID", "SerialNumber", "HostName", "Model", "Manufacturer"]
    processor = ["ProcessorModel", "ProcessorCount", "LogicalProcessorCount"]
    memory    = ["TotalSystemMemoryGiB"]
    bmc       = ["BmcModel", "BmcFirmwareVersion"]
    metrics   = {}

    try:
        # Update service tag
        if system_metrics:
            service_tag = system_metrics.get("SKU", None)
        else:
            service_tag = None

        metrics.update({
            "ServiceTag": service_tag
        })

        # Update System metrics
        if system_metrics:
            for metric in general:
                metrics.update({
                    metric: system_metrics.get(metric, None)
                })
            for metric in processor:
                if metric.startswith("Processor"):
                    metrics.update({
                        metric: system_metrics.get("ProcessorSummary", {}).get(metric[9:], None)
                    })
                else:
                    metrics.update({
                        metric: system_metrics.get("ProcessorSummary", {}).get(metric, None)
                    })
            for metric in memory:
                metrics.update({
                    metric: system_metrics.get("MemorySummary", {}).get("TotalSystemMemoryGiB", None)
                })
        else:
            for metric in general + processor + memory:
                metrics.update({
                    metric: None
                })

        metrics.update({
            "Bmc_Ip_Addr": bmc_ip_addr
        })

        # Update BMC metrics
        if bmc_metrics:
            for metric in bmc:
                metrics.update({
                    metric: bmc_metrics.get(metric[3:], None)
                })
        else:
            for metric in bmc:
                metrics.update({
                    metric: None
                })

        # Update Status
        if (not system_metrics and not bmc_metrics):
            metrics.update({
                "Status": "BMC unreachable"
            })
        else:
            metrics.update({
                "Status": system_metrics.get("Status", {}).get("Health", None)
            })

        # Sanity check for hostname
        # Todo: Add more checks. Some nodes report hostname as "cpu-24-11.localdomain" (for redraider)
        # On repacss, the hostname is set to c+number, e.g. c001, and g+number, e.g. g001
        # This part is currently hardcoded for the repacss cluster
        hostname = metrics.get("HostName", None)
        if hostname is not None and hostname.startswith("c"):
            new_hostname = bmc_ip_addr.replace("10.101.", "rpc-").replace(".", "-")
        elif hostname is not None and hostname.startswith("g"):
            new_hostname = bmc_ip_addr.replace("10.101.", "rpg-").replace(".", "-")
        else:
            # This is only for the h100-build node, as it does not have a valid hostname
            new_hostname = bmc_ip_addr.replace("10.101.", "rpg-").replace(".", "-")
        # print(f"Hostname {hostname} is not valid, set to {new_hostname}")
        metrics.update({
            "HostName": new_hostname
        })

        return metrics
    except Exception as err:
        log.error(f"Cannot extract info from system and bmc: {err}")


def parallel_extract_metadata(system_info_list: list,
                              bmc_info_list: list,
                              nodelist: list):
    info = []
    process_args = zip(system_info_list,
                       bmc_info_list,
                       nodelist)
    with multiprocessing.Pool() as pool:
        info = pool.starmap(extract_metadata, process_args)

    return info


def extract_fqdd_source_pull(redfish_report: list, metrics: list):
    fqdd = []
    source = []
    for i in redfish_report:
        for metric in metrics:
            for item in i.get(metric, []):
                f_value = item.get("Name", "None").replace(" ", "_")
                s_value = item.get("@odata.type", "None")
                if f_value not in fqdd:
                    fqdd.append(f_value)
                if s_value not in source:
                    source.append(s_value)
    return (fqdd, source)


def extract_fqdd_source_push(telemetry_service: dict):
    fqdd = telemetry_service['Oem']['Dell']['FQDDList']
    source = telemetry_service['Oem']['Dell']['SourceList']
    return (fqdd, source)


def process_all_idracs_pull(idrac_api: list, timestamp, idrac_metrics: list,
                           nodelist: list, redfish_report: list,
                           nodeid_map: dict, source_map: dict, fqdd_map: dict):
    processed_records = {}
    # Breakdown the redfish report by API
    idrac_reports = []
    for i in range(len(idrac_api)):
        idrac_reports.append(redfish_report[i * len(nodelist): (i + 1) * len(nodelist)])

    for idrac_metric in idrac_metrics:
        table_name = f"idrac.{idrac_metric.lower()}"
        for reports in idrac_reports:
            records = parallel_process_idrac_pull(timestamp, idrac_metric, nodelist,
                                                 reports, nodeid_map, source_map, fqdd_map)
            if table_name not in processed_records:
                processed_records[table_name] = records
            else:
                processed_records[table_name].extend(records)

    return processed_records


def parallel_process_idrac_pull(timestamp, idrac_metric: str, nodelist: list,
                               reports: list, nodeid_map: dict, source_map: dict,
                               fqdd_map: dict):
    records = []
    process_args = zip(repeat(timestamp), repeat(idrac_metric), nodelist, reports,
                       repeat(nodeid_map), repeat(source_map), repeat(fqdd_map))
    with multiprocessing.Pool() as pool:
        records = pool.starmap(process_node_idrac_pull, process_args)

    # Remove empty lists
    records = [item for sublist in records for item in sublist]
    return records


def process_node_idrac_pull(timestamp, idrac_metric: str, node: str, report: list,
                           nodeid_map: dict, source_map: dict, fqdd_map: dict):
    records = []
    # The first item corresponds to the fqdd field
    # The second item corresponds to the source field
    # The third item corresponds to the value field
    field_map = {'Fans': ['FanName', '@odata.type', 'Reading'],
                 'Temperatures': ['Name', '@odata.type', 'ReadingCelsius'],
                 'PowerControl': ['Name', '@odata.type', 'PowerConsumedWatts']}
    fqdd_field = field_map.get(idrac_metric)[0]
    source_field = field_map.get(idrac_metric)[1]
    value_field = field_map.get(idrac_metric)[2]

    if report:
        try:
            for item in report.get(idrac_metric, []):
                fqdd = item.get(fqdd_field, "None").replace(" ", "_")
                source = item.get(source_field, "None")
                value = int(item.get(value_field, 0))
                records.append((timestamp, nodeid_map[node], source_map[source], fqdd_map[fqdd], value))
        except Exception as err:
            log.error(f"Cannot process idrac metrics: {err}")

    return records


def process_all_pdu_pull(pdu_api: list, timestamp, pdu_list: list, redfish_report: list, nodeid_map: dict):
    processed_records = {}
    # Breakdown the redfish report by API
    pdu_reports = []
    for i in range(len(pdu_api)):
        pdu_reports.append(redfish_report[i * len(pdu_list): (i + 1) * len(pdu_list)])

    table_name = "pdu.pdu"
    for reports in pdu_reports:
        records = parallel_process_pdu_pull(timestamp, pdu_list, reports, nodeid_map)
        if table_name not in processed_records:
            processed_records[table_name] = records
        else:
            processed_records[table_name].extend(records)

    return processed_records


def parallel_process_pdu_pull(timestamp, pdu_list: list, reports: list, nodeid_map: dict):
    records = []
    process_args = zip(repeat(timestamp), pdu_list, reports, repeat(nodeid_map))
    with multiprocessing.Pool() as pool:
        records = pool.starmap(process_node_pdu_pull, process_args)

    # Remove empty lists
    records = [item for sublist in records for item in sublist]
    return records


def process_node_pdu_pull(timestamp, node: str, report: list, nodeid_map: dict):
    records = []
    value_field = 'Reading'
    if report:
        try:
            value = round(report.get(value_field, 0.0), 2)
            records.append((timestamp, nodeid_map[node], value))
        except Exception as err:
            log.error(f"Cannot process PDU metrics: {err}")
    return records


def process_job_metrics_slurm(jobs_metrics: list):
    jobs_info = []
    attributes = sql.job_info_column_names
    for job in jobs_metrics:
        if job['nodes']:
            hostnames = hostlist.expand_hostlist(job['nodes'])
        else:
            hostnames = []
        info = []
        for attribute in attributes:
            if attribute == 'nodes':
                info.append(hostnames)
            else:
                # Try to get the attribute metric
                metric = job.get(attribute, None)
                # If metric is a dictory, get the "number" key
                if isinstance(metric, dict):
                    if 'number' in metric:
                        info.append(metric['number'])
                    elif 'return_code' in metric:
                        info.append(metric['return_code']['number'])
                    else:
                        info.append(None)
                else:
                    info.append(metric)
        jobs_info.append(tuple(info))

    return jobs_info


def process_node_metrics_slurm(nodes_metrics: list,
                               hostname_id_map: dict,
                               timestamp):
    nodes_info = {}
    for node in nodes_metrics:
        node_id = hostname_id_map[node['hostname']]

        state = node['state']
        if state == 'down':
            cpu_load = 0
            memory_used = 0
            f_memory_usage = 0.0
        else:
            cpu_load = int(node['cpu_load'])
            # Some down nodes report cpu_load large than 2147483647, which is
            # not INT4 and cannot saved in TSDB, we set it to -1
            if cpu_load > 2147483647:
                cpu_load = -1

            # Memory usage
            free_memory = node['free_mem']['number']
            real_memory = node['real_memory']
            memory_usage = ((real_memory - free_memory) / real_memory) * 100
            memory_used = real_memory - free_memory
            f_memory_usage = float("{:.2f}".format(memory_usage))

        # Check if reason is already in the previous record.
        node_data = {
            'cpu_load': cpu_load,
            'memoryusage': f_memory_usage,
            'memory_used': memory_used,
            'state': state,
        }
        nodes_info.update({
            node_id: node_data
        })

    # Convert nodes_info to lists of tuples, each tuple is a record to be inserted
    # Each list corresponds to the key in node_data
    cpu_load_record = []
    memoryusage_record = []
    memory_used_record = []
    state_record = []
    for node_id, node_data in nodes_info.items():
        cpu_load_record.append((timestamp, int(node_id), node_data['cpu_load']))
        memoryusage_record.append((timestamp, int(node_id), node_data['memoryusage']))
        memory_used_record.append((timestamp, int(node_id), node_data['memory_used']))
        state_record.append((timestamp, int(node_id), node_data['state']))

    records = {
        'cpu_load': cpu_load_record,
        'memoryusage': memoryusage_record,
        'memory_used': memory_used_record,
        'state': state_record,
    }

    return records


def process_node_job_correlation(jobs_metrics: list,
                                 hostname_id_map: dict,
                                 timestamp):
    nodes_jobs = {}
    for job in jobs_metrics:
        if "RUNNING" in job['job_state']:
            job_id = job['job_id']
            allocated_nodes = hostlist.expand_hostlist(job['job_resources']['nodes']['list'])
            for item in job['job_resources']['nodes']['allocation']:
                # Check if item['nodename'] is in the hostname_id_map
                if item['name'] not in hostname_id_map:
                    continue
                nodeid = hostname_id_map[item['name']]
                cpus   = item['cpus']['count']
                if nodeid not in nodes_jobs:
                    nodes_jobs.update({
                        nodeid: {
                            'jobs': [job_id],
                            'cpus': [cpus]
                        }
                    })
                else:
                    nodes_jobs[nodeid]['jobs'].append(job_id)
                    nodes_jobs[nodeid]['cpus'].append(cpus)

    # Convert nodes_jobs to lists of tuples, each tuple is a record to be inserted
    records = [(timestamp, int(nodeid), nodes_jobs[nodeid]['jobs'], nodes_jobs[nodeid]['cpus']) for nodeid in
               nodes_jobs]
    return records


async def listen_idrac_push(node: str, username: str, password: str, mr_queue: asyncio.Queue):
    url = f"https://{node}/redfish/v1/SSE?$filter=EventFormatType%20eq%20MetricReport"
    while True:
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False,
                                                                            force_close=False,
                                                                            limit=None),
                                             auth=aiohttp.BasicAuth(username, password),
                                             timeout=aiohttp.ClientTimeout(total=0)) as session:
                async with sse_client.EventSource(url,
                                                  session=session,
                                                  read_until_eof=True,
                                                  read_bufsize=1024 * 1024) as event_source:
                    async for event in event_source:
                        report = json.loads(event.data)
                        if report:
                            await mr_queue.put((node, report))
                            await asyncio.sleep(0)
        except Exception as err:
            log.error(f"Cannot listen to {node}: {err}")
            await asyncio.sleep(5)
            continue


async def process_idrac_push(mr_queue: asyncio.Queue, mp_queue: asyncio.Queue, idrac_metrics: list):
    while True:
        data = await mr_queue.get()
        ip = data[0]
        report = data[1]
        report_id = report.get('Id', None)
        metric_values = report.get('MetricValues', [])
        if report_id and metric_values:
            # print(f"Processing report from {ip}")
            processed_metrics = single_process_idrac_push(ip, report_id, metric_values, idrac_metrics)
            if processed_metrics:
                await mp_queue.put((ip, processed_metrics))
            mr_queue.task_done()


def single_process_idrac_push(ip: str, report_id: str, metric_values: list, idrac_metrics: list):
    metrics = {}
    for metric in metric_values:
        table_name = metric.get('MetricId', None)
        timestamp  = metric.get('Timestamp', None)
        source     = metric.get('Oem', {}).get('Dell', {}).get('Source', None)
        fqdd       = metric.get('Oem', {}).get('Dell', {}).get('FQDD', None)
        value      = metric.get('MetricValue', None)
        
        # if idrac_metrics is empty, we assume all metrics are valid
        if not idrac_metrics or table_name in idrac_metrics:
            if timestamp and source and fqdd and value:
                parse_timestamp = parse(timestamp).replace(microsecond=0)
                record = {
                    'timestamp': parse_timestamp,
                    'source': source,
                    'fqdd': fqdd,
                    'value': value
                }
                if table_name not in metrics:
                    metrics[table_name] = [record]
                else:
                    metrics[table_name].append(record)
    return metrics


async def write_idrac_push(conn: object, nodeid_map: dict, source_map: dict, fqdd_map: dict,
                          metric_dtype_mapping: dict, mp_queue: asyncio.Queue):
    cols = ('timestamp', 'nodeid', 'source', 'fqdd', 'value')
    while True:
        data = await mp_queue.get()
        ip = data[0]
        metrics = data[1]
        nodeid = nodeid_map[ip]

        try:
            for table_name, table_metrics in metrics.items():
                all_records = []
                dtype = metric_dtype_mapping[table_name]
                target_table = f"idrac.{table_name.lower()}"
                # print(f"Writing metrics from {ip} to {target_table}")

                for metric in table_metrics:
                    timestamp = metric['timestamp']
                    source = source_map[metric['source']]
                    fqdd = fqdd_map[metric['fqdd']]
                    value = utils.cast_value_type(metric['value'], dtype)
                    all_records.append((timestamp, nodeid, source, fqdd, value))

                mgr = CopyManager(conn, target_table, cols)
                mgr.copy(all_records)
            conn.commit()
        except Exception as err:
            curs = conn.cursor()
            curs.execute("ROLLBACK")
            conn.commit()
            log.error(f"Cannot write metrics from {ip}: {err}")
        mp_queue.task_done()


def process_all_irc_metrics(timestamp, irc_metrics, nodeid_map):
    """
    Process the IRC metrics definition.
    """
    processed_records = {}
    for item in irc_metrics:
        node_id = nodeid_map[item['irc_ip']]
        for metric in item['metrics']:
            table_name = f"irc.{metric['metric_id'].lower()}"
            if table_name not in processed_records:
                processed_records[table_name] = []
            processed_records[table_name].append((timestamp, node_id, metric['value']))

    return processed_records