import asyncio
import psycopg2
import random

from monster import logger, process

log = logger.get_logger(__name__)


def get_nodes_metadata(nodelist: list, valid_nodelist: list, username: str, password: str):
    system_base_url = "/redfish/v1/Systems/System.Embedded.1"
    bmc_base_url    = "/redfish/v1/Managers/iDRAC.Embedded.1"

    system_urls = ["https://" + node + system_base_url for node in nodelist]
    bmc_urls    = ["https://" + node + bmc_base_url for node in nodelist]

    # Fetch system info
    system_info = process.run_fetch_all(system_urls,
                                        username,
                                        password)
    # Fetch bmc info
    bmc_info = process.run_fetch_all(bmc_urls,
                                     username,
                                     password)

    # Process system and bmc info
    metadata = process.parallel_extract_metadata(system_info, bmc_info, nodelist)
    for m in metadata:
        if m['Status'] != 'BMC unreachable':
            valid_nodelist.append(m['Bmc_Ip_Addr'])
    return metadata


def get_fqdd_source_pull(nodelist: list, api: list, metrics: list,
                        username: str, password: str):
    for node in nodelist:
        urls = [f"https://{node}{url}" for url in api]
        redfish_report = process.run_fetch_all(urls, username, password)
        if redfish_report:
            return process.extract_fqdd_source_pull(redfish_report, metrics)


def get_fqdd_source_push(nodelist: list, username: str, password: str):
    for node in nodelist:
        url = f"https://{node}/redfish/v1/TelemetryService"
        telemetry_service = process.single_fetch(url, username, password)
        if telemetry_service:
            return process.extract_fqdd_source_push(telemetry_service)


def get_metric_definitions_pull(idrac_metrics: list):
    metric_definitions = []
    unit_map = {'Fans': 'RPM', 'Temperatures': 'Cel', 'PowerControl': 'Watts'}
    for metric in idrac_metrics:
        metric_definitions.append({'Id': metric,
                                   'MetricDataType': 'Integer',
                                   'Units': unit_map.get(metric, None)})
    return metric_definitions


def get_metric_definitions_push(nodelist: list, idrac_metrics: list, username: str, password: str):
    metric_definitions = []
    # Random select one from the nodelist
    node = random.choice(nodelist)
    metric_definition_urls = get_metric_definition_urls_push(node, idrac_metrics, username, password)
    available_fields = ['Id', 'Name', 'Description', 'MetricType', \
                        'MetricDataType', 'Units', 'Accuracy', \
                        'SensingInterval', 'DiscreteValues']
    # Random select nodes from the nodelist; this is used to get the metric definitions in parallel
    # If the length of metric_definition_urls is greater than the length of nodelist, each node will 
    # handle multiple metric definitions. Otherwise, randomly select nodes to handle each metric definition.
    if len(metric_definition_urls) > len(nodelist):
        nodes = random.choices(nodelist, k=len(metric_definition_urls))
    else:
        nodes = random.sample(nodelist, len(metric_definition_urls))
    
    urls  = [f"https://{nodes[i]}{url}" for i, url in enumerate(metric_definition_urls)]
    responses = process.run_fetch_all(urls, username, password)
    if responses:
        for response in responses:
            metric_definition = {}
            for field in available_fields:
                field_value = response.get(field, None)
                metric_definition[field] = field_value
            metric_definitions.append(metric_definition)
        return metric_definitions


def get_metric_definition_urls_push(node: str, idrac_metrics: list, username: str, password: str):
    selected_metrics_urls = []
    url = f'https://{node}/redfish/v1/TelemetryService/MetricDefinitions'
    response = process.single_fetch(url, username, password)
    if response:
        members               = response.get('Members', [])
        all_metrics_urls      = [m.get('@odata.id', None) for m in members]
        return all_metrics_urls
        selected_metrics_urls = [m for m in all_metrics_urls if m.split('/')[-1] in idrac_metrics]
    return selected_metrics_urls


def get_idrac_metrics_pull(api: list, timestamp, idrac_metrics: list,
                          nodelist: list, username: str, password: str,
                          nodeid_map: dict, source_map: dict, fqdd_map: dict):
    urls = [f"https://{node}{url}" for url in api for node in nodelist]
    redfish_report = process.run_fetch_all(urls, username, password)
    if redfish_report:
        processed_records = process.process_all_idracs_pull(api, timestamp, idrac_metrics,
                                                           nodelist, redfish_report,
                                                           nodeid_map, source_map, fqdd_map)
        return processed_records


def get_idrac_metrics_push(nodelist: list, idrac_metrics: list, username: str, password: str,
                          connection: str, nodeid_map: dict, source_map: dict,
                          fqdd_map: dict, metric_dtype_mapping: dict):
    with psycopg2.connect(connection) as conn:
        while True:
            asyncio.run(listen_process_write_idrac_push(nodelist, idrac_metrics, username, password,
                                                       conn, nodeid_map, source_map,
                                                       fqdd_map, metric_dtype_mapping))


async def listen_process_write_idrac_push(nodelist: list, idrac_metrics: list, username: str, password: str,
                                         conn: object, nodeid_map: dict, source_map: dict,
                                         fqdd_map: dict, metric_dtype_mapping: dict):
    buf_size = 1024 * 1024 * 10
    # Metrics read queue
    mr_queue = asyncio.Queue(maxsize=buf_size)
    # Metrics process queue
    mp_queue = asyncio.Queue(maxsize=buf_size)

    listen_task = [asyncio.create_task(process.listen_idrac_push(node, username, password, mr_queue)) for node in
                   nodelist]
    process_task = [asyncio.create_task(process.process_idrac_push(mr_queue, mp_queue, idrac_metrics))]
    write_task = [asyncio.create_task(
        process.write_idrac_push(conn, nodeid_map, source_map, fqdd_map, metric_dtype_mapping, mp_queue))]

    tasks = listen_task + process_task + write_task
    await asyncio.gather(*tasks)
