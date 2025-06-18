import pandas as pd
import sqlalchemy as db
import json
import copy

from mbuilder import mb_sql
from monster import utils

GPU_DEVICE_USAGE_PWR_MAPPING = {
    'Video.Slot.31-1': 'GPU-0',
    'Video.Slot.33-1': 'GPU-1',
    'Video.Slot.36-1': 'GPU-2',
    'Video.Slot.38-1': 'GPU-3'
}

GPU_DEVICE_TEMP_MAPPING = {
    'iDRAC.Embedded.1#GPUTemp31': 'GPU-0',
    'iDRAC.Embedded.1#GPUTemp33': 'GPU-1',
    'iDRAC.Embedded.1#GPUTemp36': 'GPU-2',
    'iDRAC.Embedded.1#GPUTemp38': 'GPU-3'
}

CPU_DEVICE_TEMP_MAPPING = {
    'iDRAC.Embedded.1#CPU1Temp': 'CPU-0',
    'iDRAC.Embedded.1#CPU2Temp': 'CPU-1',
}

CPU_DEVICE_PWR_MAPPING = {
    'CPU.Socket.1': 'CPU-0',
    'CPU.Socket.2': 'CPU-1',
}

DRAM_DEVICE_PWR_MAPPING = {
    'CPU.Socket.1': 'DRAM-0',
    'CPU.Socket.2': 'DRAM-1',
}

node_time_format_template = {'time': 0,
                            'node': '',
                            'used_cores': 0,
                            'jobs': [],
                            'cores': [],
                            # '*_diff' is the difference between the current and the previous value
                            # GPU Usage
                            'gpu_usage_labels': [],
                            'gpu_usage': [],
                            'gpu_usage_diff': [],
                            # GPU Power Consumption
                            'gpu_power_consumption_labels': [],
                            'gpu_power_consumption': [],
                            'gpu_power_consumption_diff': [],
                            # GPU Memory Usage
                            'gpu_memory_usage_labels': [],
                            'gpu_memory_usage': [],
                            'gpu_memory_usage_diff': [],
                            # Temperatures
                            'temperature_labels': [],
                            'temperature': [],
                            'temperature_diff': [],
                            # CPU Usage
                            'cpu_usage': 0,
                            'cpu_usage_diff': 0,
                            # CPU Power Consumption
                            'cpu_power_consumption_labels': [],
                            'cpu_power_consumption': [],
                            'cpu_power_consumption_diff': [],
                            # DRAM Usage
                            'dram_usage': 0,
                            'dram_usage_diff': 0,
                            # DRAM Power Consumption
                            'dram_power_consumption_labels': [],
                            'dram_power_consumption': [],
                            'dram_power_consumption_diff': [],
                            # System power consumption
                            'system_power_consumption': 0,
                            'system_power_consumption_diff': 0,
                            }


def get_metrics_map(config):
    metrics_mapping = config['fastapi']
    return metrics_mapping


def get_jobs_cpus(row, item):
    jobs = []
    cpus = []
    jobs_cpus_map = {}

    if row['jobs_copy']:
        for job_list, cpu_list in zip(row['jobs_copy'], row['cpus']):
            for i, job in enumerate(job_list):
                if job not in jobs_cpus_map:
                    jobs_cpus_map[job] = cpu_list[i]

        # Order the dictionary by key
        jobs_cpus_map = dict(sorted(jobs_cpus_map.items()))

        jobs = list(jobs_cpus_map.keys())
        cpus = list(jobs_cpus_map.values())

    if item == 'jobs':
        return jobs
    elif item == 'cpus':
        return cpus
    else:
        return None


def query_db(connection: str, sql: str, nodelist: list):
    record = {}
    engine = db.create_engine(connection)
    dataframe = pd.read_sql_query(sql, engine)

    # If the dataframe is not empty
    if not dataframe.empty:
        # If the dataframe has a node colume, remove rows that are not in the nodelist
        if 'node' in dataframe.columns:
            dataframe = dataframe[dataframe['node'].isin(nodelist)]

        # If the dataframe has a time colume, convert it to epoch time
        if 'time' in dataframe.columns:
            dataframe['time'] = pd.to_datetime(dataframe['time'])
            dataframe['time'] = dataframe['time'].astype(int) // 10 ** 9

        if 'jobs' in dataframe.columns:
            # Copy the jobs column to a new column, this is for the get_jobs_cpus function.
            # The get_jobs_cpus function will modify the jobs column, so we need to keep a
            # copy of the original jobs column to avoid the error when applying the
            # get_jobs_cpus function on the cpus column.
            dataframe['jobs_copy'] = dataframe['jobs']
            dataframe['jobs'] = dataframe.apply(lambda x: get_jobs_cpus(x, 'jobs'), axis=1)
            dataframe['cpus'] = dataframe.apply(lambda x: get_jobs_cpus(x, 'cpus'), axis=1)
            # Drop the jobs_copy column
            dataframe = dataframe.drop(columns=['jobs_copy'])

        # Fill all NaN with 0
        with pd.option_context("future.no_silent_downcasting", True):
            dataframe = dataframe.fillna(0).infer_objects(copy=False)

        # Convert the dataframe to a dictionary
        record = dataframe.to_dict(orient='records')
    return record


def query_db_wrapper(connection: str, start: str, end: str, interval: str,
                     aggregation: str, nodelist: list, table: str):
    metric = []
    if table == 'slurm.jobs':
        sql = mb_sql.generate_slurm_jobs_sql(start, end)
        metric = query_db(connection, sql, nodelist)
    elif table == 'slurm.node_jobs':
        sql = mb_sql.generate_slurm_node_jobs_sql(start, end, interval)
        metric = query_db(connection, sql, nodelist)
    elif table == 'slurm.state':
        sql = mb_sql.generate_slurm_state_sql(start, end, interval)
        metric = query_db(connection, sql, nodelist)
    elif 'slurm' in table:
        slurm_metric = table.split('.')[1]
        sql = mb_sql.generate_slurm_metric_sql(slurm_metric, start, end,
                                               interval, aggregation)
        metric = query_db(connection, sql, nodelist)
    elif 'idrac' in table:
        idrac_metric = table.split('.')[1]
        sql = mb_sql.generate_idrac_metric_sql(idrac_metric, start, end,
                                               interval, aggregation)
        metric = query_db(connection, sql, nodelist)
    else:
        pass
    return metric


def rename_device(metrics_mapping, results):
    """
    Rename device names in the results dictionary.
    """
    gpu_rename = ['idrac.gpuusage', 'idrac.powerconsumption', 'idrac.gpumemoryusage']
    for g in gpu_rename:
        if g in results:
            for item in results[g]:
                if item['label'] in GPU_DEVICE_USAGE_PWR_MAPPING:
                    item['label'] = GPU_DEVICE_USAGE_PWR_MAPPING[item['label']]

    if 'idrac.temperaturereading' in results:
        updated_tmp = []
        for item in results['idrac.temperaturereading']:
            if item['label'] in GPU_DEVICE_TEMP_MAPPING:
                item['label'] = GPU_DEVICE_TEMP_MAPPING[item['label']]
                updated_tmp.append(item)
            elif item['label'] in CPU_DEVICE_TEMP_MAPPING:
                item['label'] = CPU_DEVICE_TEMP_MAPPING[item['label']]
                updated_tmp.append(item)
            else:
                # Discard the item if it does not match any of the GPU or CPU device mappings
                continue
        results['idrac.temperaturereading'] = updated_tmp
    if 'idrac.cpuusage' in results:
        for item in results['idrac.cpuusage']:
            item['label'] = 'CPU'
    if 'idrac.cpupower' in results:
        for item in results['idrac.cpupower']:
            if item['label'] in CPU_DEVICE_PWR_MAPPING:
                item['label'] = CPU_DEVICE_PWR_MAPPING[item['label']]
    if 'idrac.memoryusage' in results:
        memoryusage = results['idrac.memoryusage']
        for item in memoryusage:
            item['label'] = 'DRAM'
    if 'idrac.drampwr' in results:
        drampwr = results['idrac.drampwr']
        for item in drampwr:
            if item['label'] in DRAM_DEVICE_PWR_MAPPING:
                item['label'] = DRAM_DEVICE_PWR_MAPPING[item['label']]
    if 'idrac.systempowerconsumption' in results:
        systempowerconsumption = results['idrac.systempowerconsumption']
        for item in systempowerconsumption:
            item['label'] = 'System'
    return results

def reformat_results(partition, results):
    reformated_results = {}
    summary = {}
    job_nodes_cpus = {}
    node_time_records = {}
    job_time_records = {}
    node_system_power_track = {}
    gpu_usage_track = {}
    gpu_power_consumption_track = {}
    gpu_memory_usage_track = {}
    node_temperatures_track = {} 
    cpu_usage_track = {}
    cpu_power_consumption_track = {}
    dram_usage_track = {}
    dram_power_consumption_track = {}
    # node_memory_used_track = {}
    all_system_power_track = {}
    all_memory_used_track = {}

    slurm_jobs = results.get('slurm.jobs', {})
    if slurm_jobs:
        # Get the nodes, CPUs, memory_per_cpu
        for item in slurm_jobs:
            job_nodes_cpus.update({item['job_id']: {'nodes': item['nodes'],
                                                    'used_cores': int(item['cpus']),
                                                    'memory_per_core': item['memory_per_cpu'],
                                                    'cores_per_node': round(item['cpus'] / item['node_count'])}})

    # Precess the system power consumption
    system_power = results.get(f"idrac.systempowerconsumption", {})
    if system_power:
        for item in system_power:
            idx = f"{item['node']}_{item['time']}"
            node_time_records[idx] = copy.deepcopy(node_time_format_template)
            node_time_records[idx].update({'time': int(item['time']),
                                           'node': item['node'],
                                           'system_power_consumption': item['value'],
                                           'system_power_consumption_diff': 0})
            if item['node'] not in node_system_power_track:
                node_system_power_track[item['node']] = {'power': [item['value']],
                                                         'time': [int(item['time'])]}
            else:
                node_system_power_track[item['node']]['power'].append(item['value'])
                node_system_power_track[item['node']]['time'].append(int(item['time']))

            if item['time'] not in all_system_power_track:
                all_system_power_track[item['time']] = [item['value']]
            else:
                all_system_power_track[item['time']].append(item['value'])

    for node, records in node_system_power_track.items():
        power = records['power']
        diff = [0]
        diff.extend([power[i] - power[i - 1] for i in range(1, len(power))])
        records['diff'] = diff
        for i, time in enumerate(records['time']):
            node_time_records[f'{node}_{time}']['system_power_consumption_diff'] = diff[i]

    # Process the GPU-related metrics if they exist
    if partition == 'h100':
        gpu_usage = results.get(f"idrac.gpuusage", {})
        if gpu_usage:
            for item in gpu_usage:
                label = item['label']
                idx = f"{item['node']}_{item['time']}"
                if idx in node_time_records:
                    node_time_records[idx]['gpu_usage'].append(item['value'])
                    node_time_records[idx]['gpu_usage_labels'].append(label)
                    node_time_records[idx]['gpu_usage_diff'].append(0)
                else:
                    node_time_records[idx] = copy.deepcopy(node_time_format_template)
                    node_time_records[idx].update({'time': int(item['time']),
                                                   'node': item['node'],
                                                   'gpu_usage_labels': [label],
                                                   'gpu_usage': [item['value']],
                                                   'gpu_usage_diff': [0]})

                # Track GPU usage for each node
                if item['node'] not in gpu_usage_track:
                    gpu_usage_track[item['node']] = {label: {'usage': [item['value']],
                                                             'time': [int(item['time'])],}}
                else:
                    if label not in gpu_usage_track[item['node']]:
                        gpu_usage_track[item['node']][label] = {'usage': [item['value']],
                                                                'time': [int(item['time'])]}
                    else:
                        gpu_usage_track[item['node']][label]['usage'].append(item['value'])
                        gpu_usage_track[item['node']][label]['time'].append(int(item['time']))

            # Calculate the difference for GPU usage
            for node, records in gpu_usage_track.items():
                for label, readings in records.items():
                    usage = readings['usage']
                    diff = [0]
                    diff.extend([usage[i] - usage[i - 1] for i in range(1, len(usage))])
                    readings['diff'] = diff
                    for i, time in enumerate(records[label]['time']):
                        idx = node_time_records[f'{node}_{time}']['gpu_usage_labels'].index(label)
                        node_time_records[f'{node}_{time}']['gpu_usage_diff'][idx] = diff[i]
            
        
        gpu_power_consumption = results.get(f"idrac.powerconsumption", {})
        if gpu_power_consumption:
            for item in gpu_power_consumption:
                label = item['label']
                idx = f"{item['node']}_{item['time']}"
                if idx in node_time_records:
                    node_time_records[idx]['gpu_power_consumption'].append(item['value'])
                    node_time_records[idx]['gpu_power_consumption_labels'].append(label)
                    node_time_records[idx]['gpu_power_consumption_diff'].append(0)
                else:
                    node_time_records[idx] = copy.deepcopy(node_time_format_template)
                    node_time_records[idx].update({'time': int(item['time']),
                                                   'node': item['node'],
                                                   'gpu_power_consumption_labels': [label],
                                                   'gpu_power_consumption': [item['value']],
                                                   'gpu_power_consumption_diff': [0]})
                # Track GPU power consumption for each node
                if item['node'] not in gpu_power_consumption_track:
                    gpu_power_consumption_track[item['node']] = {label: {'power': [item['value']],
                                                                         'time': [int(item['time'])],}}
                else:
                    if label not in gpu_power_consumption_track[item['node']]:
                        gpu_power_consumption_track[item['node']][label] = {'power': [item['value']],
                                                                            'time': [int(item['time'])]}
                    else:
                        gpu_power_consumption_track[item['node']][label]['power'].append(item['value'])
                        gpu_power_consumption_track[item['node']][label]['time'].append(int(item['time']))
            # Calculate the difference for GPU power consumption
            for node, records in gpu_power_consumption_track.items():
                for label, readings in records.items():
                    power = readings['power']
                    diff = [0]
                    diff.extend([power[i] - power[i - 1] for i in range(1, len(power))])
                    readings['diff'] = diff
                    for i, time in enumerate(records[label]['time']):
                        idx = node_time_records[f'{node}_{time}']['gpu_power_consumption_labels'].index(label)
                        node_time_records[f'{node}_{time}']['gpu_power_consumption_diff'][idx] = diff[i]

        gpu_memory_usage = results.get(f"idrac.gpumemoryusage", {})
        if gpu_memory_usage:
            for item in gpu_memory_usage:
                label = item['label']
                idx = f"{item['node']}_{item['time']}"
                if idx in node_time_records:
                    node_time_records[idx]['gpu_memory_usage'].append(item['value'])
                    node_time_records[idx]['gpu_memory_usage_labels'].append(label)
                    node_time_records[idx]['gpu_memory_usage_diff'].append(0)
                else:
                    node_time_records[idx] = copy.deepcopy(node_time_format_template)
                    node_time_records[idx].update({'time': int(item['time']),
                                                   'node': item['node'],
                                                   'gpu_memory_usage_labels': [label],
                                                   'gpu_memory_usage': [item['value']],
                                                   'gpu_memory_usage_diff': [0]})
                # Track GPU memory usage for each node
                if item['node'] not in gpu_memory_usage_track:
                    gpu_memory_usage_track[item['node']] = {label: {'memory': [item['value']],
                                                                    'time': [int(item['time'])],}}
                else:
                    if label not in gpu_memory_usage_track[item['node']]:
                        gpu_memory_usage_track[item['node']][label] = {'memory': [item['value']],
                                                                       'time': [int(item['time'])]}
                    else:
                        gpu_memory_usage_track[item['node']][label]['memory'].append(item['value'])
                        gpu_memory_usage_track[item['node']][label]['time'].append(int(item['time']))
            # Calculate the difference for GPU memory usage
            for node, records in gpu_memory_usage_track.items():
                for label, readings in records.items():
                    memory = readings['memory']
                    diff = [0]
                    diff.extend([memory[i] - memory[i - 1] for i in range(1, len(memory))])
                    readings['diff'] = diff
                    for i, time in enumerate(records[label]['time']):
                        idx = node_time_records[f'{node}_{time}']['gpu_memory_usage_labels'].index(label)
                        node_time_records[f'{node}_{time}']['gpu_memory_usage_diff'][idx] = diff[i]

    temperatures = results.get('idrac.temperaturereading', {})
    if temperatures:
        for item in temperatures:
            label = item['label']
            idx = f"{item['node']}_{item['time']}"
            if idx in node_time_records:
                node_time_records[idx]['temperature'].append(item['value'])
                node_time_records[idx]['temperature_labels'].append(label)
                node_time_records[idx]['temperature_diff'].append(0)
            else:
                node_time_records[idx] = copy.deepcopy(node_time_format_template)
                node_time_records[idx].update({'time': int(item['time']),
                                                                            'node': item['node'],
                                                                            'temperature_labels': [label],
                                                                            'temperature': [item['value']],
                                                                            'temperature_diff': [0]})
            # Track temperatures for each node
            if item['node'] not in node_temperatures_track:
                node_temperatures_track[item['node']] = {label: {'temperatures': [item['value']],
                                                                 'time': [int(item['time'])],}}
            else:
                if label not in node_temperatures_track[item['node']]:
                    node_temperatures_track[item['node']][label] = {'temperatures': [item['value']],
                                                                   'time': [int(item['time'])]}
                else:
                    node_temperatures_track[item['node']][label]['temperatures'].append(item['value'])
                    node_temperatures_track[item['node']][label]['time'].append(int(item['time']))
        # Calculate the difference for temperatures
        for node, records in node_temperatures_track.items():
            for label, readings in records.items():
                temperatures = readings['temperatures']
                diff = [0]
                diff.extend([temperatures[i] - temperatures[i - 1] for i in range(1, len(temperatures))])
                readings['diff'] = diff
                for i, time in enumerate(records[label]['time']):
                    idx = node_time_records[f'{node}_{time}']['temperature_labels'].index(label)
                    node_time_records[f'{node}_{time}']['temperature_diff'][idx] = diff[i]

    cpu_usage = results.get('idrac.cpuusage', {})
    if cpu_usage:
        for item in cpu_usage:
            idx = f"{item['node']}_{item['time']}"
            if idx in node_time_records:
                node_time_records[idx].update({'cpu_usage': item['value'],
                                               'cpu_usage_diff': 0})
            else:
                node_time_records[idx] = copy.deepcopy(node_time_format_template)
                node_time_records[idx].update({'time': int(item['time']),
                                               'node': item['node'],
                                               'cpu_usage': item['value'],
                                               'cpu_usage_diff': 0})
            # Track CPU usage for each node
            if item['node'] not in cpu_usage_track:
                cpu_usage_track[item['node']] = {'usage': [item['value']],
                                                 'time': [int(item['time'])]}
            else:
                cpu_usage_track[item['node']]['usage'].append(item['value'])
                cpu_usage_track[item['node']]['time'].append(int(item['time']))
        # Calculate the difference for CPU usage
        for node, records in cpu_usage_track.items():
            usage = records['usage']
            diff = [0]
            diff.extend([usage[i] - usage[i - 1] for i in range(1, len(usage))])
            records['diff'] = diff
            for i, time in enumerate(records['time']):
                node_time_records[f'{node}_{time}']['cpu_usage_diff'] = diff[i]

    cpu_power_consumption = results.get('idrac.cpupower', {})
    if cpu_power_consumption:
        for item in cpu_power_consumption:
            label = item['label']
            idx = f"{item['node']}_{item['time']}"
            if idx in node_time_records:
                node_time_records[idx]['cpu_power_consumption'].append(item['value'])
                node_time_records[idx]['cpu_power_consumption_labels'].append(label)
                node_time_records[idx]['cpu_power_consumption_diff'].append(0)
            else:
                node_time_records[idx] = copy.deepcopy(node_time_format_template)
                node_time_records[idx].update({'time': int(item['time']),
                                               'node': item['node'],
                                               'cpu_power_consumption_labels': [label],
                                               'cpu_power_consumption': [item['value']],
                                               'cpu_power_consumption_diff': [0]})
            # Track CPU power consumption for each node
            if item['node'] not in cpu_power_consumption_track:
                cpu_power_consumption_track[item['node']] = {label: {'power': [item['value']],
                                                                     'time': [int(item['time'])],}}
            else:
                if label not in cpu_power_consumption_track[item['node']]:
                    cpu_power_consumption_track[item['node']][label] = {'power': [item['value']],
                                                                        'time': [int(item['time'])]}
                else:
                    cpu_power_consumption_track[item['node']][label]['power'].append(item['value'])
                    cpu_power_consumption_track[item['node']][label]['time'].append(int(item['time']))
        # Calculate the difference for CPU power consumption
        for node, records in cpu_power_consumption_track.items():
            for label, readings in records.items():
                power = readings['power']
                diff = [0]
                diff.extend([power[i] - power[i - 1] for i in range(1, len(power))])
                readings['diff'] = diff
                for i, time in enumerate(records[label]['time']):
                    idx = node_time_records[f'{node}_{time}']['cpu_power_consumption_labels'].index(label)
                    node_time_records[f'{node}_{time}']['cpu_power_consumption_diff'][idx] = diff[i]

    dram_usage = results.get('idrac.memoryusage', {})
    if dram_usage:
        for item in dram_usage:
            idx = f"{item['node']}_{item['time']}"
            if idx in node_time_records:
                node_time_records[idx].update({'dram_usage': item['value'],
                                               'dram_usage_diff': 0})
            else:
                node_time_records[idx] = copy.deepcopy(node_time_format_template)
                node_time_records[idx].update({'time': int(item['time']),
                                               'node': item['node'],
                                               'dram_usage': item['value'],
                                               'dram_usage_diff': 0})
            # Track DRAM usage for each node
            if item['node'] not in dram_usage_track:
                dram_usage_track[item['node']] = {'usage': [item['value']],
                                                  'time': [int(item['time'])]}
            else:
                dram_usage_track[item['node']]['usage'].append(item['value'])
                dram_usage_track[item['node']]['time'].append(int(item['time']))
        # Calculate the difference for DRAM usage
        for node, records in dram_usage_track.items():
            usage = records['usage']
            diff = [0]
            diff.extend([usage[i] - usage[i - 1] for i in range(1, len(usage))])
            records['diff'] = diff
            for i, time in enumerate(records['time']):
                node_time_records[f'{node}_{time}']['dram_usage_diff'] = diff[i]
    
    dram_power_consumption = results.get('idrac.drampwr', {})
    if dram_power_consumption:
        for item in dram_power_consumption:
            label = item['label']
            idx = f"{item['node']}_{item['time']}"
            if idx in node_time_records:
                node_time_records[idx]['dram_power_consumption'].append(item['value'])
                node_time_records[idx]['dram_power_consumption_labels'].append(label)
                node_time_records[idx]['dram_power_consumption_diff'].append(0)
            else:
                node_time_records[idx] = copy.deepcopy(node_time_format_template)
                node_time_records[idx].update({'time': int(item['time']),
                                               'node': item['node'],
                                               'dram_power_consumption_labels': [label],
                                               'dram_power_consumption': [item['value']],
                                               'dram_power_consumption_diff': [0]})
            # Track DRAM power consumption for each node
            if item['node'] not in dram_power_consumption_track:
                dram_power_consumption_track[item['node']] = {label: {'power': [item['value']],
                                                                      'time': [int(item['time'])],}}
            else:
                if label not in dram_power_consumption_track[item['node']]:
                    dram_power_consumption_track[item['node']][label] = {'power': [item['value']],
                                                                        'time': [int(item['time'])]}
                else:
                    dram_power_consumption_track[item['node']][label]['power'].append(item['value'])
                    dram_power_consumption_track[item['node']][label]['time'].append(int(item['time']))
        # Calculate the difference for DRAM power consumption
        for node, records in dram_power_consumption_track.items():
            for label, readings in records.items():
                power = readings['power']
                diff = [0]
                diff.extend([power[i] - power[i - 1] for i in range(1, len(power))])
                readings['diff'] = diff
                for i, time in enumerate(records[label]['time']):
                    idx = node_time_records[f'{node}_{time}']['dram_power_consumption_labels'].index(label)
                    node_time_records[f'{node}_{time}']['dram_power_consumption_diff'][idx] = diff[i]

    node_jobs = results.get('slurm.node_jobs', {})
    if node_jobs:
        for item in node_jobs:
            idx = f"{item['node']}_{item['time']}"
            if idx in node_time_records:
                node_time_records[idx].update({'jobs': item['jobs'],
                                               'cores': item['cpus'],
                                               'used_cores': sum(item['cpus'])})
            else:
                # If the node_time_records does not have the key, create a new record
                node_time_records[idx] = copy.deepcopy(node_time_format_template)
                node_time_records[idx].update({'time': int(item['time']),
                                               'node': item['node'],
                                               'jobs': item['jobs'],
                                               'cores': item['cpus'],
                                               'used_cores': sum(item['cpus'])})
        
    # # Calculate the summary
    # for time, records in all_system_power_track.items():
    #     summary[time] = {'time': time,
    #                      'average_system_power': round(sum(records) / len(records), 2),
    #                      'total_system_power': round(sum(records), 2),
    #                      'average_memory_used': 0,
    #                      'total_memory_used': 0}

    # for time, records in all_memory_used_track.items():
    #     if time in summary:
    #         summary[time].update({'average_memory_used': round(sum(records) / len(records), 2),
    #                               'total_memory_used': round(sum(records), 2)})
    #     else:
    #         summary[time] = {'time': time,
    #                          'average_system_power': 0,
    #                          'total_system_power': 0,
    #                          'average_memory_used': round(sum(records) / len(records), 2),
    #                          'total_memory_used': round(sum(records), 2)}

    # Calculate the power consumption for each job
    for key, value in node_time_records.items():
        this_node = key.split('_')[0]
        timestamp = key.split('_')[1]
        this_node_power = value['system_power_consumption']
        this_node_cores = value['used_cores']
        for i, job in enumerate(value['jobs']):
            if this_node_cores != 0:
                this_node_power_part = round(this_node_power * (value['cores'][i] / this_node_cores), 2)
            else:
                this_node_power_part = 0
            if f'{job}_{timestamp}' not in job_time_records:
                if this_node_cores != 0:
                    power_per_core = round(this_node_power_part / this_node_cores, 2)
                else:
                    power_per_core = 0
                if job not in job_nodes_cpus:
                    memory_per_core = 0
                    memory_used = 0
                else:
                    memory_per_core = job_nodes_cpus[job].get('memory_per_core', 0)
                    memory_used = job_nodes_cpus[job].get('memory_per_core', 0) * job_nodes_cpus[job].get('used_cores',
                                                                                                          0)
                job_time_records[f'{job}_{timestamp}'] = {
                    'time': int(timestamp),
                    'job_id': job,
                    'data': [{
                        'node': this_node,
                        'power': this_node_power_part,
                        'cores': value['cores'][i],
                    }],
                    'power': this_node_power_part,
                    'cores': value['cores'][i],
                    'power_per_core': power_per_core,
                    'memory_per_core': memory_per_core,
                    'memory_used': memory_used
                }
            else:
                job_time_records[f'{job}_{timestamp}']['data'].append({
                    'node': this_node,
                    'power': this_node_power_part,
                    'cores': value['cores'][i],
                })
                job_time_records[f'{job}_{timestamp}']['power'] += this_node_power_part
                job_time_records[f'{job}_{timestamp}']['cores'] += value['cores'][i]
                if job_time_records[f'{job}_{timestamp}']['cores'] != 0:
                    job_time_records[f'{job}_{timestamp}']['power_per_core'] = round(
                        job_time_records[f'{job}_{timestamp}']['power'] / job_time_records[f'{job}_{timestamp}'][
                            'cores'], 2)
                else:
                    job_time_records[f'{job}_{timestamp}']['power_per_core'] = 0

    reformated_results['job_details'] = results.get('slurm.jobs', [])

    reformated_results['nodes'] = list(node_time_records.values())
    reformated_results['jobs'] = list(job_time_records.values())
    # reformated_results['summary'] = list(summary.values())
    return reformated_results
