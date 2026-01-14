from monster import utils, logger

log = logger.get_logger(__name__)


def build_idrac_table_schemas(metric_definitions: list):
    table_schemas = {}
    for metric in metric_definitions:
        table_name = metric['Id']
        metric_type = metric['MetricDataType']
        metric_unit = metric.get('Units', None)

        # For network metrics, use BIG INT for storing the metric readings
        if metric_unit == 'By' or metric_unit == 'Pkt':
            value_type = 'BIGINT'
        else:
            value_type = utils.data_type_mapping.get(metric_type, 'TEXT')

        column_names = ['Timestamp', 'NodeID', 'Source', 'FQDD', 'Value']
        column_types = ['TIMESTAMPTZ NOT NULL', 'INT NOT NULL', 'INT', \
                        'INT', value_type]

        table_schemas.update({
            table_name: {
                'column_names': column_names,
                'column_types': column_types,
            }
        })
    return table_schemas


def build_slurm_table_schemas():
    table_schemas = {}
    add_tables = {
        'memoryusage': {
            'add_columns': ['Value'],
            'add_types': ['REAL']
        },
        'memory_used': {
            'add_columns': ['Value'],
            'add_types': ['INT']
        },
        'cpu_load': {
            'add_columns': ['Value'],
            'add_types': ['INT']
        },
        'state': {
            'add_columns': ['Value'],
            'add_types': ['TEXT[]']
        },
        'node_jobs': {
            'add_columns': ['Jobs', 'CPUs'],
            'add_types': ['INTEGER[]', 'INTEGER[]']
        },
    }
    for table_name, detail in add_tables.items():
        column_names = ['Timestamp', 'NodeID']
        column_types = ['TIMESTAMPTZ NOT NULL', 'INT NOT NULL']
        column_names.extend(detail['add_columns'])
        column_types.extend(detail['add_types'])

        table_schemas.update({
            table_name: {
                'column_names': column_names,
                'column_types': column_types
            }
        })

    return table_schemas

def build_pdu_table_schemas():
    table_schemas = {}
    add_tables = {
        'pdu': {
            'add_columns': ['Value'],
            'add_types': ['REAL']
        },
    }
    for table_name, detail in add_tables.items():
        column_names = ['Timestamp', 'NodeID']
        column_types = ['TIMESTAMPTZ NOT NULL', 'INT NOT NULL']
        column_names.extend(detail['add_columns'])
        column_types.extend(detail['add_types'])

        table_schemas.update({
            table_name: {
                'column_names': column_names,
                'column_types': column_types
            }
        })

    return table_schemas


def build_irc_table_schemas(irc_metrics_definition):
    table_schemas = {}
    add_tables = {}
    for item in irc_metrics_definition:
        table_name = item['metric_id']
        add_tables.update({
            table_name: {
                'add_columns': ['Value'],
                'add_types': [item['metric_data_type']]
            }
        })
    for table_name, detail in add_tables.items():
        column_names = ['Timestamp', 'NodeID']
        column_types = ['TIMESTAMPTZ NOT NULL', 'INT NOT NULL']
        column_names.extend(detail['add_columns'])
        column_types.extend(detail['add_types'])

        table_schemas.update({
            table_name: {
                'column_names': column_names,
                'column_types': column_types
            }
        })

    return table_schemas
