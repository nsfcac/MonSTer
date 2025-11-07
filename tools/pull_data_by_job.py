import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Sequence, List, Iterable, Tuple, DefaultDict

import hostlist
import yaml
import zoneinfo
from dateutil import parser as date_parser
from sqlalchemy import create_engine

from monster import slurm
from monster import utils
from mbuilder import mb_utils

MAX_DB_WORKERS = 4

# DEFAULT_CONFIG = Path(__file__).resolve().parents[1] / "config-zen4.yml"
# DEFAULT_METRICS = ["CPU_PowerConsumption", "System_PowerConsumption"]

DEFAULT_CONFIG = Path(__file__).resolve().parents[1] / "config-h100.yml"
DEFAULT_METRICS = ["CPU_Usage", "CPU_PowerConsumption", "CPU_Temperature", "DRAM_PowerConsumption"]

LOCAL_TIMEZONE = zoneinfo.ZoneInfo("America/Chicago")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query detailed information for a Slurm job.")
    parser.add_argument("job_id", help="Slurm job identifier to look up.")
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help=f"Path to the MonSter configuration file (default: {DEFAULT_CONFIG}).",
    )
    parser.add_argument(
        "--metrics",
        nargs="+",
        default=DEFAULT_METRICS,
        help="Space-separated list of iDRAC metrics to fetch.",
    )
    return parser.parse_args()


def load_config(config_path: Path) -> Dict[str, Any]:
    if not config_path.is_file():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as ymlfile:
        return yaml.safe_load(ymlfile)


def build_job_detail(job_id: str, slurm_config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    api_url = f"http://{slurm_config['ip']}:{slurm_config['port']}{slurm_config['slurm_db_job']}{job_id}"
    response = slurm.call_slurm_api(slurm_config, slurm.read_slurm_token(slurm_config), api_url).get("jobs", {})
    if not response:
        return None
    return response[0]


def _normalize_timestamp(raw_timestamp: Any) -> Optional[str]:
    """
    Convert a raw timestamp (epoch seconds or ISO string) into the expected
    database query format "YYYY-MM-DD HH:MM:SS±HHMM" in the local timezone.
    """
    if raw_timestamp in (None, "", 0, "0"):
        return None

    if isinstance(raw_timestamp, (int, float)):
        dt = datetime.fromtimestamp(int(raw_timestamp), tz=LOCAL_TIMEZONE)
        return dt.strftime("%Y-%m-%d %H:%M:%S%z")

    raw_str = str(raw_timestamp).strip()
    if not raw_str:
        return None

    try:
        epoch = int(float(raw_str))
    except (TypeError, ValueError):
        try:
            dt = date_parser.parse(raw_str)
        except (TypeError, ValueError):
            return None
    else:
        dt = datetime.fromtimestamp(epoch, tz=LOCAL_TIMEZONE)
        return dt.strftime("%Y-%m-%d %H:%M:%S%z")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=LOCAL_TIMEZONE)
    else:
        dt = dt.astimezone(LOCAL_TIMEZONE)
    return dt.strftime("%Y-%m-%d %H:%M:%S%z")


def _to_local_time_string(time_value: Any) -> Optional[str]:
    if not time_value:
        return None
    try:
        dt = date_parser.parse(str(time_value))
    except (TypeError, ValueError):
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=zoneinfo.ZoneInfo("UTC"))
    dt_local = dt.astimezone(LOCAL_TIMEZONE)
    return dt_local.strftime("%Y-%m-%d %H:%M:%S%z")


def _convert_record_times_to_local(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    converted: List[Dict[str, Any]] = []
    for record in records:
        time_value = _to_local_time_string(record.get("time"))
        if time_value:
            updated = dict(record)
            updated["time"] = time_value
            converted.append(updated)
        else:
            converted.append(record)
    return converted


def pull_metric_data(config: Dict[str, Any],
                     start: Any,
                     end: Any,
                     nodes: str,
                     metrics: Sequence[str]) -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    tables: List[str] = []

    start_ts = _normalize_timestamp(start)
    end_ts = _normalize_timestamp(end)
    if not start_ts or not end_ts:
        return {"ERROR": "Start and end timestamps are required to query metrics."}

    try:
        nodelist = hostlist.expand_hostlist(nodes)
    except ValueError:
        return {"ERROR": f"Unable to parse node list '{nodes}'."}

    if not nodelist:
        return {"ERROR": "Node list is empty."}

    connection = utils.init_tsdb_connection(config)
    metrics_mapping = mb_utils.get_metrics_map(config)

    # Parse the iDRAC metrics requested
    if metrics:
        for metric in metrics:
            table_name = metrics_mapping.get('idrac', {}).get(metric)
            if table_name:
                full_table = f'idrac.{table_name}'
                if full_table not in tables:
                    tables.append(full_table)
            else:
                return {"ERROR": f"Metric '{metric}' is not supported or not found in the configuration."}
    else:
        return {"ERROR": "At least one metric must be provided."}

    if not tables:
        return {}

    engine = create_engine(connection, pool_pre_ping=True, pool_size=MAX_DB_WORKERS, max_overflow=0)
    try:
        for table in tables:
            table_records = []
            for node in nodelist:
                record = mb_utils.query_db_raw_wrapper(engine, start_ts, end_ts, node, table)
                if record:
                    table_records.extend(_convert_record_times_to_local(record))
            if table_records:
                results[table] = table_records
    finally:
        engine.dispose()

    if not results:
        return {}

    # renamed_results = mb_utils.rename_device(metrics_mapping, results)
    return results


def reshape_metrics_for_plot(
    metrics_data: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    """
    Convert raw metric records into a structure that is convenient for plotting.

    Example output:
    [
        {
            "metric": "idrac.cpupower",
            "node": "rpc-96-18",
            "series": [
                {
                    "label": "CPU.Socket.1",
                    "points": [
                        {"time": "2025-10-29 15:15:30+00:00", "value": 108.0},
                        ...
                    ],
                },
                ...
            ],
        },
        ...
    ]
    """
    grouped: DefaultDict[Tuple[str, str, str], List[Tuple[str, Any]]] = DefaultDict(list)

    for metric_name, records in metrics_data.items():
        for record in records:
            time_value = record.get("time")
            if time_value is None:
                continue
            node_name = record.get("node", "unknown")
            label = record.get("label", metric_name)
            value = record.get("value")
            grouped[(metric_name, node_name, label)].append((time_value, value))

    plot_ready: List[Dict[str, Any]] = []
    series_by_metric_node: DefaultDict[Tuple[str, str], List[Dict[str, Any]]] = DefaultDict(list)

    for (metric_name, node_name, label), points in grouped.items():
        sorted_points = sorted(points, key=lambda item: item[0])
        series_by_metric_node[(metric_name, node_name)].append(
            {
                "label": label,
                "points": [{"time": time, "value": value} for time, value in sorted_points],
            }
        )

    for (metric_name, node_name), series_list in series_by_metric_node.items():
        plot_ready.append(
            {
                "metric": metric_name,
                "node": node_name,
                "series": series_list,
            }
        )

    return plot_ready

def extract_job_detail(job_data: Dict[str, Any]) -> Dict[str, Any]:
    job_info = {
        "status": job_data.get("exit_code", {}).get("status", []),
        "elapsed_time": job_data.get("time", {}).get("elapsed", ""),
        "start_time": job_data.get("time", {}).get("start", ""),
        "end_time": job_data.get("time", {}).get("end", ""),
        "nodes": job_data.get("nodes", ""),
    }
    return job_info


def main() -> None:
    args = parse_args()

    try:
        config = load_config(args.config)
    except FileNotFoundError as err:
        print(err)
        raise SystemExit(1)

    slurm_config = utils.get_slurm_config(config)
    job_detail = build_job_detail(args.job_id, slurm_config)

    if not job_detail:
        print(f"No data found for Job ID: {args.job_id}")
        raise SystemExit(0)

    job_info = extract_job_detail(job_detail)

    metrics_data = pull_metric_data(
        config=config,
        start=job_info.get("start_time"),
        end=job_info.get("end_time"),
        nodes=job_info.get("nodes", ""),
        metrics=args.metrics or DEFAULT_METRICS,
    )

    if metrics_data:
        plot_data = reshape_metrics_for_plot(metrics_data)
        print(json.dumps(plot_data, indent=2, default=str))
    else:
        print("No metric data returned.")


if __name__ == "__main__":
    # Example usage: python pull_data_by_job.py 20430_0 --metrics CPU_Temperature DRAM_PowerConsumption
    main()
