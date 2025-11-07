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
DEFAULT_CONFIG = Path(__file__).resolve().parents[1] / "config-zen4.yml"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query detailed information for a Slurm job.")
    parser.add_argument("job_id", help="Slurm job identifier to look up.")
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help=f"Path to the MonSter configuration file (default: {DEFAULT_CONFIG}).",
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

    if job_detail:
        print(json.dumps(job_detail, indent=2, default=str))
    else:
        print("No job information returned.")


if __name__ == "__main__":
    # Example usage: python pull_data_by_job.py 20430_0 
    main()
