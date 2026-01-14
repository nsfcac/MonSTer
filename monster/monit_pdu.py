import time
import psycopg2
import schedule
from pgcopy import CopyManager
from datetime import datetime, timezone

from monster import infra, utils


def monit_pdu(config): 
    cols = ('timestamp', 'nodeid', 'value')
    connection         = utils.init_tsdb_connection(config)
    username, password = utils.get_pdu_auth()
    pdu_api            = utils.get_pdu_api(config)

    # Get PDU IP list
    pdu_list = utils.get_infra_ip_list(config, 'pdu')
    with psycopg2.connect(connection) as conn:
        nodeid_map = utils.get_infra_nodeid_map(conn)
        timestamp  = datetime.now(timezone.utc).replace(microsecond=0)
        processed_records = infra.get_pdu_metrics_pull(pdu_api, timestamp, pdu_list, username, password, nodeid_map)

        for tabel, records in processed_records.items():
            mgr = CopyManager(conn, tabel, cols)
            mgr.copy(records)
        conn.commit()


if __name__ == "__main__":
    config = utils.parse_config()
    schedule.every(1).minutes.at(":00").do(monit_pdu, config)
    while True:
        schedule.run_pending()
        time.sleep(1)
