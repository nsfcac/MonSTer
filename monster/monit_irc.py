import time
import asyncio
import psycopg2
import schedule
from pgcopy import CopyManager
from datetime import datetime, timezone

from monster import infra, utils


def monit_irc(config): 
    cols = ('timestamp', 'nodeid', 'value')
    connection = utils.init_tsdb_connection(config)
    username   = utils.get_irc_auth()

    # Get IRC IP list
    irc_list = utils.get_infra_ip_list(config, 'irc')

    with psycopg2.connect(connection) as conn:
        nodeid_map = utils.get_infra_nodeid_map(conn)

        timestamp  = datetime.now(timezone.utc).replace(microsecond=0)
        processed_records = infra.get_irc_metrics_snmp(timestamp, irc_list, username, nodeid_map)

        for tabel, records in processed_records.items():
            mgr = CopyManager(conn, tabel, cols)
            mgr.copy(records)
        conn.commit()


if __name__ == "__main__":
    config = utils.parse_config()
    schedule.every(2).minutes.at(":00").do(monit_irc, config)
    while True:
        schedule.run_pending()
        time.sleep(1)
