from monster import process


def get_pdu_metrics_pull(pdu_api: list, timestamp, pdu_list: list, 
                         username: str, password: str, nodeid_map: dict):

    urls = [f"https://{node}{url}" for url in pdu_api for node in pdu_list]
    redfish_report = process.run_fetch_all(urls, username, password)
    if redfish_report:
        processed_records = process.process_all_pdu_pull(pdu_api, timestamp, pdu_list, redfish_report, nodeid_map)
        return processed_records



def get_irc_metrics_snmp(timestamp, irc_list: list, username: str, nodeid_map: dict):
    """
    Get the metrics definition via SNMP.
    """
    irc_metrics = process.run_snmp_all(irc_list, username)
    if irc_metrics:
        processed_records = process.process_all_irc_metrics(timestamp, irc_metrics, nodeid_map)
        return processed_records
    return {}
    