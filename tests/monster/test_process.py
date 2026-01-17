import asyncio
import pytest
from unittest.mock import AsyncMock, patch, Mock
from datetime import datetime
from dateutil.tz import tzutc
from monster import process


def test_run_single_snmp_success(mocker):
    mocker.patch(
        "monster.process.snmp_irc.get_irc_metrics",
        return_value=sample_metrics(),
    )
    mocker.patch("asyncio.run", return_value=sample_metrics())

    result = process.run_single_snmp("1.1.1.1", "user")
    assert result["irc_ip"] == "1.1.1.1"
    assert result["metrics"] == sample_metrics()


def test_run_single_snmp_empty(mocker):
    mocker.patch("asyncio.run", return_value=None)
    assert process.run_single_snmp("1.1.1.1", "user") == {}


def test_run_snmp_all(mocker):
    mock_pool = mocker.patch("multiprocessing.Pool").return_value.__enter__.return_value
    mock_pool.starmap.return_value = [{"irc_ip": "1.1.1.1", "metrics": sample_metrics()}]

    result = process.run_snmp_all(["n1"], "user")
    assert result == [{"irc_ip": "1.1.1.1", "metrics": sample_metrics()}]


def test_run_snmp_all_empty(mocker):
    mock_pool = mocker.patch("multiprocessing.Pool").return_value.__enter__.return_value
    mock_pool.starmap.return_value = [{}]

    result = process.run_snmp_all(["n1"], "user")
    assert result == [{}]


def test_extract_metadata_hostname_c():
    system_info = {"SKU": "s", "HostName": "c001", "Status": {"Health": "Good"}}
    bmc_info = {"Model": "x"}
    result = process.extract_metadata(system_info, bmc_info, "10.101.1.1")

    assert result["ServiceTag"] == "s"
    assert result["UUID"] is None
    assert result["Bmc_Ip_Addr"] == "10.101.1.1"
    assert result["BmcModel"] == "x"
    assert result["HostName"] == "rpc-1-1"
    assert result["Status"] == "Good"


def test_extract_metadata_hostname_g():
    system_info = {"UUID": "001", "HostName": "g001", "ProcessorSummary":
        {"Count": 2, "LogicalProcessorCount": 4}, "MemorySummary":
                       {"TotalSystemMemoryGiB": 64}}
    bmc_info = {"Model": "abc"}
    result = process.extract_metadata(system_info, bmc_info, "10.101.1.91")

    assert result["ServiceTag"] is None
    assert result["UUID"] == "001"
    assert result["Bmc_Ip_Addr"] == "10.101.1.91"
    assert result["ProcessorModel"] is None
    assert result["ProcessorCount"] == 2
    assert result["LogicalProcessorCount"] == 4
    assert result["TotalSystemMemoryGiB"] == 64
    assert result["BmcModel"] == "abc"
    assert result["HostName"] == "rpg-1-91"
    assert result["Status"] is None


def test_extract_metadata_unreachable():
    result = process.extract_metadata({}, {}, "10.101.1.99")
    assert result["Bmc_Ip_Addr"] == "10.101.1.99"
    assert result["HostName"] == "rpg-1-99"
    assert result["Status"] == "BMC unreachable"


def test_extract_metadata_exception(mocker):
    system_info = {"UUID": "001", "HostName": "g001"}
    bmc_info = Mock()
    bmc_info.get.side_effect = Exception("forced exception")
    result = process.extract_metadata(system_info, bmc_info, "10.101.1.99")
    assert result is None


def test_parallel_extract_metadata(mocker):
    system_info = [{"SKU": "s", "HostName": "c001", "Status": {"Health": "Good"}}]
    bmc_info = [{"Model": "x"}]
    pool_result = [{
        "ServiceTag": None,
        "UUID": "001",
        "Bmc_Ip_Addr": "10.101.1.91",
        "ProcessorModel": None,
        "ProcessorCount": 2,
        "LogicalProcessorCount": 4,
        "TotalSystemMemoryGiB": 64,
        "BmcModel": "abc",
        "HostName": "rpg-1-91",
        "Status": None
    }]

    mock_pool = mocker.patch("multiprocessing.Pool").return_value.__enter__.return_value
    mock_pool.starmap.return_value = pool_result

    result = process.parallel_extract_metadata(system_info, bmc_info, ["n"])
    assert result == pool_result


def test_extract_fqdd_source_pull():
    report = [{"Fans": [{"Name": "Fan 1", "@odata.type": "X"}, {"Name": "Fan 2"}]}]
    fqdd, source = process.extract_fqdd_source_pull(report, ["Fans"])
    assert fqdd == ["Fan_1", "Fan_2"]
    assert source == ["X", "None"]


def test_extract_fqdd_source_push():
    telemetry = {"Oem": {"Dell": {"FQDDList": "Fan 1", "SourceList": "XYZ"}}}
    fqdd, source = process.extract_fqdd_source_push(telemetry)
    assert fqdd == "Fan 1"
    assert source == "XYZ"


def test_process_all_idracs_pull(mocker):
    mocker.patch(
        "monster.process.parallel_process_idrac_pull",
        side_effect=[
            [{"record_1": 1}],
            [{"record_2": 2}],
        ],
    )

    idrac_api = ["idrac_api"]
    nodelist = ["node1", "node2"]
    redfish_report = ["redfish_1", "redfish_2"]

    result = process.process_all_idracs_pull(
        idrac_api=idrac_api,
        timestamp=100,
        idrac_metrics=["idrac_temp", "idrac_cpu"],
        nodelist=nodelist,
        redfish_report=redfish_report,
        nodeid_map={},
        source_map={},
        fqdd_map={}
    )

    expected_result = {"idrac.idrac_temp": [{"record_1": 1}], "idrac.idrac_cpu": [{"record_2": 2}]}

    assert result == expected_result


def test_parallel_process_idrac_pull(mocker):
    pool_mock = mocker.MagicMock()
    pool_mock.starmap.return_value = [[{"record_1": 1}], [{"record_2": 2}]]

    pool_ctx = mocker.MagicMock()
    pool_ctx.__enter__.return_value = pool_mock

    mocker.patch("monster.process.multiprocessing.Pool", return_value=pool_ctx)

    result = process.parallel_process_idrac_pull(
        timestamp=100,
        idrac_metric="idrac_temp",
        nodelist=["node1"],
        reports=["report1"],
        nodeid_map={"node1": 1},
        source_map={},
        fqdd_map={}
    )

    expected_result = [{"record_1": 1}, {'record_2': 2}]

    assert result == expected_result


def test_process_node_idrac_pull():
    report = {
        "Fans": [{"FanName": "Fan1", "@odata.type": "source1", "Reading": 10}]
    }
    result = process.process_node_idrac_pull(
        1, "Fans", "node1", report, {"node1": 1}, {"source1": 1}, {"Fan1": 1}
    )
    expected_result = [(1, 1, 1, 1, 10)]
    assert result == expected_result


def test_process_all_pdu_pull(mocker):
    mocker.patch(
        "monster.process.parallel_process_pdu_pull",
        side_effect=[
            [{"record_1": 1}],
            [{"record_2": 2}],
        ],
    )

    pdu_api = ["pdu_api"]
    redfish_report = ["redfish_1", "redfish_2"]

    result = process.process_all_pdu_pull(
        pdu_api=pdu_api,
        timestamp=100,
        pdu_list=["pdu_1", "pdu_2"],
        redfish_report=redfish_report,
        nodeid_map={"node1": 1, "node2": 2},
    )

    expected_result = {"pdu.pdu": [{"record_1": 1}]}

    assert result == expected_result


def test_parallel_process_pdu_pull(mocker):
    pool_mock = mocker.MagicMock()
    pool_mock.starmap.return_value = [[{"record_1": 1}], [{"record_2": 2}]]

    pool_ctx = mocker.MagicMock()
    pool_ctx.__enter__.return_value = pool_mock

    mocker.patch("monster.process.multiprocessing.Pool", return_value=pool_ctx)

    result = process.parallel_process_pdu_pull(
        timestamp=100,
        pdu_list=["pdu_1"],
        reports=["report1"],
        nodeid_map={"node1": 1}
    )

    expected_result = [{"record_1": 1}, {'record_2': 2}]

    assert result == expected_result


def test_process_node_pdu_pull():
    result = process.process_node_pdu_pull(1, "node1", {"Reading": 1.2}, {"node1": 1})
    expected_result = [(1, 1, 1.2)]
    assert result == expected_result


def test_process_job_metrics_slurm_basic(mocker):
    mocker.patch(
        "monster.process.sql.job_info_column_names",
        ["job_id", "nodes", "exit_code"]
    )

    mocker.patch(
        "monster.process.hostlist.expand_hostlist",
        return_value=["node1", "node2"]
    )

    jobs_metrics = [
        {
            "job_id": 123,
            "nodes": "node[1-2]",
            "exit_code": {"return_code": {"number": 0}},
        }
    ]

    result = process.process_job_metrics_slurm(jobs_metrics)

    assert result == [
        (123, ["node1", "node2"], 0)
    ]


def test_process_job_metrics_slurm_missing_fields(mocker):
    mocker.patch(
        "monster.process.sql.job_info_column_names",
        ["job_id", "nodes", "exit_code"]
    )

    jobs_metrics = [
        {
            "job_id": 456,
            "nodes": None,
            "exit_code": None,
        }
    ]

    result = process.process_job_metrics_slurm(jobs_metrics)

    assert result == [
        (456, [], None)
    ]


def test_process_node_metrics_slurm_state_down():
    nodes = [{
        "hostname": "hostname1",
        "state": "down",
        "cpu_load": 0,
        "free_mem": {"number": 0},
        "real_memory": 1,
    }]
    expected_result = {
        'cpu_load': [(1, 99, 0)],
        'memoryusage': [(1, 99, 0.0)],
        'memory_used': [(1, 99, 0)],
        'state': [(1, 99, "down")],
    }
    result = process.process_node_metrics_slurm(nodes, {"hostname1": 99}, 1)
    assert result == expected_result


def test_process_node_metrics_slurm_state_not_down():
    nodes = [{
        "hostname": "hostname1",
        "state": "good",
        "cpu_load": 22,
        "free_mem": {"number": 20},
        "real_memory": 100,
    }]
    expected_result = {
        'cpu_load': [(1, 99, 22)],
        'memoryusage': [(1, 99, 80)],
        'memory_used': [(1, 99, 80)],
        'state': [(1, 99, "good")],
    }
    result = process.process_node_metrics_slurm(nodes, {"hostname1": 99}, 1)
    assert result == expected_result


def test_process_node_job_correlation_running():
    jobs = [
        {
            "job_state": "RUNNING",
            "job_id": 1,
            "job_resources": {
                "nodes": {
                    "list": "hostname1",
                    "allocation": [
                        {
                            "name": "hostname1",
                            "cpus": {"count": 2}
                        }
                    ]
                }
            }
        },
        {
            "job_state": "RUNNING",
            "job_id": 2,
            "job_resources": {
                "nodes": {
                    "list": "hostname1",
                    "allocation": [
                        {
                            "name": "hostname1",
                            "cpus": {"count": 8}
                        }
                    ]
                }
            }
        }
    ]

    expected_result = [(1, 99, [1, 2], [2, 8])]
    result = process.process_node_job_correlation(jobs, {"hostname1": 99}, 1)
    assert result == expected_result


def test_process_node_job_correlation_not_running():
    jobs = [
        {
            "job_state": "COMPLETED",
            "job_id": 1,
            "job_resources": {
                "nodes": {
                    "list": "hostname1",
                    "allocation": [
                        {
                            "name": "hostname1",
                            "cpus": {"count": 2}
                        }
                    ]
                }
            }
        },
        {
            "job_state": "RUNNING",
            "job_id": 1,
            "job_resources": {
                "nodes": {
                    "list": "hostname1",
                    "allocation": [
                        {
                            "name": "hostname100",
                            "cpus": {"count": 2}
                        }
                    ]
                }
            }
        }
    ]

    expected_result = []
    result = process.process_node_job_correlation(jobs, {"hostname1": 99}, 1)
    assert result == expected_result


def test_single_process_idrac_push_timestamp_source_fqdd_value_present():
    metrics = [{
        "MetricId": "Fans",
        "Timestamp": "2023-01-01 00:00:00+00:00",
        "MetricValue": 10,
        "Oem": {"Dell": {"Source": "S", "FQDD": "F"}},
    }]
    expected_result = {
        "Fans": [{"timestamp": datetime(2023, 1, 1, 0, 0, tzinfo=tzutc()), "source": "S", "fqdd": "F", "value": 10}]}
    result = process.single_process_idrac_push("ip", "r", metrics, ["Fans"])
    assert result == expected_result


def test_single_process_idrac_push_tablename_in_metrics():
    metrics = [{
        "MetricId": "Fans",
        "Timestamp": "2023-01-01 00:00:00+00:00",
        "MetricValue": 10,
        "Oem": {"Dell": {"Source": "S", "FQDD": "F"}},
    },
        {
            "MetricId": "Fans",
            "Timestamp": "2023-01-02 00:00:00+00:00",
            "MetricValue": 15,
            "Oem": {"Dell": {"Source": "S", "FQDD": "F"}},
        }
    ]
    expected_result = {
        "Fans": [{"timestamp": datetime(2023, 1, 1, 0, 0, tzinfo=tzutc()), "source": "S", "fqdd": "F", "value": 10},
                 {"timestamp": datetime(2023, 1, 2, 0, 0, tzinfo=tzutc()), "source": "S", "fqdd": "F", "value": 15}]}
    result = process.single_process_idrac_push("ip", "r", metrics, ["Fans"])
    assert result == expected_result


def test_single_process_idrac_push_timestamp_source_fqdd_value_one_not_present():
    metrics = [{
        "MetricId": "Fans",
        "MetricValue": 10,
        "Oem": {"Dell": {"Source": "S", "FQDD": "F"}},
    },
        {
            "MetricId": "Temp",
            "MetricValue": 11,
            "Timestamp": "2023-01-01 00:00:00+00:00",
            "Oem": {"Dell": {"Source": "source", "FQDD": "fqdd"}},
        }
    ]
    expected_result = {
        "Temp": [
            {"timestamp": datetime(2023, 1, 1, 0, 0, tzinfo=tzutc()), "source": "source", "fqdd": "fqdd", "value": 11}]}
    result = process.single_process_idrac_push("ip", "r", metrics, ["Fans", "Temp"])
    assert result == expected_result


@pytest.mark.asyncio
async def test_write_idrac_push_success(mocker):
    conn = mocker.Mock()
    mp_queue = asyncio.Queue()

    mock_copy_mgr = mocker.Mock()
    mocker.patch(
        "monster.process.CopyManager",
        return_value=mock_copy_mgr
    )

    mocker.patch(
        "monster.process.utils.cast_value_type",
        return_value=42
    )

    await mp_queue.put((
        "1.2.3.4",
        {
            "Fans": [
                {
                    "timestamp": 123,
                    "source": "SRC",
                    "fqdd": "FQDD",
                    "value": "42"
                }
            ]
        }
    ))

    task = asyncio.create_task(
        process.write_idrac_push(
            conn,
            nodeid_map={"1.2.3.4": 1},
            source_map={"SRC": 10},
            fqdd_map={"FQDD": 20},
            metric_dtype_mapping={"Fans": int},
            mp_queue=mp_queue
        )
    )

    await asyncio.sleep(0.1)
    task.cancel()

    mock_copy_mgr.copy.assert_called_once()
    conn.commit.assert_called()


def test_process_all_irc_metrics():
    irc_metrics = [{"irc_ip": "rpc-1", "metrics": [{"metric_id": "Temp", "value": 1}]}, {"irc_ip": "rpg-1", "metrics": [
        {"metric_id": "Temp", "value": 10}, {"metric_id": "CPU", "value": 90}]}]
    expected_result = {"irc.temp": [(1, "10.1.1.1", 1), (1, "10.1.1.91", 10)], "irc.cpu": [(1, "10.1.1.91", 90)]}
    result = process.process_all_irc_metrics(1, irc_metrics,
                                             {"rpc-1": "10.1.1.1", "rpc-3": "10.1.1.3", "rpg-1": "10.1.1.91"})
    assert result == expected_result


def sample_metrics():
    sample_metrics = [
        {
            "snmp_oid": "1.3.6.1.4.1.318.1.1.12.1.1.1",
            "metric_id": "CPUUsage",
            "metric_name": "CPU Usage",
            "value": 55,
            "metric_data_type": "INT",
            "units": "%",
            "accuracy": 1
        },
        {
            "snmp_oid": "1.3.6.1.4.1.318.1.1.12.1.1.2",
            "metric_id": "MemoryUsage",
            "metric_name": "Memory Usage",
            "value": 2048,
            "metric_data_type": "INT",
            "units": "MB",
            "accuracy": 1
        }
    ]

    return sample_metrics
