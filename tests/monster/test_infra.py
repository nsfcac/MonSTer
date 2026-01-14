import pytest

from monster import infra


def test_get_pdu_metrics_pull_success(mocker):
    mocker.patch(
        "monster.infra.process.run_fetch_all",
        return_value=["raw1", "raw2"],
    )

    process_mock = mocker.patch(
        "monster.infra.process.process_all_pdu_pull",
        return_value={"pdu.pdu": ["pdu1", "pdu2"]},
    )

    result = infra.get_pdu_metrics_pull(
        pdu_api=["/redfish/v1/Power"],
        timestamp=123,
        pdu_list=["pdu1", "pdu2"],
        username="user",
        password="pass",
        nodeid_map={"node1": 1},
    )

    assert result == {"pdu.pdu": ["pdu1", "pdu2"]}
    process_mock.assert_called_once_with(
        ["/redfish/v1/Power"],
        123,
        ["pdu1", "pdu2"],
        ["raw1", "raw2"],
        {"node1": 1},
    )


def test_get_pdu_metrics_pull_no_data(mocker):
    mocker.patch(
        "monster.infra.process.run_fetch_all",
        return_value=None,
    )

    result = infra.get_pdu_metrics_pull(
        pdu_api=["/redfish/v1/Power"],
        timestamp=123,
        pdu_list=["pdu1"],
        username="user",
        password="pass",
        nodeid_map={},
    )

    assert result is None


def test_get_irc_metrics_snmp_success(mocker):
    mocker.patch(
        "monster.infra.process.run_snmp_all",
        return_value=[
            {"irc_ip": "123.123", "metrics": {
                "metric_id": "irc1",
                "value": 1
            }}
        ]
    )

    process_mock = mocker.patch(
        "monster.infra.process.process_all_irc_metrics",
        return_value={"irc.irc1": (123, 1, 1)},
    )

    result = infra.get_irc_metrics_snmp(
        timestamp=123,
        irc_list=["irc1"],
        username="user",
        nodeid_map={"node1": 1},
    )

    assert result == {"irc.irc1": (123, 1, 1)}
    process_mock.assert_called_once_with(
        123,
        [
            {"irc_ip": "123.123", "metrics": {
                "metric_id": "irc1",
                "value": 1
            }}
        ],
        {"node1": 1},
    )


def test_get_irc_metrics_snmp_success_multi_metrics(mocker):
    mocker.patch(
        "monster.infra.process.run_snmp_all",
        return_value=[
            {"irc_ip": "node1", "metrics":
                [{
                    "metric_id": "irc1",
                    "value": 1
                }]
             },
            {"irc_ip": "node2", "metrics":
                [{
                    "metric_id": "irc1",
                    "value": 11
                },
                    {
                        "metric_id": "irc2",
                        "value": 2
                    }]
             }
        ]
    )

    process_mock = mocker.patch(
        "monster.infra.process.process_all_irc_metrics",
        return_value={"irc.irc1": {(123, 1, 1), (123, 2, 11)}, "irc.irc2": {(123, 2, 2)}},
    )

    result = infra.get_irc_metrics_snmp(
        timestamp=123,
        irc_list=["irc1"],
        username="user",
        nodeid_map={"node1": 1, "node2": 2},
    )

    assert result == {"irc.irc1": {(123, 1, 1), (123, 2, 11)}, "irc.irc2": {(123, 2, 2)}}
    process_mock.assert_called_once_with(
        123,
        [
            {"irc_ip": "node1", "metrics":
                [{
                    "metric_id": "irc1",
                    "value": 1
                }]
             },
            {"irc_ip": "node2", "metrics":
                [{
                    "metric_id": "irc1",
                    "value": 11
                },
                    {
                        "metric_id": "irc2",
                        "value": 2
                    }]
             }
        ],
        {"node1": 1, "node2": 2},
    )


def test_get_irc_metrics_snmp_no_data(mocker):
    mocker.patch(
        "monster.infra.process.run_snmp_all",
        return_value=None,
    )

    result = infra.get_irc_metrics_snmp(
        timestamp=123,
        irc_list=["irc1"],
        username="user",
        nodeid_map={},
    )

    assert result == {}
