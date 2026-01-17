import pytest

from monster import idrac


def test_get_nodes_metadata_all_unreachable(mocker):
    nodelist = ["node1", "node2"]
    valid_nodelist = []

    mocker.patch(
        "monster.idrac.process.run_fetch_all",
        return_value=[]
    )

    mocker.patch(
        "monster.idrac.process.parallel_extract_metadata",
        return_value=[
            {
                "Status": "BMC unreachable",
                "Bmc_Ip_Addr": "node1",
                "HostName": "rpc-node1",
            },
            {
                "Status": "BMC unreachable",
                "Bmc_Ip_Addr": "node2",
                "HostName": "rpc-node2",
            },
        ],
    )

    metadata = idrac.get_nodes_metadata(
        nodelist=nodelist,
        valid_nodelist=valid_nodelist,
        username="user",
        password="pass",
    )

    assert metadata is not None
    assert valid_nodelist == []


def test_get_nodes_metadata_all_reachable(mocker):
    nodelist = ["node1", "node2"]
    valid_nodelist = []

    mocker.patch(
        "monster.idrac.process.run_fetch_all",
        return_value=[]
    )

    mocker.patch(
        "monster.idrac.process.parallel_extract_metadata",
        return_value=[
            {
                "Status": "OK",
                "Bmc_Ip_Addr": "node1",
                "HostName": "rpc-node1",
            },
            {
                "Status": "OK",
                "Bmc_Ip_Addr": "node2",
                "HostName": "rpc-node2",
            },
        ],
    )

    metadata = idrac.get_nodes_metadata(
        nodelist=nodelist,
        valid_nodelist=valid_nodelist,
        username="user",
        password="pass",
    )

    assert len(metadata) == 2
    assert valid_nodelist == ["node1", "node2"]


def test_get_nodes_metadata_empty_nodelist(mocker):
    nodelist = []
    valid_nodelist = []

    mocker.patch(
        "monster.idrac.process.run_fetch_all",
        return_value=[]
    )

    mocker.patch(
        "monster.idrac.process.parallel_extract_metadata",
        return_value=[],
    )

    metadata = idrac.get_nodes_metadata(
        nodelist=nodelist,
        valid_nodelist=valid_nodelist,
        username="user",
        password="pass",
    )

    assert metadata == []
    assert valid_nodelist == []


def test_get_fqdd_source_pull_success(mocker):
    nodelist = ["node1", "node2"]

    mocker.patch(
        "monster.idrac.process.run_fetch_all",
        return_value={"redfish": "report"},
    )

    mocker.patch(
        "monster.idrac.process.extract_fqdd_source_pull",
        return_value={"fqdd": "source"},
    )

    result = idrac.get_fqdd_source_pull(
        nodelist=nodelist,
        api=["/redfish/v1/MetricDefinitions/Fans"],
        metrics=["Fans", "Temperatures"],
        username="user",
        password="pass",
    )

    assert result == {"fqdd": "source"}


def test_get_fqdd_source_pull_failure(mocker):
    nodelist = ["node1", "node2"]

    mocker.patch(
        "monster.idrac.process.run_fetch_all",
        return_value=None,
    )

    result = idrac.get_fqdd_source_pull(
        nodelist=nodelist,
        api=["/redfish/v1/MetricDefinitions/Fans"],
        metrics=["Fans", "Temperatures"],
        username="user",
        password="pass",
    )

    assert result is None


def test_get_fqdd_source_push_success(mocker):
    nodelist = ["node1", "node2"]

    mocker.patch(
        "monster.idrac.process.single_fetch",
        return_value={"TelemetryService": "data"},
    )

    mocker.patch(
        "monster.idrac.process.extract_fqdd_source_push",
        return_value={"fqdd": "source"},
    )

    result = idrac.get_fqdd_source_push(
        nodelist=nodelist,
        username="user",
        password="pass",
    )

    assert result == {"fqdd": "source"}


def test_get_fqdd_source_push_failure(mocker):
    nodelist = ["node1", "node2"]

    mocker.patch(
        "monster.idrac.process.single_fetch",
        return_value=None,
    )

    result = idrac.get_fqdd_source_push(
        nodelist=nodelist,
        username="user",
        password="pass",
    )

    assert result is None


def test_get_metric_definitions_pull():
    idrac_metrics = ["Fans", "Temperatures", "PowerControl", "Unknown"]

    result = idrac.get_metric_definitions_pull(idrac_metrics)

    expected_result = [
        {"Id": "Fans", "MetricDataType": "Integer", "Units": "RPM"},
        {"Id": "Temperatures", "MetricDataType": "Integer", "Units": "Cel"},
        {"Id": "PowerControl", "MetricDataType": "Integer", "Units": "Watts"},
        {"Id": "Unknown", "MetricDataType": "Integer", "Units": None},
    ]

    assert result == expected_result


def test_get_metric_definitions_push_more_urls_than_nodes(mocker):
    mocker.patch(
        "monster.idrac.get_metric_definition_urls_push",
        return_value=[
            "/redfish/v1/MetricDefinitions/Fans",
            "/redfish/v1/MetricDefinitions/Temps",
        ],
    )

    mocker.patch(
        "monster.idrac.process.run_fetch_all",
        return_value=[
            {"Id": "Fans"},
            {"Id": "Temps"},
        ],
    )

    result = idrac.get_metric_definitions_push(
        nodelist=["node1"],
        idrac_metrics=["Fans", "Temps"],
        username="user",
        password="pass",
    )

    assert len(result) == 2
    assert result[0]["Id"] == "Fans"
    assert result[1]["Id"] == "Temps"
    assert result[0]["MetricDataType"] is None
    assert result[0]["Units"] is None


def test_get_metric_definitions_push_more_nodes(mocker):
    mocker.patch(
        "monster.idrac.get_metric_definition_urls_push",
        return_value=[
            "/redfish/v1/MetricDefinitions/Fans"
        ],
    )

    mocker.patch(
        "monster.idrac.process.run_fetch_all",
        return_value=[
            {
                "Id": "Fans",
                "MetricDataType": "Integer",
                "Units": "RPM",
            }
        ],
    )

    result = idrac.get_metric_definitions_push(
        nodelist=["node1", "node2"],
        idrac_metrics=["Fans"],
        username="user",
        password="pass"
    )

    assert result == [
        {
            "Id": "Fans",
            "Name": None,
            "Description": None,
            "MetricType": None,
            "MetricDataType": "Integer",
            "Units": "RPM",
            "Accuracy": None,
            "SensingInterval": None,
            "DiscreteValues": None
        }
    ]


def test_get_metric_definitions_push_empty(mocker):
    mocker.patch(
        "monster.idrac.get_metric_definition_urls_push",
        return_value=[
            "/redfish/v1/MetricDefinitions/Fans"
        ],
    )

    mocker.patch(
        "monster.idrac.process.run_fetch_all",
        return_value=[]
    )

    result = idrac.get_metric_definitions_push(
        nodelist=["node1", "node2"],
        idrac_metrics=["Fans"],
        username="user",
        password="pass"
    )

    assert result is None


def test_get_metric_definition_urls_push_success(mocker):
    mocker.patch(
        "monster.idrac.process.single_fetch",
        return_value={
            "Members": [
                {"@odata.id": "/redfish/v1/MetricDefinitions/Fans"},
                {"@odata.id": "/redfish/v1/MetricDefinitions/Temps"},
            ]
        },
    )

    urls = idrac.get_metric_definition_urls_push(
        node="node1",
        idrac_metrics=["Fans"],
        username="user",
        password="pass",
    )

    assert urls == [
        "/redfish/v1/MetricDefinitions/Fans",
        "/redfish/v1/MetricDefinitions/Temps",
    ]


def test_get_metric_definition_urls_push_no_response(mocker):
    mocker.patch(
        "monster.idrac.process.single_fetch",
        return_value=None,
    )

    urls = idrac.get_metric_definition_urls_push(
        node="node1",
        idrac_metrics=["Fans"],
        username="user",
        password="pass",
    )

    assert urls == []


def test_get_idrac_metrics_pull_success(mocker):
    mocker.patch(
        "monster.idrac.process.run_fetch_all",
        return_value=["raw1", "raw2"],
    )

    process_mock = mocker.patch(
        "monster.idrac.process.process_all_idracs_pull",
        return_value=["processed"],
    )

    result = idrac.get_idrac_metrics_pull(
        api=["/redfish/v1/Systems"],
        timestamp=123,
        idrac_metrics=["Fans"],
        nodelist=["node1", "node2"],
        username="user",
        password="pass",
        nodeid_map={},
        source_map={},
        fqdd_map={},
    )

    assert result == ["processed"]
    process_mock.assert_called_once()


def test_get_idrac_metrics_pull_no_data(mocker):
    mocker.patch(
        "monster.idrac.process.run_fetch_all",
        return_value=None,
    )

    result = idrac.get_idrac_metrics_pull(
        api=["/redfish/v1/Systems"],
        timestamp=123,
        idrac_metrics=["Fans"],
        nodelist=["node1"],
        username="user",
        password="pass",
        nodeid_map={},
        source_map={},
        fqdd_map={},
    )

    assert result is None


@pytest.mark.asyncio
async def test_listen_process_write_single_node(mocker):
    listen_mock = mocker.patch(
        "monster.idrac.process.listen_idrac_push",
        new_callable=mocker.AsyncMock,
    )
    process_mock = mocker.patch(
        "monster.idrac.process.process_idrac_push",
        new_callable=mocker.AsyncMock,
    )
    write_mock = mocker.patch(
        "monster.idrac.process.write_idrac_push",
        new_callable=mocker.AsyncMock,
    )

    await idrac.listen_process_write_idrac_push(
        nodelist=["node1"],
        idrac_metrics=["Fans"],
        username="user",
        password="pass",
        conn=mocker.Mock(),
        nodeid_map={"node1": 1},
        source_map={},
        fqdd_map={},
        metric_dtype_mapping={},
    )

    assert listen_mock.await_count == 1
    process_mock.assert_awaited_once()
    write_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_listen_process_write_multiple_nodes(mocker):
    listen_mock = mocker.patch(
        "monster.idrac.process.listen_idrac_push",
        new_callable=mocker.AsyncMock,
    )
    mocker.patch(
        "monster.idrac.process.process_idrac_push",
        new_callable=mocker.AsyncMock,
    )
    mocker.patch(
        "monster.idrac.process.write_idrac_push",
        new_callable=mocker.AsyncMock,
    )

    nodelist = ["node1", "node2", "node3"]

    await idrac.listen_process_write_idrac_push(
        nodelist=nodelist,
        idrac_metrics=["Fans"],
        username="user",
        password="pass",
        conn=mocker.Mock(),
        nodeid_map={},
        source_map={},
        fqdd_map={},
        metric_dtype_mapping={},
    )

    assert listen_mock.await_count == len(nodelist)
