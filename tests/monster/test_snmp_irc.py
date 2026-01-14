import pytest
from unittest.mock import patch, MagicMock
from monster import snmp_irc
from monster.snmp_irc import (
    METRIC_NAME_OID,
    METRIC_VALU_OID,
    METRIC_UNIT_OID,
    METRIC_ACCU_OID,
)


@pytest.mark.asyncio
async def test_get_irc_metrics_error_indication():
    var_binds = [
        ("timeout", None, None, None),
    ]

    with patch("monster.snmp_irc.walk_cmd", return_value=async_walk(var_binds)):
        result = await snmp_irc.get_irc_metrics("1.2.3.4", "user")

    assert result is None


@pytest.mark.asyncio
async def test_get_irc_metrics_error_status():
    error_status = MagicMock()
    error_status.prettyPrint.return_value = "error"

    var_binds = [
        (None, error_status, None, [[(MagicMock(), MagicMock())]]),
    ]

    with patch("monster.snmp_irc.walk_cmd", return_value=async_walk(var_binds)):
        result = await snmp_irc.get_irc_metrics("1.2.3.4", "user")

    assert result is None


@pytest.mark.asyncio
async def test_get_irc_metrics_var_bind_table():
    var_binds = [
        (
            None,
            None,
            None,
            [
                (SNMPOID(f"{METRIC_NAME_OID}1"), SNMPVALUE("CPU Load")),
                (SNMPOID(f"{METRIC_VALU_OID}1"), SNMPVALUE("42")),
                (SNMPOID(f"{METRIC_UNIT_OID}1"), SNMPVALUE("%")),
                (SNMPOID(f"{METRIC_ACCU_OID}1"), SNMPVALUE("1")),
            ],
        )
    ]

    with patch("monster.snmp_irc.walk_cmd", return_value=async_walk(var_binds)), \
            patch("monster.snmp_irc.SnmpEngine"), \
            patch("monster.snmp_irc.UsmUserData"), \
            patch("monster.snmp_irc.UdpTransportTarget.create"), \
            patch("monster.snmp_irc.ContextData"), \
            patch("monster.snmp_irc.ObjectIdentity"), \
            patch("monster.snmp_irc.ObjectType", return_value=MagicMock()):
        result = await snmp_irc.get_irc_metrics("1.2.3.4", "user")

    expected_result = [{"snmp_oid": "SNMPv2-SMI::enterprises.318.1.1.27.1.4.1.2.1.3.1.1", "metric_id": "CPULoad",
                        "metric_name": "CPU Load", "value": 42, "metric_data_type": "INT", "units": "%", "accuracy": 1}]
    assert len(result) == 1
    assert result == expected_result


@pytest.mark.asyncio
async def test_get_irc_metrics_duplicate_metric():
    var_binds = [
        (
            None,
            None,
            None,
            [
                (SNMPOID(f"{METRIC_NAME_OID}1"), SNMPVALUE("CPU Load")),
                (SNMPOID(f"{METRIC_VALU_OID}1"), SNMPVALUE("10")),
                (SNMPOID(f"{METRIC_ACCU_OID}1"), SNMPVALUE("2")),
            ]
        ),
        (
            None,
            None,
            None,
            [
                (SNMPOID(f"{METRIC_NAME_OID}2"), SNMPVALUE("CPU Load")),
                (SNMPOID(f"{METRIC_VALU_OID}2"), SNMPVALUE("20")),
                (SNMPOID(f"{METRIC_ACCU_OID}2"), SNMPVALUE("1")),
            ]
        ),
    ]

    with patch("monster.snmp_irc.walk_cmd", return_value=async_walk(var_binds)), \
            patch("monster.snmp_irc.SnmpEngine"), \
            patch("monster.snmp_irc.UsmUserData"), \
            patch("monster.snmp_irc.UdpTransportTarget.create"), \
            patch("monster.snmp_irc.ContextData"), \
            patch("monster.snmp_irc.ObjectIdentity"), \
            patch("monster.snmp_irc.ObjectType", return_value=MagicMock()):
        result = await snmp_irc.get_irc_metrics("1.2.3.4", "user")

    expected_result = [{"snmp_oid": "SNMPv2-SMI::enterprises.318.1.1.27.1.4.1.2.1.3.1.1", "metric_id": "CPULoad",
                        "metric_name": "CPU Load", "value": 5, "metric_data_type": "REAL", "units": None,
                        "accuracy": 2}]
    assert len(result) == 1
    assert result == expected_result


async def async_walk(rows):
    for row in rows:
        yield row


class SNMPOID:
    def __init__(self, value):
        self.value = value

    def prettyPrint(self):
        return self.value


class SNMPVALUE:
    def __init__(self, value):
        self.value = value

    def prettyPrint(self):
        return self.value
