"""Native discovery only returns supported devices owned by this account."""

import json
import ssl
from unittest.mock import AsyncMock

import pytest
from test_bulb import STATE, Broker

from ha_xthings_cloud import XthingsCloudApiClient
from ha_xthings_cloud.bulb import NativeBulbClient


@pytest.mark.asyncio
async def test_discover_supported_bulb_routes(monkeypatch):
    session = AsyncMock()
    responses = [
        [{"id": 12}],
        [{"id": 34}],
        [
            {"uuid": "owned-bulb", "model": "A19-C1"},
            {"uuid": "other-model", "model": "unverified"},
            {
                "entry_type": "Britegroup",
                "uuid": "bathroom-group",
                "lights": [
                    {"uuid": "grouped-bulb", "model": "A19-C1"},
                    {"uuid": "unsupported-member", "model": "unverified"},
                ],
            },
        ],
    ]

    async def request(method, url, **kwargs):
        assert kwargs["data"]["token"] == "account-token"
        assert "timestamp" in json.loads(kwargs["data"]["data"])
        response = AsyncMock()
        response.raise_for_status = lambda: None
        response.json.return_value = {"code": 200, "data": responses.pop(0)}
        return response

    session.request.side_effect = request
    client = XthingsCloudApiClient(session, "account-token")
    routes = await client.async_get_native_bulb_routes()
    assert set(routes) == {"owned-bulb", "grouped-bulb"}
    for device_id, expected_prefix in [
        ("owned-bulb", "utec/lightness/only/owned-bulb"),
        ("grouped-bulb", "utec/lightness/group/bathroom-group/grouped-bulb"),
    ]:
        broker = Broker()
        monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
        bulb = NativeBulbClient(
            device_id, routes[device_id], ssl.create_default_context(), lambda s: None
        )
        try:
            await bulb.async_start()
            assert bulb.state == STATE
            assert broker.subscriptions[0] == expected_prefix + "/notify"
            assert broker.subscriptions[1].startswith(expected_prefix + "/accept/")
            assert broker.topics == {f"utec/lightness/12/{device_id}/command"}
        finally:
            await bulb.async_stop()
