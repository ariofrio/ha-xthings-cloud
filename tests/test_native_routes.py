"""Native discovery only returns supported devices owned by this account."""

import json
from unittest.mock import AsyncMock

import pytest

from ha_xthings_cloud import XthingsCloudApiClient


@pytest.mark.asyncio
async def test_discover_supported_bulb_routes():
    session = AsyncMock()
    responses = [
        [{"id": 12}],
        [{"id": 34}],
        [
            {"uuid": "owned-bulb", "model": "A19-C1"},
            {"uuid": "other-model", "model": "unverified"},
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
    assert await client.async_get_native_bulb_routes() == {"owned-bulb": 12}
