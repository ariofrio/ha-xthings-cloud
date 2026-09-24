"""Lifecycle, shutdown, and malformed-input regressions."""

import asyncio
import json
import ssl
from types import SimpleNamespace
from unittest.mock import AsyncMock

import aiomqtt
import pytest
from test_bulb import Broker

from ha_xthings_cloud import XthingsCloudApiClient, XthingsCloudApiError
from ha_xthings_cloud.bulb import NativeBulbClient


@pytest.mark.asyncio
async def test_reply_followed_by_disconnect_cannot_restore_availability(monkeypatch):
    class DisconnectBroker(Broker):
        disconnect = False

        async def publish(self, *args, **kwargs):
            await super().publish(*args, **kwargs)
            if self.disconnect:
                await self.queue.put(aiomqtt.MqttError("connection lost"))

    broker = DisconnectBroker()
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    reports = []
    bulb = NativeBulbClient("bulb", 123, ssl.create_default_context(), reports.append)
    try:
        await bulb.async_start()
        reports.clear()
        broker.disconnect = True
        try:
            await bulb.async_refresh()
        except XthingsCloudApiError:
            pass
        assert not bulb._connected.is_set()
        assert bulb.state is None, f"disconnected but reports={reports}"
    finally:
        # Let disconnect cleanup finish before stopping (tested separately).
        for _ in range(10):
            await asyncio.sleep(0)
        await bulb.async_stop()


@pytest.mark.asyncio
async def test_stop_rejects_new_controls_while_draining(monkeypatch):
    cancelling = asyncio.Event()
    release_cancel = asyncio.Event()
    entered = asyncio.Event()
    late_entered = asyncio.Event()
    late_release = asyncio.Event()

    class ShutdownBroker(Broker):
        async def publish(self, topic, payload, **kwargs):
            if json.loads(payload)["hd"]["np"] == "CC":
                if not entered.is_set():
                    entered.set()
                    try:
                        await asyncio.Event().wait()
                    except asyncio.CancelledError:
                        cancelling.set()
                        await release_cancel.wait()
                        raise
                else:
                    late_entered.set()
                    await late_release.wait()
            await super().publish(topic, payload, **kwargs)

    broker = ShutdownBroker()
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    bulb = NativeBulbClient(
        "bulb", 123, ssl.create_default_context(), lambda s: None, timeout=0.1
    )
    await bulb.async_start()
    first = asyncio.create_task(bulb.async_set_state({"pw": 0}))
    await entered.wait()
    stop = asyncio.create_task(bulb.async_stop())
    await cancelling.wait()
    late = asyncio.create_task(bulb.async_set_state({"pw": 1}))
    await asyncio.sleep(0)
    release_cancel.set()
    try:
        await stop
        assert not bulb._controls, (
            f"stop returned with {len(bulb._controls)} control task(s); late publish={late_entered.is_set()}"
        )
    finally:
        late_release.set()
        await bulb.async_stop()
        await asyncio.gather(first, late, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "result", [[], {"code": 200, "data": [None]}, {"code": 200, "data": [{}]}]
)
async def test_bad_discovery_is_api_error(result):
    response = AsyncMock()
    response.raise_for_status = lambda: None
    response.json.return_value = result
    session = AsyncMock()
    session.request.return_value = response
    client = XthingsCloudApiClient(session, "token")
    with pytest.raises(XthingsCloudApiError):
        await client.async_get_native_bulb_routes()


@pytest.mark.asyncio
async def test_deep_json_does_not_kill_connection_task(monkeypatch):
    broker = Broker()
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    bulb = NativeBulbClient("bulb", 123, ssl.create_default_context(), lambda s: None)
    await bulb.async_start()
    depth = 10000
    await broker.queue.put(
        SimpleNamespace(
            topic=bulb._notify, retain=False, payload=b"[" * depth + b"0" + b"]" * depth
        )
    )
    await asyncio.wait({bulb._task}, timeout=0.1)
    try:
        assert not bulb._task.done(), repr(bulb._task.exception())
    finally:
        try:
            await bulb.async_stop()
        except RecursionError:
            pass


@pytest.mark.asyncio
async def test_stop_during_disconnect_cleanup_does_not_reconnect(monkeypatch):
    cleanup_entered = asyncio.Event()
    cleanup_release = asyncio.Event()
    reconnected = asyncio.Event()

    class ObservedBroker(Broker):
        async def __aenter__(self):
            await super().__aenter__()
            if self.connections == 2:
                reconnected.set()
            return self

    class SlowMonitorCleanupBulb(NativeBulbClient):
        async def _monitor(self):
            try:
                await super()._monitor()
            finally:
                cleanup_entered.set()
                await cleanup_release.wait()

    broker = ObservedBroker()
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    bulb = SlowMonitorCleanupBulb(
        "bulb", 123, ssl.create_default_context(), lambda s: None
    )
    await bulb.async_start()
    await broker.queue.put(aiomqtt.MqttError("connection lost"))
    await cleanup_entered.wait()
    stop = asyncio.create_task(bulb.async_stop())
    reconnect_waiter = asyncio.create_task(reconnected.wait())
    try:
        done, _ = await asyncio.wait(
            {stop, reconnect_waiter}, timeout=2, return_when=asyncio.FIRST_COMPLETED
        )
        assert stop in done, (
            f"stop still pending; broker connections={broker.connections}"
        )
        assert broker.connections == 1
    finally:
        cleanup_release.set()
        reconnect_waiter.cancel()
        if not stop.done():
            bulb._task.cancel()
        await stop
        await asyncio.gather(reconnect_waiter, return_exceptions=True)


@pytest.mark.asyncio
async def test_query_deadline_includes_both_publish_acks(monkeypatch):
    class SlowAckBroker(Broker):
        slow = False
        reads = 0

        async def publish(self, *args, **kwargs):
            if self.slow:
                self.reads += 1
                # Each publication is shorter than the configured 100ms timeout.
                await asyncio.sleep(0.06 if self.reads == 1 else 0.07)
                if self.reads == 1:
                    return
            await super().publish(*args, **kwargs)

    broker = SlowAckBroker()
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    bulb = NativeBulbClient(
        "bulb",
        123,
        ssl.create_default_context(),
        lambda s: None,
        timeout=0.1,
        backup_query_delay=0.02,
    )
    try:
        await bulb.async_start()
        broker.slow = True
        start = asyncio.get_running_loop().time()
        try:
            await bulb.async_refresh()
        except XthingsCloudApiError:
            pass
        elapsed = asyncio.get_running_loop().time() - start
        assert elapsed < 0.12, f"100ms query exceeded deadline: {elapsed:.3f}s"
    finally:
        await bulb.async_stop()
