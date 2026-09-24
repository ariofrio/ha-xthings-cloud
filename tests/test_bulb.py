"""Native bulb behavior against a simulated MQTT broker boundary."""

import asyncio
import json
import ssl
from types import SimpleNamespace

import aiomqtt
import pytest

from ha_xthings_cloud import XthingsCloudApiError
from ha_xthings_cloud.bulb import NativeBulbClient

STATE = {"pw": 1, "br": 26, "ct": 1, "tp": 47, "hu": 0, "sa": 0, "li": 0}


class Broker:
    def __init__(self):
        self.state = dict(STATE)
        self.queue = asyncio.Queue()
        self.subscriptions = []
        self.requests = []
        self.topics = set()
        self.drop = False
        self.ignore_commands = False
        self.connections = 0
        self.reply_topic = 1

    def client(self, **kwargs):
        return self

    async def __aenter__(self):
        self.connections += 1
        self.subscriptions.clear()
        return self

    async def __aexit__(self, *args):
        pass

    async def subscribe(self, topic, **kwargs):
        self.subscriptions.append(topic)

    @property
    def messages(self):
        return self

    def __aiter__(self):
        return self

    async def __anext__(self):
        message = await self.queue.get()
        if isinstance(message, Exception):
            raise message
        return message

    async def publish(self, topic, payload, **kwargs):
        self.topics.add(topic)
        request = json.loads(payload)
        self.requests.append(request)
        assert len(self.subscriptions) == 2
        if request["hd"]["np"] == "CC":
            if not self.ignore_commands:
                if request["hd"]["na"] == "pw":
                    self.state["pw"] = request["pd"]["pw"]
                    if request["pd"]["br"] != 255:
                        self.state["br"] = request["pd"]["br"]
                else:
                    self.state.update(
                        {k: v for k, v in request["pd"].items() if k != "pw"}
                    )
            return
        if self.drop:
            return
        await self.queue.put(
            SimpleNamespace(
                topic=self.subscriptions[self.reply_topic],
                retain=False,
                payload=json.dumps(
                    {"hd": {**request["hd"], "np": "NT"}, "pd": self.state}
                ).encode(),
            )
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("reply_topic", [0, 1])
async def test_startup_reads_confirmed_complete_state(monkeypatch, reply_topic):
    broker = Broker()
    broker.reply_topic = reply_topic
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    reports = []
    bulb = NativeBulbClient(
        "AA:BB:CC:DD:EE:FF",
        123,
        ssl.create_default_context(),
        reports.append,
        timeout=0.03,
    )
    try:
        await bulb.async_start()
        assert bulb.state == STATE
        assert reports[-1] == STATE
        assert broker.requests[0]["hd"]["np"] == "FC"
        assert broker.requests[0]["hd"]["na"] == "sy"
        assert 0 <= broker.requests[0]["hd"]["sd"] < 86400
    finally:
        await bulb.async_stop()


@pytest.mark.asyncio
async def test_missing_readback_marks_unavailable_and_recovers(monkeypatch):
    broker = Broker()
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    bulb = NativeBulbClient(
        "bulb", 123, ssl.create_default_context(), lambda s: None, timeout=0.03
    )
    try:
        await bulb.async_start()
        broker.drop = True
        with pytest.raises(XthingsCloudApiError):
            await bulb.async_refresh()
        assert bulb.state is None
        broker.drop = False
        broker.state["tp"] = 100
        assert (await bulb.async_refresh())["tp"] == 100
    finally:
        await bulb.async_stop()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_reply",
    [
        {"retain": True},
        {"md": -1},
        {"state": {"tp": 47}},
        {"state": {**STATE, "tp": "47"}},
        {"np": "CC"},
    ],
)
async def test_bad_or_stale_reply_cannot_confirm_state(monkeypatch, bad_reply):
    broker = Broker()
    broker.drop = True
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    bulb = NativeBulbClient(
        "bulb", 123, ssl.create_default_context(), lambda s: None, timeout=0.03
    )
    try:
        task = asyncio.create_task(bulb.async_start())
        while not broker.requests:
            await asyncio.sleep(0)
        request = broker.requests[-1]
        await broker.queue.put(
            SimpleNamespace(
                topic=broker.subscriptions[1],
                retain=bad_reply.get("retain", False),
                payload=json.dumps(
                    {
                        "hd": {
                            **request["hd"],
                            "np": bad_reply.get("np", "NT"),
                            "md": bad_reply.get("md", request["hd"]["md"]),
                        },
                        "pd": bad_reply.get("state", STATE),
                    }
                ).encode(),
            )
        )
        await task
        assert bulb.state is None
    finally:
        await bulb.async_stop()


@pytest.mark.asyncio
async def test_notification_triggers_full_read_instead_of_partial_overwrite(
    monkeypatch,
):
    broker = Broker()
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    changed = asyncio.Event()
    bulb = NativeBulbClient(
        "bulb", 123, ssl.create_default_context(), lambda s: changed.set()
    )
    try:
        await bulb.async_start()
        changed.clear()
        broker.state["tp"] = 100
        await broker.queue.put(
            SimpleNamespace(
                topic=broker.subscriptions[0],
                retain=False,
                payload=json.dumps(
                    {"hd": {"np": "NT", "na": "cs"}, "pd": {"tp": 1}}
                ).encode(),
            )
        )
        await asyncio.wait_for(changed.wait(), 2)
        assert bulb.state == {**STATE, "tp": 100}
    finally:
        await bulb.async_stop()


@pytest.mark.asyncio
async def test_disconnect_resubscribes_and_confirms_new_state(monkeypatch):
    broker = Broker()
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    reports = []
    bulb = NativeBulbClient("bulb", 123, ssl.create_default_context(), reports.append)
    try:
        await bulb.async_start()
        broker.state["tp"] = 100
        await broker.queue.put(aiomqtt.MqttError("connection lost"))
        async with asyncio.timeout(3):
            while bulb.state != {**STATE, "tp": 100}:
                await asyncio.sleep(0.01)
        assert None in reports
        assert broker.connections == 2
    finally:
        await bulb.async_stop()


@pytest.mark.asyncio
async def test_ignored_command_does_not_report_requested_state(monkeypatch):
    broker = Broker()
    broker.ignore_commands = True
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    bulb = NativeBulbClient("bulb", 123, ssl.create_default_context(), lambda s: None)
    try:
        await bulb.async_start()
        with pytest.raises(XthingsCloudApiError, match="did not confirm"):
            await bulb.async_set_state({"tp": 100})
        assert bulb.state == STATE
    finally:
        await bulb.async_stop()


@pytest.mark.asyncio
async def test_control_preserves_other_settings_and_reads_back(monkeypatch):
    broker = Broker()
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    bulb = NativeBulbClient(
        "AA:BB:CC:DD:EE:FF", 123, ssl.create_default_context(), lambda s: None
    )
    try:
        await bulb.async_start()
        broker.state["br"] = 63
        state = await bulb.async_set_state({"tp": 100})
        assert state == {**STATE, "br": 63, "tp": 100}
        assert broker.requests[-2]["hd"]["na"] == "se"
        assert broker.requests[-1]["hd"]["na"] == "sy"
    finally:
        await bulb.async_stop()


@pytest.mark.asyncio
async def test_shutdown_releases_inflight_read(monkeypatch):
    broker = Broker()
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    reports = []
    bulb = NativeBulbClient("bulb", 123, ssl.create_default_context(), reports.append)
    await bulb.async_start()
    broker.drop = True
    previous = len(broker.requests)
    read = asyncio.create_task(bulb.async_refresh())
    async with asyncio.timeout(1):
        while len(broker.requests) == previous:
            await asyncio.sleep(0)
        await bulb.async_stop()
        with pytest.raises(XthingsCloudApiError):
            await read
    assert bulb.state is None
    assert reports[-1] is None


def test_bundled_tls_credentials_load_with_server_verification():
    from ha_xthings_cloud.bulb import create_bulb_ssl_context

    context = create_bulb_ssl_context()
    assert context.check_hostname is True
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.cert_store_stats()["x509_ca"] > 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "initial_power,changes",
    [
        (1, {"pw": 0}),
        (0, {"pw": 1}),
        (0, {"pw": 1, "br": 42, "ct": 1, "tp": 100}),
    ],
)
async def test_power_control_preserves_settings(monkeypatch, initial_power, changes):
    broker = Broker()
    broker.state["pw"] = initial_power
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    bulb = NativeBulbClient("bulb", 123, ssl.create_default_context(), lambda s: None)
    try:
        await bulb.async_start()
        assert await bulb.async_set_state(changes) == {**STATE, **changes}
        power = [r for r in broker.requests if r["hd"]["na"] == "pw"]
        assert len(power) == 1
        assert power[0]["pd"] == {"pw": changes["pw"], "br": changes.get("br", 255)}
    finally:
        await bulb.async_stop()
