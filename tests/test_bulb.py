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
        self.lost_reads_after_command = 0
        self.lost_reads_remaining = 0
        self.reply_delays = []

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
            self.lost_reads_remaining = self.lost_reads_after_command
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
        if self.lost_reads_remaining:
            self.lost_reads_remaining -= 1
            request["hd"]["md"] = -1
        reply = SimpleNamespace(
            topic=self.subscriptions[self.reply_topic],
            retain=False,
            payload=json.dumps(
                {"hd": {**request["hd"], "np": "NT"}, "pd": self.state}
            ).encode(),
        )
        if self.reply_delays and (delay := self.reply_delays.pop(0)):
            asyncio.get_running_loop().call_later(delay, self.queue.put_nowait, reply)
            return
        await self.queue.put(reply)


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
    reports = []
    bulb = NativeBulbClient(
        "bulb", 123, ssl.create_default_context(), reports.append, timeout=0.03
    )
    try:
        await bulb.async_start()
        broker.drop = True
        for _ in range(2):
            with pytest.raises(XthingsCloudApiError):
                await bulb.async_refresh()
            # Isolated lost replies must not flap availability.
            assert bulb.state == STATE
        assert None not in reports
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
async def test_scene_control_uses_confirmed_state_without_read(monkeypatch):
    broker = Broker()
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    bulb = NativeBulbClient(
        "AA:BB:CC:DD:EE:FF", 123, ssl.create_default_context(), lambda s: None
    )
    try:
        await bulb.async_start()
        broker.requests.clear()
        state = await bulb.async_set_state({"ct": 1, "tp": 100})
        assert state == {**STATE, "tp": 100}
        assert [r["hd"]["na"] for r in broker.requests] == ["se", "sy"]
        assert broker.requests[0]["pd"] == {**STATE, "tp": 100}
    finally:
        await bulb.async_stop()


@pytest.mark.asyncio
async def test_scene_control_reads_first_without_confirmed_state(monkeypatch):
    broker = Broker()
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    bulb = NativeBulbClient(
        "AA:BB:CC:DD:EE:FF",
        123,
        ssl.create_default_context(),
        lambda s: None,
        timeout=0.05,
    )
    try:
        await bulb.async_start()
        broker.drop = True
        for _ in range(3):
            with pytest.raises(XthingsCloudApiError):
                await bulb.async_refresh()
        assert bulb.state is None
        broker.drop = False
        broker.state["br"] = 63
        broker.requests.clear()
        state = await bulb.async_set_state({"tp": 100})
        assert state == {**STATE, "br": 63, "tp": 100}
        assert [r["hd"]["na"] for r in broker.requests] == ["sy", "se", "sy"]
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


@pytest.mark.asyncio
@pytest.mark.parametrize("lost_reads", [1, 2])
async def test_command_retries_lost_confirmation_without_resending(
    monkeypatch, lost_reads
):
    broker = Broker()
    broker.lost_reads_after_command = lost_reads
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    reports = []
    bulb = NativeBulbClient(
        "bulb", 123, ssl.create_default_context(), reports.append, timeout=0.03
    )
    try:
        await bulb.async_start()
        reports.clear()
        state = await bulb.async_set_state({"pw": 1, "br": 15, "ct": 1, "tp": 11})
        assert state == {**STATE, "br": 15, "tp": 11}
        assert None not in reports
        assert reports[-1] == state
        commands = [r for r in broker.requests if r["hd"]["np"] == "CC"]
        # The bulb is already on, so the confirmed scene needs no power command.
        assert [r["hd"]["na"] for r in commands] == ["se"]
    finally:
        await bulb.async_stop()


@pytest.mark.asyncio
async def test_command_without_any_confirmation_stays_unavailable(monkeypatch):
    broker = Broker()
    broker.lost_reads_after_command = 100
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    bulb = NativeBulbClient(
        "bulb", 123, ssl.create_default_context(), lambda s: None, timeout=0.03
    )
    try:
        await bulb.async_start()
        with pytest.raises(XthingsCloudApiError, match="No confirmed response"):
            await bulb.async_set_state({"pw": 1, "br": 15, "ct": 1, "tp": 11})
        assert bulb.state is None
        assert len([r for r in broker.requests if r["hd"]["np"] == "CC"]) == 1
    finally:
        await bulb.async_stop()


@pytest.mark.asyncio
async def test_combined_control_confirms_scene_before_power(monkeypatch):
    broker = Broker()
    broker.state["pw"] = 0
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    bulb = NativeBulbClient("bulb", 123, ssl.create_default_context(), lambda s: None)
    try:
        await bulb.async_start()
        result = await bulb.async_set_state({"pw": 1, "br": 15, "ct": 1, "tp": 11})
        assert result == {**STATE, "br": 15, "tp": 11}
        names = [r["hd"]["na"] for r in broker.requests]
        assert names[names.index("se") : names.index("pw") + 1] == ["se", "sy", "pw"]
    finally:
        await bulb.async_stop()


@pytest.mark.asyncio
async def test_shutdown_during_publish_does_not_leak_future_exception(monkeypatch):
    publishing = asyncio.Event()
    release = asyncio.Event()

    class SlowPublishBroker(Broker):
        async def publish(self, *args, **kwargs):
            publishing.set()
            await release.wait()

    broker = SlowPublishBroker()
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    unhandled = []
    loop.set_exception_handler(lambda loop, context: unhandled.append(context))
    bulb = NativeBulbClient(
        "bulb", 123, ssl.create_default_context(), lambda s: None, timeout=0.03
    )
    try:
        startup = asyncio.create_task(bulb.async_start())
        await asyncio.wait_for(publishing.wait(), 1)
        await bulb.async_stop()
        await startup
        await asyncio.sleep(0)
        assert not unhandled
        assert bulb.state is None
    finally:
        await bulb.async_stop()
        loop.set_exception_handler(previous)


@pytest.mark.asyncio
async def test_overlapping_controls_preserve_each_change(monkeypatch):
    broker = Broker()
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    bulb = NativeBulbClient("bulb", 123, ssl.create_default_context(), lambda s: None)
    try:
        await bulb.async_start()
        results = await asyncio.gather(
            bulb.async_set_state({"br": 42}),
            bulb.async_set_state({"pw": 1, "ct": 1, "tp": 100}),
            bulb.async_set_state({"pw": 0}),
        )
        assert results == [
            {**STATE, "br": 42, "tp": 100},
            {**STATE, "br": 42, "tp": 100},
            {**STATE, "br": 42, "tp": 100, "pw": 0},
        ]
        assert await bulb.async_refresh() == {**STATE, "br": 42, "tp": 100, "pw": 0}
    finally:
        await bulb.async_stop()


@pytest.mark.asyncio
async def test_complete_power_command_needs_only_confirmation(monkeypatch):
    broker = Broker()
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    bulb = NativeBulbClient("bulb", 123, ssl.create_default_context(), lambda s: None)
    try:
        await bulb.async_start()
        broker.requests.clear()
        assert await bulb.async_set_state({"pw": 1, "br": 18}) == {**STATE, "br": 18}
        assert [r["hd"]["na"] for r in broker.requests] == ["pw", "sy"]
    finally:
        await bulb.async_stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_waiter", [False, True])
async def test_slider_burst_coalesces_pending_changes(monkeypatch, cancel_waiter):
    entered, release = asyncio.Event(), asyncio.Event()

    class SlowCommandBroker(Broker):
        async def publish(self, topic, payload, **kwargs):
            if json.loads(payload)["hd"]["np"] == "CC" and not entered.is_set():
                entered.set()
                await release.wait()
            await super().publish(topic, payload, **kwargs)

    broker = SlowCommandBroker()
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    bulb = NativeBulbClient("bulb", 123, ssl.create_default_context(), lambda s: None)
    tasks = []
    try:
        await bulb.async_start()
        tasks.append(asyncio.create_task(bulb.async_set_state({"pw": 1, "br": 10})))
        await asyncio.wait_for(entered.wait(), 1)
        for brightness in range(11, 21):
            tasks.append(
                asyncio.create_task(bulb.async_set_state({"pw": 1, "br": brightness}))
            )
            await asyncio.sleep(0)
        tasks.append(
            asyncio.create_task(bulb.async_set_state({"pw": 1, "ct": 1, "tp": 70}))
        )
        await asyncio.sleep(0)
        if cancel_waiter:
            tasks[1].cancel()
        release.set()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        assert results[0] == {**STATE, "br": 10}
        for result in results[2 if cancel_waiter else 1 :]:
            assert result == {**STATE, "br": 20, "tp": 70}
        assert await bulb.async_refresh() == {**STATE, "br": 20, "tp": 70}
        commands = [r for r in broker.requests if r["hd"]["np"] == "CC"]
        assert [r["hd"]["na"] for r in commands] == ["pw", "se"]
    finally:
        release.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        await bulb.async_stop()


@pytest.mark.asyncio
async def test_power_changes_are_ordering_barriers(monkeypatch):
    broker = Broker()
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    bulb = NativeBulbClient("bulb", 123, ssl.create_default_context(), lambda s: None)
    try:
        await bulb.async_start()
        results = await asyncio.gather(
            bulb.async_set_state({"pw": 1, "br": 15}),
            bulb.async_set_state({"pw": 0}),
            bulb.async_set_state({"pw": 1, "br": 20}),
        )
        assert results == [
            {**STATE, "br": 15},
            {**STATE, "br": 15, "pw": 0},
            {**STATE, "br": 20},
        ]
        assert [r["pd"]["pw"] for r in broker.requests if r["hd"]["na"] == "pw"] == [
            1,
            0,
            1,
        ]
    finally:
        await bulb.async_stop()


@pytest.mark.asyncio
async def test_recent_confirmation_defers_health_poll(monkeypatch):
    broker = Broker()
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    bulb = NativeBulbClient(
        "bulb", 123, ssl.create_default_context(), lambda s: None, poll_interval=0.6
    )
    try:
        await bulb.async_start()
        await asyncio.sleep(0.4)
        await bulb.async_set_state({"pw": 1, "br": 15})
        broker.requests.clear()
        await asyncio.sleep(0.4)
        assert broker.requests == []
        await asyncio.sleep(0.4)
        assert [r["hd"]["na"] for r in broker.requests] == ["sy"]
    finally:
        await bulb.async_stop()


@pytest.mark.asyncio
async def test_failed_health_poll_retries_before_marking_unavailable(monkeypatch):
    broker = Broker()
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    bulb = NativeBulbClient(
        "bulb",
        123,
        ssl.create_default_context(),
        lambda s: None,
        timeout=0.03,
        poll_interval=0.2,
        retry_interval=0.05,
    )
    try:
        await bulb.async_start()
        broker.drop = True
        async with asyncio.timeout(1):
            while bulb.state is not None:
                await asyncio.sleep(0.01)
        polls = [r for r in broker.requests if r["hd"]["na"] == "sy"]
        assert len(polls) == 4
    finally:
        await bulb.async_stop()


@pytest.mark.asyncio
async def test_lost_reply_is_recovered_by_backup_query(monkeypatch):
    broker = Broker()
    broker.lost_reads_after_command = 1
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    reports = []
    bulb = NativeBulbClient(
        "bulb",
        123,
        ssl.create_default_context(),
        reports.append,
        timeout=2,
        backup_query_delay=0.05,
    )
    try:
        await bulb.async_start()
        broker.requests.clear()
        loop = asyncio.get_running_loop()
        start = loop.time()
        assert await bulb.async_set_state({"pw": 1, "br": 15}) == {**STATE, "br": 15}
        # Recovered by the backup query, not by waiting out the timeout.
        assert loop.time() - start < 1
        assert [r["hd"]["na"] for r in broker.requests] == ["pw", "sy", "sy"]
        assert None not in reports
        assert not bulb._pending
    finally:
        await bulb.async_stop()


@pytest.mark.asyncio
async def test_slow_first_reply_still_confirms_after_backup_query(monkeypatch):
    broker = Broker()
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    bulb = NativeBulbClient(
        "bulb",
        123,
        ssl.create_default_context(),
        lambda s: None,
        timeout=2,
        backup_query_delay=0.05,
    )
    try:
        await bulb.async_start()
        broker.requests.clear()
        # The first reply is slow and the backup reply never arrives.
        broker.reply_delays = [0.1]
        broker.lost_reads_remaining = 0
        original_publish = broker.publish

        async def drop_backup(topic, payload, **kwargs):
            if len(broker.requests) == 1:
                broker.requests.append(json.loads(payload))
                return
            await original_publish(topic, payload, **kwargs)

        monkeypatch.setattr(broker, "publish", drop_backup)
        assert await bulb.async_refresh() == STATE
        assert [r["hd"]["na"] for r in broker.requests] == ["sy", "sy"]
        assert not bulb._pending
    finally:
        await bulb.async_stop()


@pytest.mark.asyncio
async def test_prompt_reply_needs_no_backup_query(monkeypatch):
    broker = Broker()
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    bulb = NativeBulbClient(
        "bulb",
        123,
        ssl.create_default_context(),
        lambda s: None,
        backup_query_delay=0.2,
    )
    try:
        await bulb.async_start()
        broker.requests.clear()
        assert await bulb.async_refresh() == STATE
        await asyncio.sleep(0.3)
        assert [r["hd"]["na"] for r in broker.requests] == ["sy"]
    finally:
        await bulb.async_stop()


class BlockFirstCommandBroker(Broker):
    """Hold the first command so that later commands queue behind it."""

    def __init__(self):
        super().__init__()
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        self.fail_later_commands = False

    async def publish(self, topic, payload, **kwargs):
        header = json.loads(payload)["hd"]
        if header["np"] == "CC":
            if not self.entered.is_set():
                self.entered.set()
                await self.release.wait()
            elif self.fail_later_commands:
                raise aiomqtt.MqttError("publish failed")
        await super().publish(topic, payload, **kwargs)


@pytest.mark.asyncio
async def test_superseded_confirmation_is_not_reported(monkeypatch):
    broker = BlockFirstCommandBroker()
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    reports = []
    bulb = NativeBulbClient("bulb", 123, ssl.create_default_context(), reports.append)
    try:
        await bulb.async_start()
        reports.clear()
        first = asyncio.create_task(bulb.async_set_state({"pw": 1, "br": 10}))
        await asyncio.wait_for(broker.entered.wait(), 1)
        second = asyncio.create_task(bulb.async_set_state({"pw": 1, "br": 69}))
        await asyncio.sleep(0)
        # A power change is never merged, so it queues behind the first command.
        third = asyncio.create_task(bulb.async_set_state({"pw": 0}))
        await asyncio.sleep(0)
        broker.release.set()
        await asyncio.gather(first, second, third)
        # Each earlier confirmation was already superseded; HA sees only the last.
        assert reports == [{**STATE, "br": 69, "pw": 0}]
    finally:
        broker.release.set()
        await bulb.async_stop()


@pytest.mark.asyncio
async def test_held_state_is_reported_when_queued_command_fails(monkeypatch):
    broker = BlockFirstCommandBroker()
    broker.fail_later_commands = True
    monkeypatch.setattr("ha_xthings_cloud.bulb.aiomqtt.Client", broker.client)
    reports = []
    bulb = NativeBulbClient("bulb", 123, ssl.create_default_context(), reports.append)
    try:
        await bulb.async_start()
        reports.clear()
        first = asyncio.create_task(bulb.async_set_state({"pw": 1, "br": 10}))
        await asyncio.wait_for(broker.entered.wait(), 1)
        second = asyncio.create_task(bulb.async_set_state({"pw": 0}))
        await asyncio.sleep(0)
        broker.release.set()
        assert await first == {**STATE, "br": 10}
        with pytest.raises(XthingsCloudApiError, match="command failed"):
            await second
        # The failed command cannot leave HA showing a value never confirmed.
        assert reports == [{**STATE, "br": 10}]
    finally:
        broker.release.set()
        await bulb.async_stop()
