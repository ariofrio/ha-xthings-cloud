"""Confirmed native MQTT state for the U-tec A19-C1.

The bundled app credential authenticates native MQTT; it is shared, not per user.
"""

from __future__ import annotations

import asyncio
import json
import secrets
import ssl
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from importlib.resources import as_file, files

import aiomqtt

from .exceptions import XthingsCloudApiError

SUPPORTED_MODELS = frozenset({"A19-C1"})
BROKER = "a30xqtffg389ek-ats.iot.us-west-2.amazonaws.com"
RANGES = {
    "pw": (0, 1),
    "br": (1, 100),
    "ct": (0, 1),
    "tp": (0, 100),
    "hu": (0, 360),
    "sa": (0, 100),
    "li": (0, 100),
}


@dataclass(frozen=True)
class NativeBulbRoute:
    """Address command route and optional group-specific response route."""

    address_id: int
    group_id: str | None = None


def create_bulb_ssl_context() -> ssl.SSLContext:
    """Load the bundled app identity with normal server/hostname verification.

    This performs file I/O; async callers should run it in an executor.
    """
    context = ssl.create_default_context()
    resources = files("ha_xthings_cloud").joinpath("certs")
    with (
        as_file(resources.joinpath("client-cert.pem")) as certificate,
        as_file(resources.joinpath("client-key.pem")) as private_key,
    ):
        context.load_cert_chain(certificate, private_key)
    return context


def _valid_state(state: object) -> bool:
    return isinstance(state, dict) and all(
        type(state.get(key)) is int and low <= state[key] <= high
        for key, (low, high) in RANGES.items()
    )


class NativeBulbClient:
    """One bulb connection, with resubscription and device-dependent readback.

    The state callback receives a complete native state or None when freshness
    cannot be confirmed. All callbacks run on the caller's asyncio event loop.
    """

    def __init__(
        self,
        device_id: str,
        address_id: int | NativeBulbRoute,
        tls_context: ssl.SSLContext,
        on_state: Callable[[dict[str, int] | None], None],
        *,
        timeout: float = 5,
        poll_interval: float = 30,
    ) -> None:
        if any(c in device_id for c in "/+#") or not device_id:
            raise ValueError("Invalid device ID")
        if (
            not tls_context.check_hostname
            or tls_context.verify_mode != ssl.CERT_REQUIRED
        ):
            raise ValueError(
                "MQTT requires server certificate and hostname verification"
            )
        route = (
            address_id
            if isinstance(address_id, NativeBulbRoute)
            else NativeBulbRoute(address_id)
        )
        if route.group_id is not None and (
            not route.group_id or any(c in route.group_id for c in "/+#")
        ):
            raise ValueError("Invalid group ID")
        self._device_id = device_id
        self._tls = tls_context
        self._on_state = on_state
        self._timeout = timeout
        self._poll_interval = poll_interval
        self._sid = secrets.randbelow(86400)
        prefix = (
            f"utec/lightness/group/{route.group_id}/{device_id}"
            if route.group_id is not None
            else f"utec/lightness/only/{device_id}"
        )
        self._notify = f"{prefix}/notify"
        self._accept = f"{prefix}/accept/{self._sid}"
        self._command = f"utec/lightness/{route.address_id}/{device_id}/command"
        self._client: aiomqtt.Client | None = None
        self._task: asyncio.Task | None = None
        self._connected = asyncio.Event()
        self._initial = asyncio.Event()
        self._changed = asyncio.Event()
        self._lock = asyncio.Lock()
        self._pending: dict[int, asyncio.Future] = {}
        self._state: dict[str, int] | None = None

    @property
    def state(self) -> dict[str, int] | None:
        """Return a copy of the latest confirmed state, or None if unavailable."""
        return dict(self._state) if self._state is not None else None

    def _set_state(self, state: dict[str, int] | None) -> None:
        self._state = state
        self._on_state(self.state)

    async def async_start(self) -> None:
        """Start recovery and health polling; await the first bounded attempt."""
        if self._task is not None:
            return
        self._initial.clear()
        self._task = asyncio.create_task(self._run(), name="xthings-bulb")
        with suppress(TimeoutError):
            await asyncio.wait_for(self._initial.wait(), self._timeout * 2)

    async def async_stop(self) -> None:
        """Close the connection and all background tasks."""
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _run(self) -> None:
        delay = 1
        while True:
            monitor = None
            try:
                async with aiomqtt.Client(
                    hostname=BROKER,
                    port=8883,
                    tls_context=self._tls,
                    identifier="app_" + secrets.token_hex(12),
                    clean_session=True,
                    keepalive=30,
                    timeout=self._timeout,
                ) as client:
                    self._client = client
                    await client.subscribe(self._notify, qos=1)
                    await client.subscribe(self._accept, qos=1)
                    self._connected.set()
                    delay = 1
                    monitor = asyncio.create_task(self._monitor())
                    async for message in client.messages:
                        self._receive(
                            str(message.topic), bytes(message.payload), message.retain
                        )
            except (aiomqtt.MqttError, OSError):
                pass
            finally:
                self._connected.clear()
                self._client = None
                self._set_state(None)
                for future in self._pending.values():
                    if not future.done():
                        future.set_exception(XthingsCloudApiError("MQTT disconnected"))
                if monitor is not None:
                    monitor.cancel()
                    with suppress(asyncio.CancelledError):
                        await monitor
                self._initial.set()
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)

    def _receive(self, topic: str, payload: bytes, retained: bool) -> None:
        if retained or topic not in (self._accept, self._notify):
            return
        try:
            message = json.loads(payload)
        except (ValueError, UnicodeDecodeError):
            return
        if not isinstance(message, dict):
            return
        header, state = message.get("hd"), message.get("pd")
        if not isinstance(header, dict) or header.get("np") != "NT":
            return
        if header.get("na") == "sy":
            mid = header.get("md")
            if type(mid) is not int:
                return
            future = self._pending.get(mid)
            if future is not None and not future.done() and _valid_state(state):
                future.set_result({k: state[k] for k in RANGES})
        elif (
            topic == self._notify
            and isinstance(state, dict)
            and RANGES.keys() & state.keys()
        ):
            # A notification is a hint to query; partial or delayed values never
            # overwrite a newer complete state or prove startup availability.
            self._changed.set()

    async def _monitor(self) -> None:
        while True:
            self._changed.clear()
            with suppress(XthingsCloudApiError):
                await self.async_refresh()
            self._initial.set()
            with suppress(TimeoutError):
                await asyncio.wait_for(self._changed.wait(), self._poll_interval)
            await asyncio.sleep(0.1)

    async def async_refresh(self) -> dict[str, int]:
        """Request fresh state; HTTP cache and MQTT retained messages are excluded."""
        async with self._lock:
            return await self._sync()

    async def async_set_state(self, changes: dict[str, int]) -> dict[str, int]:
        """Apply native settings, preserving others, and confirm with fresh reads."""
        if not changes or not changes.keys() <= RANGES.keys():
            raise ValueError("Unknown or empty bulb settings")
        for key, value in changes.items():
            low, high = RANGES[key]
            if type(value) is not int or not low <= value <= high:
                raise ValueError(f"Invalid {key} setting")
        async with self._lock:
            state = await self._sync()
            desired = {**state, **changes}
            try:
                if changes.keys() - {"pw", "br"}:
                    await self._publish(
                        "CC", "se", desired, secrets.randbelow(2**31 - 1) + 1
                    )
                if "pw" in changes or "br" in changes:
                    # Scene commands do not reliably change power on A19-C1.
                    await self._publish(
                        "CC",
                        "pw",
                        {"pw": desired["pw"], "br": changes.get("br", 255)},
                        secrets.randbelow(2**31 - 1) + 1,
                    )
            except aiomqtt.MqttError as err:
                self._set_state(None)
                raise XthingsCloudApiError("Bulb command failed") from err
            for _ in range(3):
                state = await self._sync()
                if all(state[k] == v for k, v in changes.items()):
                    return state
                await asyncio.sleep(0.2)
            raise XthingsCloudApiError("Bulb did not confirm the requested settings")

    async def _sync(self) -> dict[str, int]:
        mid = secrets.randbelow(2**31 - 1) + 1
        future = asyncio.get_running_loop().create_future()
        try:
            await asyncio.wait_for(self._connected.wait(), self._timeout)
            self._pending[mid] = future
            await self._publish("FC", "sy", {}, mid)
            state = await asyncio.wait_for(future, self._timeout)
        except (TimeoutError, aiomqtt.MqttError, XthingsCloudApiError) as err:
            self._set_state(None)
            raise XthingsCloudApiError("No confirmed response from bulb") from err
        finally:
            self._pending.pop(mid, None)
            if not future.done():
                future.cancel()
        self._set_state(state)
        return dict(state)

    async def _publish(self, namespace: str, name: str, state: dict, mid: int) -> None:
        client = self._client
        if client is None:
            raise XthingsCloudApiError("MQTT disconnected")
        await client.publish(
            self._command,
            json.dumps(
                {
                    "hd": {
                        "np": namespace,
                        "na": name,
                        "md": mid,
                        "pv": "3",
                        "sd": self._sid,
                    },
                    "pd": state,
                }
            ),
            qos=1,
            retain=False,
        )
