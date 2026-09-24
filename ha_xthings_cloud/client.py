"""Xthings Cloud API client."""

from __future__ import annotations

import json
import logging
import time
from typing import Any

import aiohttp
import jwt

from .const import (
    API_BRITE_BRIGHTNESS,
    API_BRITE_COLOR,
    API_BRITE_OFF,
    API_BRITE_ON,
    API_CAMERA_WEBRTC,
    API_DEVICE_STATUS,
    API_DEVICES,
    API_FRP_HTTP,
    API_LOCK_LOCK,
    API_LOCK_UNLOCK,
    API_LOGIN,
    API_PLUG_OFF,
    API_PLUG_ON,
    API_REFRESH_TOKEN,
    API_SWITCH_BRIGHTNESS,
    API_SWITCH_OFF,
    API_SWITCH_ON,
    AUTH_ERROR_CODES,
)
from .exceptions import XthingsCloudApiError, XthingsCloudAuthError
from .bulb import SUPPORTED_MODELS, NativeBulbRoute

_LOGGER = logging.getLogger(__name__)


class XthingsCloudApiClient:
    """Async client for Xthings Cloud API.

    Args:
        session: aiohttp.ClientSession instance (injected, not created internally).
        token: Optional pre-existing auth token.
    """

    def __init__(
        self,
        session: aiohttp.ClientSession,
        token: str | None = None,
    ) -> None:
        self._session = session
        self._token = token

    @property
    def token(self) -> str | None:
        """Return current auth token."""
        return self._token

    @token.setter
    def token(self, value: str | None) -> None:
        """Set auth token."""
        self._token = value

    def _headers(self) -> dict[str, str]:
        """Build request headers with auth token."""
        headers = {"Content-Type": "application/json"}
        if self._token:
            headers["x-token"] = self._token
        return headers

    async def _request(
        self,
        url: str,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Send POST request and parse response.

        Xthings API always uses POST and returns {"code": int, "data": ...}.
        Success is indicated by code=200 in the response body (not HTTP status).
        """
        if headers is None:
            headers = self._headers()

        _LOGGER.debug("Request: POST %s, body=%s", url, json)

        try:
            resp = await self._session.request("POST", url, json=json, headers=headers)
            resp.raise_for_status()
            result = await resp.json()
        except aiohttp.ClientError as err:
            raise XthingsCloudApiError(f"Request failed: {err}") from err

        code = result.get("code")
        data = result.get("data", {})

        _LOGGER.debug("Response: POST %s, code=%s", url, code)

        if code == 200:
            return data

        if code in AUTH_ERROR_CODES:
            raise XthingsCloudAuthError(f"Auth failed (code={code})", code=code)

        raise XthingsCloudApiError(f"API error (code={code})", code=code)

    # ---- Auth ----

    def is_token_expired(self) -> bool:
        """Check if the current token is expired or about to expire (within 60s)."""
        if not self._token:
            return True
        try:
            payload = jwt.decode(
                self._token, options={"verify_signature": False}
            )
            return payload.get("exp", 0) < time.time() + 60
        except jwt.DecodeError:
            return True

    async def async_login(
        self,
        email: str,
        password: str,
        client_id: str | None = None,
    ) -> dict[str, Any]:
        """Login and obtain token.

        Returns {"token": str, "refresh_token": str, "client_id": str, "user_id": str} on success.
        """
        payload: dict[str, Any] = {"email": email, "password": password}
        if client_id:
            payload["client_id"] = client_id

        data = await self._request(
            API_LOGIN, json=payload,
            headers={"Content-Type": "application/json"},
        )

        self._token = data["token"]
        return {
            "token": data["token"],
            "refresh_token": data.get("refresh_token", ""),
            "client_id": data.get("client_id", ""),
            "user_id": data["user_id"],
        }

    async def async_refresh_token(self, refresh_token: str) -> dict[str, Any]:
        """Refresh auth token using refresh_token."""
        data = await self._request(
            API_REFRESH_TOKEN,
            json={"refresh_token": refresh_token},
            headers={"Content-Type": "application/json"},
        )
        self._token = data["token"]
        return {
            "token": data["token"],
            "refresh_token": data.get("refresh_token", ""),
        }

    # ---- Devices ----

    async def async_get_devices(self) -> list[dict[str, Any]]:
        """Get device list."""
        data = await self._request(API_DEVICES)
        return data.get("devices", [])

    async def async_get_device_status(self, device_id: str) -> dict[str, Any]:
        """Get single device status."""
        return await self._request(API_DEVICE_STATUS, json={"id": device_id})

    async def async_get_native_bulb_routes(self) -> dict[str, NativeBulbRoute]:
        """Discover supported bulb routing via the authenticated app API.

        Return command addresses and group response routes; discard other metadata.
        """
        async def request(path: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
            try:
                response = await self._session.request(
                    "POST", f"https://cloud.u-tec.com/app/{path}",
                    data={"token": self._token,
                          "data": json.dumps({**payload, "timestamp": str(time.time())})},
                    timeout=aiohttp.ClientTimeout(total=15), allow_redirects=False,
                )
                response.raise_for_status()
                result = await response.json()
            except (aiohttp.ClientError, TimeoutError, ValueError) as err:
                raise XthingsCloudApiError("Native device discovery failed") from err
            code = result.get("code")
            if code in AUTH_ERROR_CODES:
                raise XthingsCloudAuthError("Native discovery authentication failed", code)
            if code != 200 or not isinstance(result.get("data"), list):
                raise XthingsCloudApiError("Invalid native discovery response")
            return result["data"]

        routes = {}
        for address in await request("address", {}):
            for room in await request("room", {"id": address["id"]}):
                for device in await request("device/list", {"room_id": room["id"]}):
                    if device.get("model") in SUPPORTED_MODELS:
                        routes[device["uuid"]] = NativeBulbRoute(address["id"])
                    elif device.get("entry_type") == "Britegroup":
                        for bulb in device.get("lights", []):
                            if bulb.get("model") in SUPPORTED_MODELS:
                                routes[bulb["uuid"]] = NativeBulbRoute(
                                    address["id"], device["uuid"]
                                )
        return routes

    # ---- Switch ----

    async def async_switch_on(self, device_id: str) -> dict[str, Any]:
        """Turn on switch."""
        return await self._request(API_SWITCH_ON, json={"uuid": device_id})

    async def async_switch_off(self, device_id: str) -> dict[str, Any]:
        """Turn off switch."""
        return await self._request(API_SWITCH_OFF, json={"uuid": device_id})

    async def async_switch_brightness(
        self, device_id: str, brightness: int
    ) -> dict[str, Any]:
        """Set switch brightness (0-100)."""
        return await self._request(
            API_SWITCH_BRIGHTNESS, json={"uuid": device_id, "brightness": brightness}
        )

    # ---- Plug ----

    async def async_plug_on(self, device_id: str) -> dict[str, Any]:
        """Turn on plug."""
        return await self._request(API_PLUG_ON, json={"uuid": device_id})

    async def async_plug_off(self, device_id: str) -> dict[str, Any]:
        """Turn off plug."""
        return await self._request(API_PLUG_OFF, json={"uuid": device_id})

    # ---- Light (Brite) ----

    async def async_brite_on(self, device_id: str) -> dict[str, Any]:
        """Turn on light."""
        return await self._request(API_BRITE_ON, json={"uuid": device_id})

    async def async_brite_off(self, device_id: str) -> dict[str, Any]:
        """Turn off light."""
        return await self._request(API_BRITE_OFF, json={"uuid": device_id})

    async def async_brite_brightness(
        self, device_id: str, brightness: int
    ) -> dict[str, Any]:
        """Set light brightness (0-100)."""
        return await self._request(
            API_BRITE_BRIGHTNESS, json={"uuid": device_id, "brightness": brightness}
        )

    async def async_brite_color(
        self, device_id: str, color: dict[str, Any]
    ) -> dict[str, Any]:
        """Set light color.

        Args:
            color: Dict with keys like colortype, hue, saturation, lightness,
                   brightness, temperature depending on color mode.
        """
        return await self._request(
            API_BRITE_COLOR, json={"uuid": device_id, "color": color}
        )

    # ---- Lock ----

    async def async_lock_lock(self, device_id: str) -> dict[str, Any]:
        """Lock the device."""
        return await self._request(API_LOCK_LOCK, json={"uuid": device_id})

    async def async_lock_unlock(self, device_id: str) -> dict[str, Any]:
        """Unlock the device."""
        return await self._request(API_LOCK_UNLOCK, json={"uuid": device_id})

    # ---- Camera ----

    async def async_get_camera_webrtc(self, device_id: str) -> dict[str, Any]:
        """Get camera KVS WebRTC credentials."""
        return await self._request(API_CAMERA_WEBRTC, json={"uuid": device_id})

    # ---- FRP Remote Access ----

    async def async_get_frp_config(self, client_id: str) -> dict[str, Any]:
        """Get FRP remote access configuration."""
        return await self._request(API_FRP_HTTP, json={"uuid": client_id})

    # ---- Utility ----

    async def async_get_snapshot(self, url: str) -> bytes | None:
        """Fetch image bytes from a snapshot URL.

        Args:
            url: The snapshot image URL (e.g. from device status).

        Returns:
            Image bytes, or None on failure.
        """
        try:
            resp = await self._session.get(url)
            if resp.status == 200:
                return await resp.read()
        except Exception:  # noqa: BLE001
            _LOGGER.debug("Failed to fetch snapshot from %s", url)
        return None
