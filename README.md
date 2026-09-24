# ha-xthings-cloud

Async Python client for the [Xthings Cloud](https://cloud.xthings.com) API, designed for [Home Assistant](https://www.home-assistant.io/) integration.

## Experimental A19-C1 extension

This fork adds optional native cloud MQTT readback and confirmed controls for the A19-C1 bulb. See [protocol behavior, installation, limitations, and validation](docs/native-bulb.md). TLS credentials are supplied privately by the caller.

## Installation

```bash
pip install ha-xthings-cloud
```

## Usage

### API Client

```python
import aiohttp
from ha_xthings_cloud import XthingsCloudApiClient

async with aiohttp.ClientSession() as session:
    client = XthingsCloudApiClient(session)

    # Login (supports 2FA)
    result = await client.async_login("user@example.com", "password")

    # Get devices
    devices = await client.async_get_devices()

    # Control devices
    await client.async_switch_on("device-uuid")
    await client.async_brite_brightness("device-uuid", 80)
    await client.async_lock_lock("device-uuid")
```

### WebSocket (Real-time Push)

```python
from ha_xthings_cloud import XthingsCloudWebSocket

def on_status(device_uuid, status):
    print(f"Device {device_uuid}: {status}")

async def on_token_expired():
    # refresh token logic
    pass

ws = XthingsCloudWebSocket(session, token, on_status, on_token_expired)
await ws.async_start()
```

### KVS Signaling (Camera WebRTC)

```python
from ha_xthings_cloud import KvsSignalingClient

kvs = KvsSignalingClient(session, region, channel_arn, credentials)
answer_sdp = await kvs.async_get_answer_sdp(offer_sdp, on_ice_candidate=callback)
```

## Features

- Async HTTP client with `aiohttp.ClientSession` injection
- Authentication: login, token refresh, 2FA (email/phone)
- Device control: Switch, Plug, Light (brightness/HS color/color temp), Lock, Camera
- WebSocket: real-time device status push with auto-reconnect
- KVS Signaling: AWS Kinesis Video Streams WebRTC SDP exchange with SigV4 auth
- FRP remote access configuration

## API Reference

### XthingsCloudApiClient

| Method | Description |
|--------|-------------|
| `async_login(email, password, ...)` | Login with optional 2FA |
| `async_refresh_token(refresh_token)` | Refresh auth token |
| `async_get_devices()` | Get device list |
| `async_get_device_status(device_id)` | Get single device status |
| `async_switch_on/off(device_id)` | Switch control |
| `async_plug_on/off(device_id)` | Plug control |
| `async_brite_on/off(device_id)` | Light on/off |
| `async_brite_brightness(device_id, brightness)` | Light brightness (0-100) |
| `async_brite_color(device_id, color)` | Light color (HS/color temp) |
| `async_lock_lock/unlock(device_id)` | Lock control |
| `async_get_camera_webrtc(device_id)` | Get camera KVS credentials |
| `async_get_frp_config(client_id)` | Get FRP remote access config |

### XthingsCloudWebSocket

| Method | Description |
|--------|-------------|
| `async_start()` | Start WebSocket with auto-reconnect |
| `async_stop()` | Stop WebSocket connection |

### KvsSignalingClient

| Method | Description |
|--------|-------------|
| `async_get_answer_sdp(offer_sdp, on_ice_candidate)` | SDP offer/answer exchange |
| `async_send_ice_candidate(candidate, sdp_mid, sdp_mline_index)` | Send ICE candidate |
| `async_get_ice_server_config()` | Get TURN/STUN servers |
| `async_close()` | Close signaling connection |

## License

Apache-2.0
