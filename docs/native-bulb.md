# Experimental A19-C1 native MQTT support

This branch extends the existing Xthings client and Home Assistant integration with confirmed state readback for the U-tec Bright A19-C1. Other models retain the existing HTTP/WebSocket behavior. The matching [Home Assistant Core development branch](https://github.com/ariofrio/core/tree/ariofrio/xthings-bulb-mqtt) targets upstream `dev`. The [HA 2026.9.3 deployment branch](https://github.com/ariofrio/core/tree/ariofrio/xthings-bulb-mqtt-ha2026.9.3) preserves the version installed and tested below.

The transport is **cloud MQTT**, not a LAN or Bluetooth connection. It requires internet access and caller-supplied TLS client credentials. No certificate or private key is distributed in either fork. There is no implemented public credential provisioning or renewal flow; a vendor certificate revocation would require replacement credentials. This remains an experimental deployment, not an upstream-supported release.

## State and controls

`NativeBulbClient` in [bulb.py](../ha_xthings_cloud/bulb.py) subscribes to two exact topics for the selected device, then issues a non-mutating `FC/sy` request. It accepts complete, validated `NT/sy` responses with the matching request ID on either response topic. Retained, incomplete, malformed, and unrelated replies cannot establish availability.

Startup and reconnect perform a fresh query. A health query runs approximately every 30 seconds, and recognized notifications trigger an earlier query. A failed query marks the bulb unavailable; subsequent successful queries restore availability. Changes made outside HA can therefore take roughly one polling interval plus network time to appear. This is bounded retry behavior, not a guarantee that an offline device or unavailable cloud service will answer.

Commands read the current state, preserve unrelated settings, send `CC/se`, and require fresh readback matching the requested fields. A broker acknowledgement alone does not confirm success. Unconfirmed commands raise an error; the requested state is never substituted for the observed state. Independent controllers can still race with the read/modify/write sequence because the protocol has no known conditional-write primitive.

The tested native fields are power (`pw`), brightness (`br`), mode (`ct`), temperature slider (`tp`), and HSL color (`hu`, `sa`, `li`). HA converts HSL to its HS representation and maps temperature settings 1–100 linearly to the advertised 2700–6500 K range. **Intermediate Kelvin values are approximate, not measured color temperatures.** Legacy slider value 0 is displayed at the warm endpoint; new HA commands use 1–100.

Account-scoped route discovery uses the existing authenticated client. Returned routing data contains only supported device identifiers and address IDs; unrelated metadata is discarded. Discovery is in `XthingsCloudApiClient.async_get_native_bulb_routes()` in [client.py](../ha_xthings_cloud/client.py).

## Installing on HAOS

The Core source remains the development source. [tools/package_ha.py](../tools/package_ha.py) generates a custom integration archive from it and the client wheel; there is no second manually maintained implementation. This layout is for direct installation, not HACS.

1. Check out the client branch and a Core branch matching your HA version. For HA 2026.9.3, use the deployment branch linked above. Run the client tests and the Core Xthings integration tests. Follow Core's contributor setup instructions, including generating English translations when changing strings.
2. Build the client wheel with `uv build --wheel --out-dir dist`.
3. From the client checkout, package it:

   ```sh
   python tools/package_ha.py /path/to/core dist/ha_xthings_cloud-1.0.6.dev1-py3-none-any.whl dist/xthings_cloud.tar.gz
   ```

4. Create an HA backup. If an `xthings_cloud` custom integration already exists, preserve it before replacing it. Extract the archive into HA's `/config`; it creates `/config/custom_components/xthings_cloud/` and bundles the wheel there. The generated manifest references that local wheel.
5. Supply your TLS certificate and private key as private files readable by HA, for example `/config/.xthings_cloud/client-cert.pem` and `client-key.pem`. Restrict the directory to mode 700 and both files to 600. Do not commit them or include them in a public archive.
6. Run HA's configuration check and restart HA. In the existing Xthings integration's options, enable native MQTT and enter the two paths. The options flow checks that the TLS files load; a successful load does not itself prove the broker will accept them.
7. Check that the existing bulb entity exposes HS and color temperature, reads its current temperature after startup, and confirms controls. Test another controller's changes and an integration reload. Other device models should continue using their existing entities and transport.

For rollback, disable native MQTT in the options, move the custom `xthings_cloud` directory outside `/config/custom_components`, and restart HA. HA will load the built-in integration and its dependency requirement again. The backup can restore the complete previous HA configuration if needed. Preserve the private credential files only if you intend to reinstall.

The [client draft PR](https://github.com/XthingsJacobs/ha-xthings-cloud/pull/1) needs review and a published release before the companion Core change can merge. Credential provisioning and renewal remain unresolved.

## Validation

On 2026-09-24, testing against HA 2026.9.3 / HAOS 18.3 and an A19-C1 running firmware 01.42.0301 confirmed:

- Startup readback through the installed custom integration.
- HA commands for both temperature endpoints, saturated and desaturated colors, brightness, and power, independently checked with a second native MQTT client.
- An external temperature change appearing in HA after 29.7 seconds.
- Fresh temperature recovery after an integration reload.
- Exact restoration of the original native state after testing.

Five control measurements took 1.52–2.14 seconds from HA service call through the independent verification query. These are protocol round-trip measurements, not optical response latency.

Validation passed 14 client tests and 50 HA integration tests (including 21 snapshots), plus all applicable Core hooks for the changed files: Ruff, formatting, spelling, JSON, mypy, pylint, requirements generation, and hassfest. A repository-wide hook run failed on an unrelated duplicate `homeassistant.util.event_type` module (`.py` / `.pyi`); its long-running whole-repository pylint process was stopped after the focused checks passed.

Automated tests cover response validation, retained/stale reply rejection, reconnect, missing-reply recovery, command confirmation failures, state preservation, and shutdown. Core tests cover capabilities without a temperature reading, unit conversion, stale HTTP/WebSocket isolation, options validation, unavailable startup, unload cleanup, and preserving account refresh scheduling during frequent native reports.
