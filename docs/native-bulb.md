# Experimental A19-C1 native MQTT support

This branch extends the existing Xthings client and Home Assistant integration with confirmed state readback for the U-tec Bright A19-C1. Other models retain the existing HTTP/WebSocket behavior. The matching [Home Assistant Core development branch](https://github.com/ariofrio/core/tree/ariofrio/xthings-bulb-mqtt) targets upstream `dev`. The [HA 2026.9.3 deployment branch](https://github.com/ariofrio/core/tree/ariofrio/xthings-bulb-mqtt-ha2026.9.3) preserves the version installed and tested below.

The transport is **cloud MQTT**, not a LAN or Bluetooth connection. It requires internet access and uses the shared TLS client certificate/key recovered from Xthings Android 3.7.0.2, bundled under `ha_xthings_cloud/certs/`. These are application credentials, not a user account token. `create_bulb_ssl_context()` loads them with normal server certificate and hostname verification. Vendor rotation or revocation would require replacing the bundled pair and releasing an updated package. This remains an experimental deployment, not an upstream-supported release.

## State and controls

`NativeBulbClient` in [bulb.py](../ha_xthings_cloud/bulb.py) subscribes to two exact topics for the selected device, then issues a non-mutating `FC/sy` request. It accepts complete, validated `NT/sy` responses with the matching request ID on either response topic. Retained, incomplete, malformed, and unrelated replies cannot establish availability. Each bulb keeps its own MQTT connection. Sharing one connection among the three grouped bulbs was tried and rejected: in alternating live bursts, lost confirmation replies rose from 16 of 60 bulb-runs to 36 of 60, and the median completion without a lost reply rose from 2.35 to about 2.8 seconds. A separate session ID per bulb on the shared connection did not help.

Startup and reconnect perform a fresh query. A health query runs when no query has succeeded for approximately 30 seconds, including confirmations of HA commands, and recognized notifications trigger an earlier query. A disconnect, or three consecutive failed queries, marks the bulb unavailable; failed health queries are retried after 2 seconds, and a successful query restores availability. Isolated lost replies therefore do not make the entity flicker. Changes made outside HA can therefore take roughly one polling interval plus network time to appear. This is bounded retry behavior, not a guarantee that an offline device or unavailable cloud service will answer.

Commands send `CC/pw` for power/brightness and `CC/se` for color/temperature, and require fresh readback matching the requested fields. `CC/se` carries every field, so unrelated fields come from the last confirmed state; the bulb is read first only when no confirmed state is known. A change made outside HA since the last query can therefore be overwritten by a color/temperature command from HA. This trades that uncommon case for one fewer round trip per command. While one command is in flight, newer slider changes for the same bulb merge by field into a single pending command, and their callers share its confirmed result. Explicit on/off commands are never merged, so they keep their order. Combined controls confirm the scene settings, including brightness, before any power command, so the device is not given overlapping commands. When that readback shows the requested power state, as it does for a bulb that is already on, no power command is sent. The A19-C1 drops the reply to a query that arrives within about 0.2 seconds of a command, and a dropped reply never arrives late: in live bursts, 15 of 160 such queries went unanswered, and none of 104 later queries. Each query therefore sends one backup query if no reply arrives within 1 second, accepting whichever reply comes first, within the 5-second query timeout. A confirmation that still fails triggers another fresh query, up to three attempts, without resending the setting commands. A broker acknowledgement alone does not confirm success. Unconfirmed commands raise an error; the requested state is never substituted for the observed state. The protocol has no known conditional-write primitive, so independent controllers can race regardless.

The tested native fields are power (`pw`), brightness (`br`), mode (`ct`), temperature slider (`tp`), and HSL color (`hu`, `sa`, `li`). HA converts HSL to its HS representation and maps temperature settings 1–100 linearly to the advertised 2700–6500 K range. **Intermediate Kelvin values are approximate, not measured color temperatures.** Legacy slider value 0 is displayed at the warm endpoint; new HA commands use 1–100.

Account-scoped route discovery uses the existing authenticated client. Returned routing data contains supported device identifiers, address IDs, and optional group IDs; unrelated metadata is discarded. Group members are discovered inside `Britegroup.lights` and subscribe to group-specific response topics, while commands still target each bulb individually. If a connected bulb is regrouped in the app, reload the integration to rediscover its response route. Discovery is in `XthingsCloudApiClient.async_get_native_bulb_routes()` in [client.py](../ha_xthings_cloud/client.py).

Authentication failures start HA's reauthentication flow, which requires the same account and preserves the entry's options. Other native setup failures leave HTTP/WebSocket devices available and missing native bulbs unavailable. Native setup retries on the next account poll, or immediately when the integration is reloaded.

## Kelvin mapping evidence

The native setting is read back exactly, but the nominal Kelvin conversion remains unverified. The linear formula is an integration assumption, not a vendor-provided formula or an optical calibration.

On 2026-09-24, the complete [vendor Foundational API reference](https://support.xthings.com/hc/en-us/articles/39867633454361-Developer-Foundational-APIs) and the other 18 articles in its developer section provided no native-slider-to-Kelvin conversion. The reference describes `colorTemperatureRange`, but its sample A19-C1 range is 2000–9000, inconsistent with the [product's advertised 2700–6500 K range](https://u-tec.com/products/bright-a19-color). The sample cannot establish this bulb's mapping.

A fresh OpenAPI discovery still classified the tested A19-C1 as `utec-dimmer`, with only a 1–100 brightness range. A fresh state query returned health, power, and brightness, with no color temperature. The [vendor's handler reference](https://support.xthings.com/hc/en-us/articles/39872638936217-Device-Handler-Types-Reference) assigns no color-temperature capability to that handler. The app's native device metadata reports the slider value without Kelvin metadata; its white-picker code sends a percentage and its color helper draws the UI gradient.

A vendor-defined formula or table for A19-C1 firmware 01.42.0301, or an authoritative API reading tied to native `tp`, is still needed to replace the estimate. Neither generic range examples nor SmartThings values without confirmed native changes establish that relationship.

## Installing on HAOS

The Core source remains the development source. [tools/package_ha.py](../tools/package_ha.py) generates a custom integration archive from it and the client wheel; there is no second manually maintained implementation. This layout is for direct installation, not HACS.

1. Check out the client branch and a Core branch matching your HA version. For HA 2026.9.3, use the deployment branch linked above. Run the client tests and the Core Xthings integration tests. Follow Core's contributor setup instructions, including generating English translations when changing strings.
2. Build the client wheel with `uv build --wheel --out-dir dist`.
3. From the client checkout, package it:

   ```sh
   python tools/package_ha.py /path/to/core dist/ha_xthings_cloud-1.0.6.dev9-py3-none-any.whl dist/xthings_cloud.tar.gz
   ```

4. Create an HA backup. If an `xthings_cloud` custom integration already exists, preserve it before replacing it. Extract the archive into HA's `/config`; it creates `/config/custom_components/xthings_cloud/` and bundles the wheel there. The generated manifest references that local wheel.
5. Run HA's configuration check and restart HA. In the existing Xthings integration's options, enable native MQTT. No certificate paths or manual extraction are required.
6. Check that the existing bulb entity exposes HS and color temperature, reads its current temperature after startup, and confirms controls. Test another controller's changes and an integration reload. Other device models should continue using their existing entities and transport.

For rollback, disable native MQTT in the options, move the custom `xthings_cloud` directory outside `/config/custom_components`, and restart HA. HA will load the built-in integration and its dependency requirement again. The backup can restore the complete previous HA configuration if needed.

The [client draft PR](https://github.com/XthingsJacobs/ha-xthings-cloud/pull/1) needs review and a published release before the companion Core change can merge. Maintainer review of the bundled shared credential and its replacement strategy remains pending. Bundling it does not remove the cloud dependency or revocation risk.

## Validation

On 2026-09-24, testing against HA 2026.9.3 / HAOS 18.3 and four A19-C1 bulbs running firmware 01.42.0301 confirmed:

- Startup readback through the installed custom integration for one standalone bulb and three members of an Xthings group.
- Individual HA off/on commands for all four bulbs, with independent device readback, unchanged neighboring bulbs, and exact restoration of starting settings.
- HA commands for both temperature endpoints, saturated and desaturated colors, brightness, and power, independently checked with a second native MQTT client.
- An external setting change appearing in HA after 6.1 seconds in the full functionality suite.
- A 54-scenario live suite across four individual bulbs and their HA group: power, brightness endpoints and zero, temperature endpoints, saturated/desaturated colors, settings applied from off, overlapping slider requests, external state changes, and recovery after an integration reload. Every action was checked against fresh native state; unrelated bulbs retained their settings.
- Exact restoration of the original native state after testing.

The 55 Home Assistant service calls in that suite completed within 3.58 seconds each. These measure confirmed service completion, not optical response latency.

In rapid group bursts through HA (one request every 150 ms), confirmed completion of the final value took 1.2–1.8 seconds across three runs for eight brightness steps and 0.8–1.7 seconds for six combined brightness/temperature steps, down from 11.4 and 12.7 seconds before pending changes were merged, redundant reads and power commands were removed, and backup queries were added. In alternating direct bursts on the three grouped bulbs, backup queries reduced the median from 7.5 to 3.1 seconds and eliminated waits for the 5-second timeout (13 of 24 bulb-runs to 0 of 24). Every burst ended with the last requested values read back from all three bulbs. The 54-scenario suite, rerun with this version, passed with every light command completing within 1.84 seconds (median 0.80 seconds); the only unavailable state was during the deliberate integration reload.

Validation passed 34 client tests and 59 HA integration tests (including 21 snapshots). Applicable Core hooks for the changed files cover Ruff, formatting, spelling, JSON, mypy, pylint, requirements generation, and hassfest. Whole-repository validation encounters existing errors outside this integration; these results do not establish a clean repository-wide run.

Automated tests cover standalone/group route discovery, dedicated power commands (including combined power and color settings), response validation, retained/stale reply rejection, reconnect, missing-reply recovery, availability after isolated and repeated lost replies, health-poll deferral and retry, lost confirmation replies, backup queries for lost and slow replies, scene commands from confirmed state, command confirmation failures, state preservation, overlapping controls, merging of pending slider changes with on/off ordering preserved, and shutdown during in-flight publication. Core tests cover capabilities without a temperature reading, color conversion and confirmed readback, stale HTTP/WebSocket isolation, options validation, unavailable startup, unload cleanup, account refresh scheduling, reauthentication, account mismatch rejection, and isolation/recovery of native setup failures.

The client suite also verifies that the real packaged certificate/key load while preserving server verification. The built wheel was checked for inclusion of exactly the shared pair and successfully loaded directly as a ZIP package; account tokens and other private files are excluded.
