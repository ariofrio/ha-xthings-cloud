# Experimental A19-C1 native MQTT support

This branch extends the existing Xthings client and Home Assistant integration with confirmed state readback for the U-tec Bright A19-C1. Other models retain the existing HTTP/WebSocket behavior. The matching [Home Assistant Core development branch](https://github.com/ariofrio/core/tree/ariofrio/xthings-bulb-mqtt) targets upstream `dev`. The [HA 2026.9.3 deployment branch](https://github.com/ariofrio/core/tree/ariofrio/xthings-bulb-mqtt-ha2026.9.3) preserves the version installed and tested below.

The transport is **cloud MQTT**, not a LAN or Bluetooth connection. It requires internet access and uses the shared TLS client certificate/key recovered from Xthings Android 3.7.0.2, bundled under `ha_xthings_cloud/certs/`. These are application credentials, not a user account token. `create_bulb_ssl_context()` loads them with normal server certificate and hostname verification. Vendor rotation or revocation would require replacing the bundled pair and releasing an updated package. This remains an experimental deployment, not an upstream-supported release.

## State and controls

`NativeBulbClient` in [bulb.py](../ha_xthings_cloud/bulb.py) subscribes to two exact topics for the selected device, then issues a non-mutating `FC/sy` request. It accepts complete, validated `NT/sy` responses with the matching request ID on either response topic. Retained, incomplete, malformed, and unrelated replies cannot establish availability.

Startup and reconnect perform a fresh query. A health query runs approximately every 30 seconds, and recognized notifications trigger an earlier query. A failed query marks the bulb unavailable; subsequent successful queries restore availability. Changes made outside HA can therefore take roughly one polling interval plus network time to appear. This is bounded retry behavior, not a guarantee that an offline device or unavailable cloud service will answer.

Commands read the current state, preserve unrelated settings, send `CC/pw` for power/brightness and `CC/se` for color/temperature, and require fresh readback matching the requested fields. A broker acknowledgement alone does not confirm success. Unconfirmed commands raise an error; the requested state is never substituted for the observed state. Independent controllers can still race with the read/modify/write sequence because the protocol has no known conditional-write primitive.

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
   python tools/package_ha.py /path/to/core dist/ha_xthings_cloud-1.0.6.dev3-py3-none-any.whl dist/xthings_cloud.tar.gz
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
- An external temperature change appearing in HA after 29.7 seconds.
- Fresh temperature recovery after an integration reload.
- Exact restoration of the original native state after testing.

Five control measurements took 1.52–2.14 seconds from HA service call through the independent verification query. These are protocol round-trip measurements, not optical response latency.

Validation passed 18 client tests and 59 HA integration tests (including 21 snapshots). Applicable Core hooks for the changed files cover Ruff, formatting, spelling, JSON, mypy, pylint, requirements generation, and hassfest. Whole-repository validation encounters existing errors outside this integration; these results do not establish a clean repository-wide run.

Automated tests cover standalone/group route discovery, dedicated power commands (including combined power and color settings), response validation, retained/stale reply rejection, reconnect, missing-reply recovery, command confirmation failures, state preservation, and shutdown. Core tests cover capabilities without a temperature reading, color conversion and confirmed readback, stale HTTP/WebSocket isolation, options validation, unavailable startup, unload cleanup, account refresh scheduling, reauthentication, account mismatch rejection, and isolation/recovery of native setup failures.

The client suite also verifies that the real packaged certificate/key load while preserving server verification. The built wheel was checked for inclusion of exactly the shared pair and successfully loaded directly as a ZIP package; account tokens and other private files are excluded.
