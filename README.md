# MQTT Monitor

A cross-platform desktop MQTT client built with Qt6 and Paho MQTT C++. Designed for developers who need to inspect, debug and publish MQTT messages in real time.

## Features

- **Topic tree** — live tree view of all received topics with child counts, collapsed by default
- **Message history** — last 50 messages per topic with timestamps
- **Payload formats** — view as Raw, pretty-printed JSON, Hex dump or Base64
- **Retained marker** — `[R]` indicator and amber color for retained messages
- **Publish** — send messages with configurable QoS (0/1/2) and Retained flag
- **Subscriptions** — manage multiple topic filters (wildcards supported)
- **Search** — filter the topic tree by name in real time
- **TLS/SSL** — connect to brokers over `ssl://` (port auto-switches 1883 ↔ 8883)
- **Connection profiles** — save and load broker settings as JSON files
- **Export** — dump all received messages to CSV or JSON
- **Persistence** — last connection settings restored on next launch

## Requirements

| Dependency | Version |
|---|---|
| Qt6 (Widgets, DBus) | 6.x |
| Paho MQTT C++ | 1.x |
| CMake | ≥ 3.20 |
| C++ | 17 |

On macOS via Homebrew:

```bash
brew install qt paho-mqtt-cpp
```

## Build

```bash
cmake -B build -DCMAKE_TOOLCHAIN_FILE=build/vcpkg/scripts/buildsystems/vcpkg.cmake
cmake --build build
```

### macOS DMG installer

```bash
cmake --build build --target dmg
# Output: build/MQTTMonitor.dmg
```

## Usage

1. Enter broker address, port and optional credentials in the **Połączenie** tab
2. Add topic filters in the **Subskrypcje** tab (default: `#`)
3. Click **Połącz** — the topic tree populates as messages arrive
4. Click any topic to see the current payload and message history
5. Use the **Publish** tab to send messages to any topic

### TLS

Check the **TLS** box before connecting. The port switches automatically to 8883. Certificate verification is skipped by default (suitable for development).

### Export

**Plik → Eksportuj wiadomości (CSV)** or **(JSON)** saves all messages received in the current session.

## Project structure

```
src/main.cpp        — full application (single-file)
CMakeLists.txt      — build + macOS DMG target
.env.example        — environment variable reference
```

## Environment variables

Broker settings can be pre-filled via environment variables (useful for CI or scripted testing):

```
MQTT_BROKER=localhost
MQTT_USER=
MQTT_PASSWORD=
```
