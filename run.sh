#!/usr/bin/env bash
set -e

ROOT="$(cd "$(dirname "$0")" && pwd)"
BUILD="$ROOT/build"

# Configure if build directory doesn't exist
if [ ! -f "$BUILD/CMakeCache.txt" ]; then
    echo ">>> Configuring..."
    cmake -B "$BUILD" \
        -DCMAKE_TOOLCHAIN_FILE="$BUILD/vcpkg/scripts/buildsystems/vcpkg.cmake"
fi

echo ">>> Building..."
cmake --build "$BUILD"

echo ">>> Launching MQTT Monitor..."
open "$BUILD/MQTTMonitor.app"
