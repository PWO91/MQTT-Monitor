cmake_minimum_required(VERSION 3.20)
project(MQTTMonitor)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_AUTOMOC ON)

find_package(Qt6 REQUIRED COMPONENTS Widgets DBus)
find_package(PahoMqttCpp REQUIRED)

add_executable(MQTTMonitor MACOSX_BUNDLE src/main.cpp)

set_target_properties(MQTTMonitor PROPERTIES
    MACOSX_BUNDLE_BUNDLE_NAME "MQTT Monitor"
    MACOSX_BUNDLE_GUI_IDENTIFIER "com.mcrtechlab.mqttmonitor"
)

target_link_libraries(MQTTMonitor
    Qt6::Widgets
    PahoMqttCpp::paho-mqttpp3-static
)

# --- macOS post-build: fix Qt paths + sign ---
if(APPLE)
    find_program(MACDEPLOYQT_EXECUTABLE macdeployqt
        HINTS /opt/homebrew/bin /opt/homebrew/opt/qt/bin)

    # Linker sometimes embeds absolute Homebrew paths instead of @rpath —
    # patch both /opt/homebrew/lib and /opt/homebrew/opt/qtbase/lib variants.
    set(QT_FW "@executable_path/../Frameworks")
    foreach(mod Widgets Gui Core)
        string(TOLOWER ${mod} modlc)
        set(_fw "Qt${mod}.framework/Versions/A/Qt${mod}")
        add_custom_command(TARGET MQTTMonitor POST_BUILD
            COMMAND install_name_tool
                -change "/opt/homebrew/lib/${_fw}"
                        "${QT_FW}/${_fw}"
                -change "/opt/homebrew/opt/qtbase/lib/${_fw}"
                        "${QT_FW}/${_fw}"
                "$<TARGET_FILE:MQTTMonitor>" || true
            COMMENT "Patching Qt${mod} rpath"
        )
    endforeach()
    add_custom_command(TARGET MQTTMonitor POST_BUILD
        COMMAND codesign --force --deep --sign -
            "$<TARGET_BUNDLE_DIR:MQTTMonitor>"
        COMMENT "Ad-hoc signing bundle"
    )

    # --- macOS DMG installer ---

    add_custom_target(dmg
        # Run macdeployqt WITHOUT rm -rf Frameworks first — removing Frameworks causes
        # macdeployqt to skip @executable_path deps treating them as already deployed
        COMMAND ${MACDEPLOYQT_EXECUTABLE}
                "$<TARGET_BUNDLE_DIR:MQTTMonitor>"
                -libpath=$<TARGET_FILE_DIR:Qt6::Core>/../../..
                -verbose=1
        COMMAND rm -rf "${CMAKE_BINARY_DIR}/dmg_staging"
        COMMAND mkdir -p "${CMAKE_BINARY_DIR}/dmg_staging"
        COMMAND ditto "$<TARGET_BUNDLE_DIR:MQTTMonitor>"
                "${CMAKE_BINARY_DIR}/dmg_staging/MQTT Monitor.app"
        # QtGui depends on QtDBus but macdeployqt omits it — copy manually
        COMMAND ditto "$<TARGET_FILE_DIR:Qt6::DBus>/../.."
                "${CMAKE_BINARY_DIR}/dmg_staging/MQTT Monitor.app/Contents/Frameworks/QtDBus.framework"
        COMMAND codesign --force --deep --sign -
                "${CMAKE_BINARY_DIR}/dmg_staging/MQTT Monitor.app"
        COMMAND ln -sf /Applications
                "${CMAKE_BINARY_DIR}/dmg_staging/Applications"
        COMMAND hdiutil create
                -volname "MQTT Monitor"
                -srcfolder "${CMAKE_BINARY_DIR}/dmg_staging"
                -ov -format UDZO
                "${CMAKE_BINARY_DIR}/MQTTMonitor.dmg"
        DEPENDS MQTTMonitor
        COMMENT "Building MQTTMonitor.dmg"
    )
endif()
