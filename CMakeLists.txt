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

# --- macOS DMG installer ---
if(APPLE)
    find_program(MACDEPLOYQT_EXECUTABLE macdeployqt
        HINTS /opt/homebrew/bin /opt/homebrew/opt/qt/bin)

    add_custom_target(dmg
        # macdeployqt dodaje brakujące frameworki Qt (np. QtWidgets, QtSvg)
        # BEZ wcześniejszego rm -rf Frameworks — usunięcie Frameworks powoduje
        # że macdeployqt pomija @executable_path deps jako "już wdrożone"
        COMMAND ${MACDEPLOYQT_EXECUTABLE}
                "$<TARGET_BUNDLE_DIR:MQTTMonitor>"
                -libpath=$<TARGET_FILE_DIR:Qt6::Core>/../../..
                -verbose=1
        # ditto zachowuje symlinki i xattry wewnątrz frameworków Qt
        COMMAND rm -rf "${CMAKE_BINARY_DIR}/dmg_staging"
        COMMAND mkdir -p "${CMAKE_BINARY_DIR}/dmg_staging"
        COMMAND ditto "$<TARGET_BUNDLE_DIR:MQTTMonitor>"
                "${CMAKE_BINARY_DIR}/dmg_staging/MQTT Monitor.app"
        # QtGui zależy od QtDBus, ale macdeployqt go pomija — kopiujemy ręcznie
        COMMAND ditto "$<TARGET_FILE_DIR:Qt6::DBus>/../.."
                "${CMAKE_BINARY_DIR}/dmg_staging/MQTT Monitor.app/Contents/Frameworks/QtDBus.framework"
        # Podpisanie ad-hoc po skopiowaniu (na plikach które trafią do DMG)
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
