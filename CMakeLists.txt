cmake_minimum_required(VERSION 3.20)
project(MQTTMonitor)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_AUTOMOC ON)

find_package(Qt6 REQUIRED COMPONENTS Widgets)
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
