cmake_minimum_required(VERSION 3.20)
project(MyProject)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_AUTOMOC ON)

find_package(Qt6 REQUIRED COMPONENTS Widgets)
find_package(PahoMqttCpp REQUIRED)

add_executable(my_app src/main.cpp)

target_link_libraries(my_app
    Qt6::Widgets
    PahoMqttCpp::paho-mqttpp3-static
)