cmake_minimum_required(VERSION 3.10)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

project(sdsql)

include_directories(include)

aux_source_directory(src/client CLIENT_SRC)
aux_source_directory(src/server SERVER_SRC)
aux_source_directory(src/network NETWORK_SRC)

add_executable(sdsql-server ${SERVER_SRC} ${NETWORK_SRC} "driver/server_main.cpp")
add_executable(sdsql-client ${CLIENT_SRC} ${NETWORK_SRC} "driver/client_main.cpp")
 