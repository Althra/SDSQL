cmake_minimum_required(VERSION 3.10)
set(CMAKE_CXX_COMPILER clang++)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

project(sdsql)

include_directories(include)

aux_source_directory(src/client CLIENT_SRC)
aux_source_directory(src/server SERVER_SRC)
aux_source_directory(src/network NETWORK_SRC)
set(NET_CLIENT "src/network/protocol.cpp" "src/network/serializer.cpp" "src/network/socket_client.cpp" "src/network/query.cpp")
set(NET_SERVER "src/network/protocol.cpp" "src/network/serializer.cpp" "src/network/socket_server.cpp" "src/network/query.cpp")

add_executable(sdsql-server ${SERVER_SRC} ${NETWORK_SRC} "driver/server_main.cpp")
add_executable(sdsql-client ${CLIENT_SRC} ${NETWORK_SRC} "driver/client_main.cpp")
