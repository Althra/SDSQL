cmake_minimum_required(VERSION 3.10)
set(CMAKE_CXX_COMPILER clang++)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

project(sdsql)

include_directories(include)

aux_source_directory(src/client CLIENT_SRC)
aux_source_directory(src/server SERVER_SRC)
set(NET_CLIENT "src/net/protocol.cpp" "src/net/serializer.cpp" "src/net/socket_client.cpp")
set(NET_SERVER "src/net/protocol.cpp" "src/net/serializer.cpp" "src/net/socket_server.cpp")

add_executable(sdsql-server ${SERVER_SRC} ${NET_SERVER} "driver/server_main.cpp")
add_executable(sdsql-client ${CLIENT_SRC} ${NET_CLIENT} "driver/client_main.cpp")
