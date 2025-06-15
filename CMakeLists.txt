cmake_minimum_required(VERSION 3.10)

project(sdsql CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(include)

add_executable(sdsql )
add_executable(sdsql-server )
add_executable(sdsql-cli )
add_executable(sdsql-gui )
