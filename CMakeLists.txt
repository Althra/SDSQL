# --- 修改后的 CMakeLists.txt ---

cmake_minimum_required(VERSION 3.10)
project(sdsql CXX)

# 设置C++17标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(include)

# 暂时注释掉未完成的程序目标
# add_executable(sdsql )
# add_executable(sdsql-server )
# add_executable(sdsql-cli )
# add_executable(sdsql-gui )

# 保留我们正在测试的目标，这个是完整的，可以正常编译
add_executable(server_test 
    src/server/main.cpp
    src/server/Database.cpp
    src/server/DDLOperations.cpp
    src/server/TransactionManager.cpp
)