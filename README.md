# SDSQL

C++ 课程大作业，一个简单的C/S分离的RDBMS

## Build

```bash
mkdir build
cd build
cmake ..
make sdsql-server # Build Server
make sdsql-client # Build Client
```

## Run

```bash
./sdsql-server # Run Server
./sdsql-client # Run Client
```

默认用户名: admin
默认密码: 123456

默认使用 `127.0.0.1:4399` 通信。

测试用例位于 `test/cli/testcase.txt`
