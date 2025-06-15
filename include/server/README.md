# README

此文件夹存放服务端头文件。

#### 2025-6-14  更新
**更新 DatabaseAPI.hpp**
为了支持权限控制，我们需要在 DatabaseAPI.hpp 中添加新的概念和对 Database::Impl 的要求：
- 权限类型枚举 PermissionType 
  - 新增权限类型:
    - CREATE_USER,    // 创建用户权限
    - DROP_USER,      // 删除用户权限
    - GRANT_PERMISSION, // 授予权限权限
    - REVOKE_PERMISSION // 撤销权限权限
- User 结构体: 存储用户信息。
- PermissionEntry 结构体: 
  - 记录具体的权限规则。
- Database::Impl 的前向声明
  - 假设其包含所有核心模块的内部成员和方法 
  - 这些成员和方法的具体定义将在 Database.cpp 中。