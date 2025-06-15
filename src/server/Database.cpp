// src/server/Database.cpp

#include "../../include/server/DatabaseAPI.hpp"
#include <filesystem>
#include <fstream> // 用于从文件加载/保存数据
#include <iostream>
#include <map>
#include <sstream> // For std::stringstream

// ========================================================================
// DatabaseCoreImpl 现在在 DatabaseAPI.hpp 中定义，并且是 Database 的 Pimpl
// 各个模块的 Impl 类现在也将在各自的 .cpp 文件中完全定义
// ========================================================================

// 移除所有 DDLOperations::Impl, DMLOperations::Impl, TransactionManager::Impl
// 的前向声明和伪定义 它们会在各自的 .cpp
// 文件中被完全定义，并由相应的公共类来管理其 unique_ptr

// ========================================================================
// Database 公共接口的实现
// ========================================================================

Database::Database(const std::string &dbPath)
    // 直接将 DatabaseCoreImpl 作为 Database 的 Pimpl
    : core_state_pImpl(std::make_unique<DatabaseCoreImpl>()) {

  // 初始化核心状态的 rootPath
  core_state_pImpl->rootPath = dbPath;
  if (!std::filesystem::exists(dbPath)) {
    std::filesystem::create_directory(dbPath);
  } else if (!std::filesystem::is_directory(dbPath)) {
    throw std::runtime_error("Provided dbPath is not a directory: " + dbPath);
  }

  // 使用 core_state_pImpl.get() 将 DatabaseCoreImpl
  // 的裸指针传递给各个模块的操作类
  ddl_ops = std::make_unique<DDLOperations>(core_state_pImpl.get());
  dml_ops = std::make_unique<DMLOperations>(core_state_pImpl.get());
  tx_manager = std::make_unique<TransactionManager>(core_state_pImpl.get());
}

// 析构函数必须在 .cpp 中定义，因为 unique_ptr 析构时需要完整类型
Database::~Database() = default;

DDLOperations &Database::getDDLOperations() { return *ddl_ops; }

DMLOperations &Database::getDMLOperations() { return *dml_ops; }

TransactionManager &Database::getTransactionManager() { return *tx_manager; }
