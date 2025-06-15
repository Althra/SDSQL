// src/server/Database.cpp

#include "../../include/server/DatabaseAPI.hpp" // 包含数据库API头文件
#include <filesystem>                           // 用于文件和目录操作
#include <fstream>                              // 用于文件读写
#include <iostream> // 用于在控制台打印信息
#include <map>      // 用于 std::map
#include <sstream>  // 用于 std::stringstream

// DDLOperations::Impl 的前向声明，其完整定义将在 DDLOperations.cpp 中
// DMLOperations::Impl 的前向声明，其完整定义将在 DMLOperations.cpp 中
// TransactionManager::Impl 的前向声明，其完整定义将在 TransactionManager.cpp 中

// ========================================================================
// Database 公共接口的实现
// ========================================================================

Database::Database(const std::string &dbPath)
    // 直接将 DatabaseCoreImpl 作为 Database 的 Pimpl 管理
    : core_state_pImpl(std::make_unique<DatabaseCoreImpl>()) {

  // 初始化核心状态的 rootPath
  core_state_pImpl->rootPath = dbPath;
  if (!std::filesystem::exists(dbPath)) {
    std::filesystem::create_directory(dbPath);
  } else if (!std::filesystem::is_directory(dbPath)) {
    throw std::runtime_error("Provided dbPath is not a directory: " + dbPath);
  }

  // 使用 core_state_pImpl.get() 将 DatabaseCoreImpl
  // 的裸指针传递给各个模块的操作类 这些操作类将使用该指针来初始化它们各自的
  // Impl Pimpl
  ddl_ops = std::make_unique<DDLOperations>(core_state_pImpl.get());
  dml_ops = std::make_unique<DMLOperations>(core_state_pImpl.get());
  tx_manager = std::make_unique<TransactionManager>(core_state_pImpl.get());
}

// 析构函数必须在 .cpp 中定义，因为 unique_ptr 析构时需要完整类型
Database::~Database() = default;

DDLOperations &Database::getDDLOperations() { return *ddl_ops; }

DMLOperations &Database::getDMLOperations() { return *dml_ops; }

TransactionManager &Database::getTransactionManager() { return *tx_manager; }