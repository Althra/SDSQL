// src/server/Database.cpp

#include "../../include/server/DatabaseAPI.hpp"
#include <filesystem>
#include <iostream>

// ========================================================================
// 这是所有模块共享的核心状态，现在我们把它放到 Database::Impl 中
// ========================================================================
struct DatabaseCoreImpl {
    std::string rootPath;
    std::string currentDbName;
    bool isTransactionActive = false;
    std::string transactionLogPath;
    DMLOperations* dml_ops_ = nullptr; // 暂时用不到
};


// ========================================================================
// 各个模块的内部实现类 (Pimpl) 的定义
// ========================================================================
class DDLOperations::Impl {
public:
    DatabaseCoreImpl* core_impl_;
    explicit Impl(DatabaseCoreImpl* core) : core_impl_(core) {}
    // 这里可以添加DDL::Impl特有的成员
};

class DMLOperations::Impl {
    // ... DML的实现 ...
};
class TransactionManager::Impl {
public:
    DatabaseCoreImpl* core_impl_;
    explicit Impl(DatabaseCoreImpl* core) : core_impl_(core) {}
    // ... TransactionManager::Impl的实现 ...
};
class AccessControl::Impl {
    // ... AccessControl的实现 ...
};


// ========================================================================
// Database 类的 Pimpl 实现
// ========================================================================
class Database::Impl {
public:
    // 持有真正的核心数据
    DatabaseCoreImpl core_state_;

    // 持有所有模块的内部实现
    std::unique_ptr<DDLOperations::Impl> ddl_impl_;
    std::unique_ptr<TransactionManager::Impl> tx_impl_;
    // ... 其他模块的 impl ...

    Impl(const std::string& dbPath) {
        // 初始化核心数据
        core_state_.rootPath = dbPath;
        std::filesystem::create_directory(dbPath);

        // 创建并初始化所有模块的内部实现
        ddl_impl_ = std::make_unique<DDLOperations::Impl>(&core_state_);
        tx_impl_  = std::make_unique<TransactionManager::Impl>(&core_state_);
        // ... 初始化其他 impl ...
    }
};


// ========================================================================
// Database 公共接口的实现
// ========================================================================

Database::Database(const std::string& dbPath) : pImpl(std::make_unique<Impl>(dbPath)) {
    // 使用 pImpl 中的内部实现来构造公开的操作类
    ddl_ops = std::make_unique<DDLOperations>(pImpl->ddl_impl_.get());
    tx_manager = std::make_unique<TransactionManager>(pImpl->tx_impl_.get());
    // ... 构造其他操作类 ...
}

Database::~Database() = default;

DDLOperations& Database::getDDLOperations() {
    return *ddl_ops;
}

DMLOperations& Database::getDMLOperations() {
    // TODO: 实现 DMLOperations
    // return *dml_ops;
    // 临时返回一个会崩溃的引用，提醒我们这里没实现
    throw std::logic_error("DMLOperations not implemented"); 
}

TransactionManager& Database::getTransactionManager() {
    return *tx_manager;
}

AccessControl& Database::getAccessControl() {
    // TODO: 实现 AccessControl
    throw std::logic_error("AccessControl not implemented");
}