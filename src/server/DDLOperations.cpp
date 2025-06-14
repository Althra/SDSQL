// DDLOperations.cpp
// 该文件实现了DMLOperations类的具体逻辑，并增强了条件评估功能

#include "DatabaseAPI.hpp" // 你的头文件名
#include <filesystem>     // 用于文件和目录操作 (需要C++17)
#include <fstream>        // 用于文件读写
#include <iostream>       // 用于在控制台打印错误信息
#include <stdexcept>      // 用于抛出异常

// 移除队友代码中的独立 DatabaseCoreImpl 结构体，因为它与 Database::Impl 冲突

/**
 * @brief DDLOperations的内部实现类 (Pimpl)，所有具体逻辑都在这里。
 */
class DDLOperations::Impl {
private:
    // 指向核心状态的指针，现在是我们的统一 Database::Impl
    Database::Impl* db_core_impl_;

public:
    // 构造函数，接收我们统一的 Database::Impl*
    explicit Impl(Database::Impl* db_core_impl) : db_core_impl_(db_core_impl) {
        if (!db_core_impl_) {
            throw std::invalid_argument("Core database implementation pointer cannot be null.");
        }
        std::cout << "DDLOperations::Impl: 已初始化。" << std::endl;
    }

    // 实现 createDatabase
    bool createDatabase(const std::string& dbName) {
        // --- 权限检查 ---
        if (!db_core_impl_->checkPermissionInternal(db_core_impl_->getCurrentUser(), PermissionType::CREATE_DATABASE, "DATABASE", dbName)) {
            throw PermissionDeniedException("CREATE_DATABASE 权限不足: 用户 '" + db_core_impl_->getCurrentUser() + "' 无法创建数据库 '" + dbName + "'。");
        }

        if (dbName.empty()) {
            std::cerr << "Error: Database name cannot be empty." << std::endl;
            return false;
        }
        // 调用 Database::Impl 中的文件操作方法
        return db_core_impl_->createDatabaseInternal(dbName);
    }

    // 实现 dropDatabase
    bool dropDatabase(const std::string& dbName) {
        // --- 权限检查 ---
        if (!db_core_impl_->checkPermissionInternal(db_core_impl_->getCurrentUser(), PermissionType::DROP_DATABASE, "DATABASE", dbName)) {
            throw PermissionDeniedException("DROP_DATABASE 权限不足: 用户 '" + db_core_impl_->getCurrentUser() + "' 无法删除数据库 '" + dbName + "'。");
        }

        if (dbName.empty()) {
            std::cerr << "Error: Database name cannot be empty." << std::endl;
            return false;
        }
        // 调用 Database::Impl 中的文件操作方法
        return db_core_impl_->dropDatabaseInternal(dbName);
    }

    // 实现 useDatabase
    bool useDatabase(const std::string& dbName) {
        // --- 权限检查 ---
        // 假设使用数据库也需要一个 USE_DATABASE 权限，这里简化为 SELECT on DATABASE 级别
        if (!db_core_impl_->checkPermissionInternal(db_core_impl_->getCurrentUser(), PermissionType::SELECT, "DATABASE", dbName)) {
            throw PermissionDeniedException("USE_DATABASE 权限不足: 用户 '" + db_core_impl_->getCurrentUser() + "' 无法使用数据库 '" + dbName + "'。");
        }

        if (dbName.empty()) {
            std::cerr << "Error: Database name cannot be empty." << std::endl;
            return false;
        }
        // 调用 Database::Impl 中的文件操作方法
        return db_core_impl_->useDatabaseInternal(dbName);
    }

    // 实现 createTable
    bool createTable(const std::string& tableName, const std::vector<ColumnDefinition>& columns) {
        // --- 权限检查 ---
        if (!db_core_impl_->checkPermissionInternal(db_core_impl_->getCurrentUser(), PermissionType::CREATE_TABLE, "TABLE", tableName)) {
            throw PermissionDeniedException("CREATE_TABLE 权限不足: 用户 '" + db_core_impl_->getCurrentUser() + "' 无法在表 '" + tableName + "' 上执行操作。");
        }

        if (db_core_impl_->currentDbName_.empty()) {
            std::cerr << "Error: No database selected. Use USE DATABASE first." << std::endl;
            return false;
        }
        if (tableName.empty() || columns.empty()) {
            std::cerr << "Error: Table name or columns cannot be empty." << std::endl;
            return false;
        }

        // DDLOperations 负责文件操作，并同步到 Database::Impl 的内存表
        return db_core_impl_->createTableFileInternal(tableName, columns);
    }

    // 实现 dropTable
    bool dropTable(const std::string& tableName) {
        // --- 权限检查 ---
        if (!db_core_impl_->checkPermissionInternal(db_core_impl_->getCurrentUser(), PermissionType::DROP_TABLE, "TABLE", tableName)) {
            throw PermissionDeniedException("DROP_TABLE 权限不足: 用户 '" + db_core_impl_->getCurrentUser() + "' 无法在表 '" + tableName + "' 上执行操作。");
        }

        if (db_core_impl_->currentDbName_.empty()) {
            std::cerr << "Error: No database selected." << std::endl;
            return false;
        }
        if (tableName.empty()) {
            std::cerr << "Error: Table name cannot be empty." << std::endl;
            return false;
        }
        // DDLOperations 负责文件操作，并同步到 Database::Impl 的内存表
        return db_core_impl_->dropTableFileInternal(tableName);
    }
    
    // alterTableAddColumn (在 DDLOperations.cpp 中缺失，但 API 里有)
    // 如果队友提供了实现，就放在这里。否则，这里只是一个桩实现或者直接移除 API 声明
    bool alterTableAddColumn(const std::string& tableName, const ColumnDefinition& column) {
        // --- 权限检查 ---
        if (!db_core_impl_->checkPermissionInternal(db_core_impl_->getCurrentUser(), PermissionType::ALTER_TABLE, "TABLE", tableName)) { // 假设 ALTER_TABLE 权限
            throw PermissionDeniedException("ALTER_TABLE 权限不足: 用户 '" + db_core_impl_->getCurrentUser() + "' 无法修改表 '" + tableName + "'。");
        }
        std::cerr << "Warning: alterTableAddColumn not fully implemented in DDLOperations.cpp. (Placeholder)" << std::endl;
        // 调用 Database::Impl 中的内部方法 (即使它是桩)
        return db_core_impl_->alterTableAddColumnInternal(tableName, column);
    }
};

// DDLOperations 公共接口的实现，将调用转发给 pImpl 对象
DDLOperations::DDLOperations(Database::Impl* db_core_impl)
    : pImpl(std::make_unique<Impl>(db_core_impl)) {}

DDLOperations::~DDLOperations() = default; // unique_ptr 会自动调用 Impl 的析构函数

bool DDLOperations::createDatabase(const std::string& dbName) {
    return pImpl->createDatabase(dbName);
}

bool DDLOperations::dropDatabase(const std::string& dbName) {
    return pImpl->dropDatabase(dbName);
}

bool DDLOperations::useDatabase(const std::string& dbName) {
    return pImpl->useDatabase(dbName);
}

bool DDLOperations::createTable(const std::string& tableName, const std::vector<ColumnDefinition>& columns) {
    return pImpl->createTable(tableName, columns);
}

bool DDLOperations::dropTable(const std::string& tableName) {
    return pImpl->dropTable(tableName);
}
// DDLOperations.cpp 队友未提供 alterTableAddColumn 的 Impl 实现，所以这里暂时不转发
// 在 DDLOperations.cpp 的 DDLOperations::Impl 中添加了 alterTableAddColumn 的实现，所以这里需要转发
bool DDLOperations::alterTableAddColumn(const std::string& tableName, const ColumnDefinition& column) {
    return pImpl->alterTableAddColumn(tableName, column);
}