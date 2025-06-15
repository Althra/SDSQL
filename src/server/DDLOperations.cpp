#include "../../include/server/DatabaseAPI.hpp"
#include <filesystem>     // 用于文件和目录操作 (需要C++17)
#include <fstream>        // 用于文件读写
#include <iostream>       // 用于在控制台打印错误信息
#include <stdexcept>      // 用于抛出异常

// 这是一个假设的核心实现类，用于存储数据库的全局状态。
// 在你的实际项目中，它可能是 class Database::Impl。
namespace {
    struct DatabaseCoreImpl {
        std::string rootPath;      // 数据库系统的根目录
        std::string currentDbName; // 当前 'USE' 的数据库名
    };
}

/**
 * @brief DDLOperations的内部实现类 (Pimpl)，所有具体逻辑都在这里。
 */
class DDLOperations::Impl {
private:
    // 指向核心状态的指针
    DatabaseCoreImpl* core_impl_;

public:
    // 构造函数
    explicit Impl(DatabaseCoreImpl* core_impl) : core_impl_(core_impl) {
        if (!core_impl_) {
            throw std::invalid_argument("Core implementation pointer cannot be null.");
        }
    }

    // 实现 createDatabase
    bool createDatabase(const std::string& dbName) {
        if (dbName.empty()) {
            std::cerr << "Error: Database name cannot be empty." << std::endl;
            return false;
        }
        std::filesystem::path dbPath = std::filesystem::path(core_impl_->rootPath) / dbName;
        try {
            if (std::filesystem::exists(dbPath)) {
                std::cerr << "Error: Database '" << dbName << "' already exists." << std::endl;
                return false;
            }
            return std::filesystem::create_directory(dbPath);
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "Filesystem error: " << e.what() << std::endl;
            return false;
        }
    }

    // 实现 dropDatabase
    bool dropDatabase(const std::string& dbName) {
        if (dbName.empty()) {
            std::cerr << "Error: Database name cannot be empty." << std::endl;
            return false;
        }
        std::filesystem::path dbPath = std::filesystem::path(core_impl_->rootPath) / dbName;
        try {
            if (!std::filesystem::exists(dbPath)) {
                std::cerr << "Error: Database '" << dbName << "' does not exist." << std::endl;
                return false;
            }
            if (core_impl_->currentDbName == dbName) {
                core_impl_->currentDbName.clear();
            }
            std::filesystem::remove_all(dbPath);
            return true;
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "Filesystem error: " << e.what() << std::endl;
            return false;
        }
    }

    // 实现 useDatabase
    bool useDatabase(const std::string& dbName) {
        if (dbName.empty()) {
            std::cerr << "Error: Database name cannot be empty." << std::endl;
            return false;
        }
        std::filesystem::path dbPath = std::filesystem::path(core_impl_->rootPath) / dbName;
        if (std::filesystem::is_directory(dbPath)) {
            core_impl_->currentDbName = dbName;
            return true;
        } else {
            std::cerr << "Error: Database '" << dbName << "' not found." << std::endl;
            return false;
        }
    }

    // 实现 createTable
    bool createTable(const std::string& tableName, const std::vector<ColumnDefinition>& columns) {
        if (core_impl_->currentDbName.empty()) {
            std::cerr << "Error: No database selected." << std::endl;
            return false;
        }
        if (tableName.empty() || columns.empty()) {
            std::cerr << "Error: Table name or columns cannot be empty." << std::endl;
            return false;
        }
        std::filesystem::path tableMetaPath = std::filesystem::path(core_impl_->rootPath) / core_impl_->currentDbName / (tableName + ".meta");
        try {
            if (std::filesystem::exists(tableMetaPath)) {
                std::cerr << "Error: Table '" << tableName << "' already exists." << std::endl;
                return false;
            }
            std::ofstream metaFile(tableMetaPath);
            if (!metaFile.is_open()) {
                std::cerr << "Error: Could not create metadata file for table '" << tableName << "'." << std::endl;
                return false;
            }
            bool hasPrimaryKey = false;
            for (const auto& col : columns) {
                metaFile << col.name << "," << static_cast<int>(col.type) << "," << (col.isPrimaryKey ? "1" : "0") << "\n";
                if (col.isPrimaryKey) {
                    if (hasPrimaryKey) {
                        std::cerr << "Error: Multiple primary keys defined for table '" << tableName << "'." << std::endl;
                        metaFile.close();
                        std::filesystem::remove(tableMetaPath);
                        return false;
                    }
                    hasPrimaryKey = true;
                }
            }
            metaFile.close();
            if (hasPrimaryKey) {
                std::filesystem::path indexPath = std::filesystem::path(core_impl_->rootPath) / core_impl_->currentDbName / (tableName + ".idx");
                std::ofstream indexFile(indexPath);
            }
            std::filesystem::path dataPath = std::filesystem::path(core_impl_->rootPath) / core_impl_->currentDbName / (tableName + ".dat");
            std::ofstream dataFile(dataPath);
            return true;
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "Filesystem error: " << e.what() << std::endl;
            return false;
        }
    }

    // 实现 dropTable
    bool dropTable(const std::string& tableName) {
        if (core_impl_->currentDbName.empty()) {
            std::cerr << "Error: No database selected." << std::endl;
            return false;
        }
        if (tableName.empty()) {
            std::cerr << "Error: Table name cannot be empty." << std::endl;
            return false;
        }
        std::filesystem::path dbPath = std::filesystem::path(core_impl_->rootPath) / core_impl_->currentDbName;
        std::filesystem::path tableMetaPath = dbPath / (tableName + ".meta");
        std::filesystem::path tableIdxPath = dbPath / (tableName + ".idx");
        std::filesystem::path tableDataPath = dbPath / (tableName + ".dat");
        try {
            if (!std::filesystem::exists(tableMetaPath)) {
                std::cerr << "Error: Table '" << tableName << "' does not exist." << std::endl;
                return false;
            }
            bool success = true;
            if (std::filesystem::exists(tableMetaPath)) success &= std::filesystem::remove(tableMetaPath);
            if (std::filesystem::exists(tableDataPath)) success &= std::filesystem::remove(tableDataPath);
            if (std::filesystem::exists(tableIdxPath)) success &= std::filesystem::remove(tableIdxPath);
            return success;
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "Filesystem error: " << e.what() << std::endl;
            return false;
        }
    }
};

// DDLOperations 公共接口的实现，将调用转发给 pImpl 对象
DDLOperations::DDLOperations(DDLOperations::Impl* db_core_impl) 
    : pImpl(db_core_impl) {}

DDLOperations::~DDLOperations() = default;

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