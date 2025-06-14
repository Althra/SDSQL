// Database.cpp
// 核心数据库实现，连接所有模块，并处理底层文件I/O和内存数据同步

#include "DatabaseAPI.hpp" // 包含API头文件
#include <iostream>
#include <algorithm> // For std::reverse, std::remove_if
#include <functional> // For std::hash (simple password hashing, still not secure)
#include <filesystem> // For file and directory operations (C++17)
#include <fstream>    // For file I/O
#include <sstream>    // For string streams

// =======================================================================
// --- Database::Impl 的具体定义 ---
// 这是数据库的核心，包含所有表数据和低层操作
// =======================================================================
class Database::Impl {
public:
    std::string dbPath_;      // 数据库系统的根目录 (由 Database 构造函数传入)
    std::string currentDbName_; // 当前 'USE' 的数据库名

    std::map<std::string, TableData> tables_; // 内存中的表数据 (用于DML的内存表)

    // --- 事务相关成员 ---
    bool inTransaction_;
    long long currentTransactionId_;
    long long nextTransactionId_;
    std::vector<LogEntry> currentTransactionLog_;

    // --- 权限控制相关成员 ---
    std::map<std::string, User> users_; // 用户列表 (username -> User)
    std::vector<PermissionEntry> permissions_; // 权限列表
    std::string currentUser_;           // 当前登录的用户名 (空字符串表示未登录)

    // --- 构造函数和析构函数 ---
    explicit Impl(const std::string& dbPath) :
        dbPath_(dbPath),
        currentDbName_(""), // 初始无选中数据库
        inTransaction_(false),
        currentTransactionId_(0),
        nextTransactionId_(1),
        currentUser_("")
    {
        std::cout << "Database::Impl: 正在初始化数据库在 " << dbPath_ << std::endl;
        // 确保根路径存在
        if (!std::filesystem::exists(dbPath_)) {
            std::filesystem::create_directories(dbPath_);
            std::cout << "Database::Impl: 已创建数据库根目录: " << dbPath_ << std::endl;
        }

        // 加载用户和权限（简化：从文件加载或硬编码）
        loadUsersAndPermissions();
        // 创建一个默认管理员用户（如果不存在）
        if (users_.find("admin") == users_.end()) {
            createUserInternal("admin", "adminpass"); // 实际应在安全模式下进行密码哈希
            // 默认管理员拥有所有权限，不需要通过 AccessControl::Impl 来授予，直接添加到 permissions_
            // 否则会陷入鸡生蛋蛋生鸡的问题（谁来授予管理员权限）
            PermissionEntry adminAllPerm;
            adminAllPerm.username = "admin";

            adminAllPerm.objectType = "DATABASE"; adminAllPerm.objectName = "";
            adminAllPerm.permission = PermissionType::CREATE_TABLE; permissions_.push_back(adminAllPerm);
            adminAllPerm.permission = PermissionType::DROP_TABLE; permissions_.push_back(adminAllPerm);
            adminAllPerm.permission = PermissionType::SELECT; permissions_.push_back(adminAllPerm);
            adminAllPerm.permission = PermissionType::INSERT; permissions_.push_back(adminAllPerm);
            adminAllPerm.permission = PermissionType::UPDATE; permissions_.push_back(adminAllPerm);
            adminAllPerm.permission = PermissionType::DELETE; permissions_.push_back(adminAllPerm);
            adminAllPerm.permission = PermissionType::CREATE_DATABASE; permissions_.push_back(adminAllPerm);
            adminAllPerm.permission = PermissionType::DROP_DATABASE; permissions_.push_back(adminAllPerm);
            adminAllPerm.permission = PermissionType::ALTER_TABLE; permissions_.push_back(adminAllPerm); // 新增 ALTER_TABLE

            adminAllPerm.objectType = "SYSTEM"; adminAllPerm.objectName = "";
            adminAllPerm.permission = PermissionType::CREATE_USER; permissions_.push_back(adminAllPerm);
            adminAllPerm.permission = PermissionType::DROP_USER; permissions_.push_back(adminAllPerm);
            adminAllPerm.permission = PermissionType::GRANT_PERMISSION; permissions_.push_back(adminAllPerm);
            adminAllPerm.permission = PermissionType::REVOKE_PERMISSION; permissions_.push_back(adminAllPerm);

            std::cout << "Database::Impl: 已创建默认管理员用户 'admin' 并授予所有权限。" << std::endl;
        }
        
        // 登录默认管理员，以便DML/DDL操作能够执行
        // authenticateInternal("admin", "adminpass"); // 不在这里调用，由AccessControl来做
        setCurrentUser("admin"); // 直接设置，因为是内部初始化
        std::cout << "Database::Impl: 默认管理员 'admin' 已自动设置为当前用户。" << std::endl;

        // 在实际数据库中，这里会加载所有数据库和表的元数据到内存
    }

    ~Impl() {
        // 析构函数中不应抛出异常，因此需要捕获 rollbackInternal 中的异常
        if (inTransaction_) {
            std::cerr << "警告: 数据库关闭时有未提交的事务 " << currentTransactionId_ << "，尝试自动回滚。" << std::endl;
            try {
                rollbackInternal();
            } catch (const DatabaseException& e) {
                std::cerr << "回滚异常 (在析构函数中捕获): " << e.what() << std::endl;
            }
        }
        // 持久化用户和权限
        saveUsersAndPermissions();
        // 实际数据库中，这里会进行所有内存中脏数据的持久化
        std::cout << "Database::Impl: 数据库已关闭。" << std::endl;
    }

    // --- DDL 内部辅助方法 (负责文件操作和与内存表同步) ---
    // 注意：这里的 internal 方法不进行权限检查，权限检查由 DDLOperations::Impl 完成

    bool createDatabaseInternal(const std::string& dbName) {
        std::filesystem::path dbPath = std::filesystem::path(dbPath_) / dbName;
        try {
            if (std::filesystem::exists(dbPath)) {
                std::cerr << "Error: Database '" << dbName << "' already exists." << std::endl;
                return false;
            }
            std::cout << "Database::Impl: 正在创建数据库目录: " << dbPath << std::endl;
            return std::filesystem::create_directory(dbPath);
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "Filesystem error: " << e.what() << std::endl;
            return false;
        }
    }

    bool dropDatabaseInternal(const std::string& dbName) {
        std::filesystem::path dbPath = std::filesystem::path(dbPath_) / dbName;
        try {
            if (!std::filesystem::exists(dbPath)) {
                std::cerr << "Error: Database '" << dbName << "' does not exist." << std::endl;
                return false;
            }
            if (currentDbName_ == dbName) {
                currentDbName_.clear();
            }
            std::cout << "Database::Impl: 正在删除数据库目录: " << dbPath << std::endl;
            std::filesystem::remove_all(dbPath);
            // 同时从内存中移除该数据库下的所有表
            // (这里的逻辑需要更精确，假设表名是唯一的，并且所有属于该db的表都在tables_中)
            std::vector<std::string> tablesToRemove;
            for(auto const& [name, data] : tables_) {
                // 更严谨的做法是 TableData 结构中包含其所属的数据库名
                // 这里为简化，假设所有表都属于当前操作的数据库
                tablesToRemove.push_back(name);
            }
            for (const auto& name : tablesToRemove) {
                // 如果能判断出表属于被删除的数据库，则从内存中移除
                tables_.erase(name);
            }
            return true;
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "Filesystem error: " << e.what() << std::endl;
            return false;
        }
    }

    bool useDatabaseInternal(const std::string& dbName) {
        std::filesystem::path dbPath = std::filesystem::path(dbPath_) / dbName;
        if (std::filesystem::is_directory(dbPath)) {
            currentDbName_ = dbName;
            // 切换数据库时，可以考虑加载该数据库下的所有表元数据到内存 tables_
            // 为了简化，这里不自动加载，DML操作会按需处理 TableNotFound
            std::cout << "Database::Impl: 已切换当前数据库到 '" << dbName << "'。" << std::endl;
            return true;
        } else {
            std::cerr << "Error: Database '" << dbName << "' not found." << std::endl;
            return false;
        }
    }

    bool createTableFileInternal(const std::string& tableName, const std::vector<ColumnDefinition>& columns) {
        if (currentDbName_.empty()) {
            std::cerr << "Error: No database selected. Use USE DATABASE first." << std::endl;
            return false;
        }
        std::filesystem::path tableMetaPath = std::filesystem::path(dbPath_) / currentDbName_ / (tableName + ".meta");
        std::filesystem::path tableDataPath = std::filesystem::path(dbPath_) / currentDbName_ / (tableName + ".dat");
        std::filesystem::path tableIdxPath = std::filesystem::path(dbPath_) / currentDbName_ / (tableName + ".idx"); // 索引文件

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
                std::ofstream indexFile(tableIdxPath); // 仅创建文件，不写入内容
                if (!indexFile.is_open()) {
                    std::cerr << "Warning: Could not create index file for table '" << tableName << "'." << std::endl;
                }
            }

            std::ofstream dataFile(tableDataPath); // 创建数据文件
            if (!dataFile.is_open()) {
                std::cerr << "Warning: Could not create data file for table '" << tableName << "'." << std::endl;
            }
            
            // 同时在内存中创建 TableData 结构
            TableData newTable;
            newTable.tableName = tableName;
            newTable.columns = columns;
            tables_[tableName] = newTable; // 添加到内存映射

            std::cout << "Database::Impl: 文件和内存中创建表 '" << tableName << "' 成功。" << std::endl;
            return true;
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "Filesystem error: " << e.what() << std::endl;
            return false;
        }
    }

    bool dropTableFileInternal(const std::string& tableName) {
        if (currentDbName_.empty()) {
            std::cerr << "Error: No database selected." << std::endl;
            return false;
        }
        std::filesystem::path dbPath = std::filesystem::path(dbPath_) / currentDbName_;
        std::filesystem::path tableMetaPath = dbPath / (tableName + ".meta");
        std::filesystem::path tableIdxPath = dbPath / (tableName + ".idx");
        std::filesystem::path tableDataPath = dbPath / (tableName + ".dat");
        try {
            if (!std::filesystem::exists(tableMetaPath)) {
                std::cerr << "Error: Table '" << tableName << "' does not exist." << std::endl;
                return false;
            }
            bool success = true;
            std::cout << "Database::Impl: 正在删除表文件: " << tableName << std::endl;
            if (std::filesystem::exists(tableMetaPath)) success &= std::filesystem::remove(tableMetaPath);
            if (std::filesystem::exists(tableDataPath)) success &= std::filesystem::remove(tableDataPath);
            if (std::filesystem::exists(tableIdxPath)) success &= std::filesystem::remove(tableIdxPath);

            // 同时从内存中移除 TableData 结构
            tables_.erase(tableName);

            return success;
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "Filesystem error: " << e.what() << std::endl;
            return false;
        }
    }

    // alterTableAddColumnInternal (桩实现，DDLOperations.cpp 队友未提供具体实现)
    bool alterTableAddColumnInternal(const std::string& tableName, const ColumnDefinition& column) {
        std::cerr << "Warning: alterTableAddColumnInternal not fully implemented in Database.cpp. (Placeholder)" << std::endl;
        TableData* table = getMutableTableData(tableName);
        if (!table) {
            std::cerr << "Error: Table '" << tableName << "' not found in memory for alter operation." << std::endl;
            return false;
        }
        for (const auto& col_def : table->columns) {
            if (col_def.name == column.name) {
                std::cerr << "Error: Column '" << column.name << "' already exists in table '" << tableName << "'." << std::endl;
                return false;
            }
        }
        table->columns.push_back(column);
        for (Row& row : table->rows) {
            row.push_back(""); // 添加默认值
        }
        std::cout << "Database::Impl: 内存中修改表 '" << tableName << "' 添加列 '" << column.name << "' 成功。(桩实现)" << std::endl;
        // 实际还需要更新 .meta 文件和 .dat 文件
        return true;
    }


    // --- DML 内部辅助方法 ---
    // (这些方法由 DMLOperations::Impl 调用，不直接处理文件 I/O，只操作内存中的 tables_)

    // --- 事务管理内部辅助方法 (桩实现) ---
    void beginTransactionInternal() {
        if (inTransaction_) {
            throw DatabaseException("Cannot start a new transaction, another transaction is already active (ID: " + std::to_string(currentTransactionId_) + ").");
        }
        currentTransactionId_ = nextTransactionId_++;
        inTransaction_ = true;
        currentTransactionLog_.clear();
        logOperation(LogEntry(currentTransactionId_, LogEntryType::BEGIN_TRANSACTION));
        std::cout << "Database::Impl: 事务 " << currentTransactionId_ << " 开始 (桩实现)。" << std::endl;
    }

    void commitInternal() {
        if (!inTransaction_) {
            throw DatabaseException("Cannot commit: No active transaction.");
        }
        logOperation(LogEntry(currentTransactionId_, LogEntryType::COMMIT_TRANSACTION));
        currentTransactionLog_.clear();
        inTransaction_ = false;
        std::cout << "Database::Impl: 事务 " << currentTransactionId_ << " 已提交 (桩实现)，日志已清除。" << std::endl;
        currentTransactionId_ = 0;
    }

    void rollbackInternal() {
        if (!inTransaction_) {
            throw DatabaseException("Cannot rollback: No active transaction.");
        }
        logOperation(LogEntry(currentTransactionId_, LogEntryType::ROLLBACK_TRANSACTION));

        std::cout << "Database::Impl: 正在回滚事务 " << currentTransactionId_ << "... (桩实现)" << std::endl;
        std::reverse(currentTransactionLog_.begin(), currentTransactionLog_.end());
        for (const auto& entry : currentTransactionLog_) {
            std::cout << "  - 回滚日志条目 (桩实现) - Type: " << static_cast<int>(entry.type) << ", Table: " << entry.tableName << std::endl;
            TableData* table = getMutableTableData(entry.tableName);
            if (table) {
                if (entry.type == LogEntryType::INSERT) {
                    auto it = std::find(table->rows.begin(), table->rows.end(), entry.newRowValues);
                    if (it != table->rows.end()) {
                        table->rows.erase(it);
                    }
                } else if (entry.type == LogEntryType::DELETE) {
                    table->rows.push_back(entry.oldRowValues);
                } else if (entry.type == LogEntryType::UPDATE_OLD_VALUE) {
                    if (entry.rowIndex != -1 && entry.rowIndex < table->rows.size()) {
                        table->rows[entry.rowIndex] = entry.oldRowValues;
                    }
                }
            }
        }
        currentTransactionLog_.clear();
        inTransaction_ = false;
        std::cout << "Database::Impl: 事务 " << currentTransactionId_ << " 已回滚 (桩实现)，日志已清除。" << std::endl;
        currentTransactionId_ = 0;
    }

    void logOperation(const LogEntry& entry) {
        if (inTransaction_) {
            currentTransactionLog_.push_back(entry);
        }
    }


    // --- 权限控制内部辅助方法 ---

    // 从文件加载用户和权限 (桩实现)
    void loadUsersAndPermissions() {
        std::cout << "Database::Impl: 加载用户和权限 (桩实现)。" << std::endl;
        // 可以在这里实现从文件加载用户和权限的逻辑
    }

    // 将用户和权限保存到文件 (桩实现)
    void saveUsersAndPermissions() {
        std::cout << "Database::Impl: 保存用户和权限 (桩实现)。" << std::endl;
    }

    bool authenticateInternal(const std::string& username, const std::string& password) {
        auto it = users_.find(username);
        if (it != users_.end()) {
            return it->second.passwordHash == password;
        }
        return false;
    }

    bool checkPermissionInternal(const std::string& username, PermissionType permission, const std::string& objectType, const std::string& objectName = "") {
        if (username.empty()) {
            return false;
        }

        for (const auto& entry : permissions_) {
            if (entry.username == username &&
                entry.permission == permission &&
                entry.objectType == objectType) {
                
                if (!objectName.empty()) {
                    if (entry.objectName == objectName) { // 精确匹配
                         return true; 
                    }
                } else { // 如果 objectName 为空，表示对该类型的所有对象都有权限
                    return true;
                }
            }
        }
        return false;
    }

    bool createUserInternal(const std::string& username, const std::string& password) {
        if (users_.count(username)) {
            std::cerr << "错误: 用户 '" << username << "' 已存在。" << std::endl;
            return false;
        }
        User newUser;
        newUser.username = username;
        newUser.passwordHash = password;
        users_[username] = newUser;
        std::cout << "Database::Impl: 内部创建用户 '" << username << "' 成功。" << std::endl;
        return true;
    }

    bool dropUserInternal(const std::string& username) {
        if (username == currentUser_) {
            std::cerr << "错误: 无法删除当前登录用户 '" << username << "'。" << std::endl;
            return false;
        }
        if (users_.erase(username) > 0) {
            permissions_.erase(std::remove_if(permissions_.begin(), permissions_.end(),
                                               [&](const PermissionEntry& entry){
                                                   return entry.username == username;
                                               }),
                               permissions_.end());
            std::cout << "Database::Impl: 内部删除用户 '" << username << "' 及其所有权限成功。" << std::endl;
            return true;
        }
        std::cerr << "错误: 删除用户 '" << username << "' 失败，用户不存在。" << std::endl;
        return false;
    }

    bool grantPermissionInternal(const std::string& username, PermissionType permission, const std::string& objectType, const std::string& objectName = "") {
        if (users_.find(username) == users_.end()) {
            std::cerr << "错误: 授予权限失败，用户 '" << username << "' 不存在。" << std::endl;
            return false;
        }

        for (const auto& entry : permissions_) {
            if (entry.username == username && entry.permission == permission &&
                entry.objectType == objectType && entry.objectName == objectName) {
                std::cerr << "警告: 权限已存在，无需重复授予。" << std::endl;
                return true;
            }
        }

        PermissionEntry newEntry;
        newEntry.username = username;
        newEntry.permission = permission;
        newEntry.objectType = objectType;
        newEntry.objectName = objectName;
        permissions_.push_back(newEntry);
        std::cout << "Database::Impl: 内部授予用户 '" << username << "' 权限成功。" << std::endl;
        return true;
    }

    bool revokePermissionInternal(const std::string& username, PermissionType permission, const std::string& objectType, const std::string& objectName = "") {
        auto initialSize = permissions_.size();
        permissions_.erase(std::remove_if(permissions_.begin(), permissions_.end(),
                                           [&](const PermissionEntry& entry){
                                               return entry.username == username &&
                                                      entry.permission == permission &&
                                                      entry.objectType == objectType &&
                                                      entry.objectName == objectName;
                                           }),
                           permissions_.end());
        if (permissions_.size() < initialSize) {
            std::cout << "Database::Impl: 内部撤销用户 '" << username << "' 权限成功。" << std::endl;
            return true;
        }
        std::cerr << "错误: 撤销权限失败，权限不存在或用户不存在。" << std::endl;
        return false;
    }

    const std::string& getCurrentUser() const { return currentUser_; }
    void setCurrentUser(const std::string& username) { currentUser_ = username; }
};

// =======================================================================
// --- DDLOperations 类的公共接口实现 (转发到 Pimpl Impl) ---
// =======================================================================
// DDLOperations 类的构造函数和析构函数实现
DDLOperations::DDLOperations(Database::Impl* db_core_impl) : pImpl(std::make_unique<Impl>(db_core_impl)) {}
DDLOperations::~DDLOperations() = default;

// DDLOperations 公开接口的实现，将调用转发给 pImpl 对象
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
// 如果 DDLOperations 后面会实现 alterTableAddColumn，这里也要添加转发
// bool DDLOperations::alterTableAddColumn(const std::string& tableName, const ColumnDefinition& column) {
//     return pImpl->alterTableAddColumn(tableName, column);
// }


// =======================================================================
// --- TransactionManager 类的公共接口实现 (桩实现) ---
// =======================================================================
// TransactionManager 类的构造函数和析构函数实现
TransactionManager::TransactionManager(Database::Impl* db_core_impl) : pImpl(std::make_unique<Impl>(db_core_impl)) {}
TransactionManager::~TransactionManager() = default;
void TransactionManager::beginTransaction() { pImpl->beginTransaction(); }
void TransactionManager::commit() { pImpl->commit(); }
void TransactionManager::rollback() { pImpl->rollback(); }


// =======================================================================
// --- AccessControl 类的公共接口实现 (转发到 Pimpl Impl) ---
// =======================================================================
// AccessControl 类的构造函数和析构函数实现
AccessControl::AccessControl(Database::Impl* db_core_impl) : pImpl(std::make_unique<Impl>(db_core_impl)) {}
AccessControl::~AccessControl() = default;
bool AccessControl::login(const std::string& username, const std::string& password) { return pImpl->login(username, password); }
bool AccessControl::logout() { return pImpl->logout(); }
bool AccessControl::createUser(const std::string& username, const std::string& password) { return pImpl->createUser(username, password); }
bool AccessControl::dropUser(const std::string& username) { return pImpl->dropUser(username); }
bool AccessControl::grantPermission(const std::string& username, PermissionType permission, const std::string& objectType, const std::string& objectName) { return pImpl->grantPermission(username, permission, objectType, objectName); }
bool AccessControl::revokePermission(const std::string& username, PermissionType permission, const std::string& objectType, const std::string& objectName) { return pImpl->revokePermission(username, permission, objectType, objectName); }


// =======================================================================
// --- Database 类公共接口的实现 ---
// =======================================================================
Database::Database(const std::string& dbPath) : pImpl(std::make_unique<Impl>(dbPath)) {
    // 初始化各个操作模块，并传入指向核心Impl的指针
    ddl_ops = std::make_unique<DDLOperations>(pImpl.get());
    dml_ops = std::make_unique<DMLOperations>(pImpl.get());
    tx_manager = std::make_unique<TransactionManager>(pImpl.get());
    ac_manager = std::make_unique<AccessControl>(pImpl.get());
}

Database::~Database() = default; // unique_ptr 会自动调用 Impl 的析构函数

DDLOperations& Database::getDDLOperations() { return *ddl_ops; }
DMLOperations& Database::getDMLOperations() { return *dml_ops; }
TransactionManager& Database::getTransactionManager() { return *tx_manager; }
AccessControl& Database::getAccessControl() { return *ac_manager; }