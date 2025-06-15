// src/server/Database.cpp

#include "../../include/server/DatabaseAPI.hpp"
#include <algorithm> // 用于 std::find_if
#include <filesystem>
#include <fstream> // 用于文件读写
#include <iostream>
#include <stdexcept> // 用于 std::runtime_error



// ========================================================================
// Database 类的 Pimpl 实现
// ========================================================================
class Database::Impl {
public:
  // 持有真正的核心数据 (原 DatabaseCoreImpl 的成员)
  std::string rootPath;
  std::string currentDbName;
  bool isTransactionActive = false;
  std::string transactionLogPath;
  // DMLOperations* dml_ops_ = nullptr; // 已移除，DML现在通过 unique_ptr 管理

  // 新增：用于在内存中存储已加载的表数据
  std::map<std::string, TableData> loadedTables_;
  // 新增：用于存储用户账户信息
  std::map<std::string, User> users_;
  // 新增：当前登录的用户
  std::string currentUser_;
  // 新增：当前的事务ID
  long currentTransactionId_ = 0;

  // 持有所有模块的内部实现
  std::unique_ptr<DDLOperations::Impl> ddl_impl_;
  std::unique_ptr<DMLOperations::Impl> dml_impl_; // 新增
  std::unique_ptr<TransactionManager::Impl> tx_impl_;
  std::unique_ptr<AccessControl::Impl> ac_impl_; // 新增

  Impl(const std::string &dbPath) {
    // 初始化核心数据
    rootPath = dbPath;
    if (!std::filesystem::exists(dbPath)) {
      std::filesystem::create_directory(dbPath);
      std::cout << "Database root directory created: " << dbPath << std::endl;
    } else {
      std::cout << "Database root directory already exists: " << dbPath
                << std::endl;
    }

    // 尝试加载用户数据 (例如从 users.meta 文件)
    loadUsers();

    // 如果没有用户，创建默认管理员用户
    if (users_.empty()) {
      std::cout << "No users found, creating default admin user." << std::endl;
      createUserInternal("admin", "admin_password"); // 默认密码
      // 授予 admin 所有系统权限
      grantPermissionInternal("admin", PermissionType::CREATE_USER, "SYSTEM");
      grantPermissionInternal("admin", PermissionType::DROP_USER, "SYSTEM");
      grantPermissionInternal("admin", PermissionType::GRANT_PERMISSION,
                              "SYSTEM");
      grantPermissionInternal("admin", PermissionType::REVOKE_PERMISSION,
                              "SYSTEM");
      // admin
      // 拥有所有表操作权限（为了简化，这里可以视为对所有表拥有权限，或在后续登录后动态赋予）
      grantPermissionInternal("admin", PermissionType::SELECT, "TABLE",
                              ""); // 对所有表拥有 SELECT 权限
      grantPermissionInternal("admin", PermissionType::INSERT, "TABLE",
                              ""); // 对所有表拥有 INSERT 权限
      grantPermissionInternal("admin", PermissionType::UPDATE, "TABLE",
                              ""); // 对所有表拥有 UPDATE 权限
      grantPermissionInternal("admin", PermissionType::DELETE, "TABLE",
                              ""); // 对所有表拥有 DELETE 权限
      grantPermissionInternal("admin", PermissionType::CREATE_TABLE, "TABLE",
                              ""); // 对所有表拥有 CREATE_TABLE 权限
      grantPermissionInternal("admin", PermissionType::DROP_TABLE, "TABLE",
                              ""); // 对所有表拥有 DROP_TABLE 权限

      saveUsers(); // 保存新创建的 admin 用户
    }

    // 创建并初始化所有模块的内部实现
    ddl_impl_ = std::make_unique<DDLOperations::Impl>(this); // 传递 this 指针
    dml_impl_ = std::make_unique<DMLOperations::Impl>(this); // 传递 this 指针
    tx_impl_ =
        std::make_unique<TransactionManager::Impl>(this); // 传递 this 指针
    ac_impl_ = std::make_unique<AccessControl::Impl>(this); // 传递 this 指针
  }

  ~Impl() {
    // 在销毁前保存所有用户数据
    saveUsers();
    // 在销毁前将所有内存中的表数据持久化到文件 (如果需要的话)
    // 目前我们的 DML 操作是直接修改内存，但 commit 时会写入文件
    // 确保在程序关闭时，所有未提交的事务被回滚或提示
    if (isTransactionActive) {
      std::cerr << "Warning: Database shutting down with an active "
                   "transaction. Transaction will be rolled back."
                << std::endl;
      // 简单回滚，实际可能需要更复杂的恢复逻辑
      // tx_impl_->rollback(); // 直接调用会导致循环依赖，或者由 Database
      // 负责清理
      std::filesystem::remove(transactionLogPath); // 删除日志文件
      isTransactionActive = false;
    }
    // 清理 loadedTables_ 中的数据
    loadedTables_.clear();
  }

  // --- 辅助方法，供 DDL, DML, Transaction, AccessControl 的 Impl 使用 ---

  // 获取可修改的 TableData 指针
  TableData *getMutableTableData(const std::string &tableName) {
    if (currentDbName.empty()) {
      std::cerr << "Error: No database selected." << std::endl;
      return nullptr;
    }
    std::string fullTableName = currentDbName + "/" + tableName;
    // 如果表已经在内存中，直接返回
    auto it = loadedTables_.find(fullTableName);
    if (it != loadedTables_.end()) {
      return &(it->second);
    }
    // 否则尝试从文件加载
    TableData loadedTable;
    if (loadTableFromFile(tableName, loadedTable)) {
      loadedTables_[fullTableName] = loadedTable; // 移动语义
      return &(loadedTables_[fullTableName]);
    }
    return nullptr;
  }

  // 获取只读的 TableData 指针
  const TableData *getTableData(const std::string &tableName) const {
    if (currentDbName.empty()) {
      std::cerr << "Error: No database selected." << std::endl;
      return nullptr;
    }
    std::string fullTableName = currentDbName + "/" + tableName;
    auto it = loadedTables_.find(fullTableName);
    if (it != loadedTables_.end()) {
      return &(it->second);
    }
    // 尝试从文件加载（只读操作也可能需要加载）
    // 注意：这里需要一个非 const 的方法来加载并插入到 loadedTables_
    // 对于 const getTableData，如果不在内存中，应该报错或返回空
    std::cerr << "Error: Table '" << tableName
              << "' not loaded into memory for read access." << std::endl;
    return nullptr;
  }

  // 将内存中的表数据持久化到文件
  bool saveTableToFile(const std::string &tableName) {
    if (currentDbName.empty()) {
      std::cerr << "Error: No database selected to save table." << std::endl;
      return false;
    }
    std::string fullTableName = currentDbName + "/" + tableName;
    auto it = loadedTables_.find(fullTableName);
    if (it == loadedTables_.end()) {
      std::cerr << "Error: Table '" << tableName
                << "' not found in memory to save." << std::endl;
      return false;
    }

    const TableData &table = it->second;
    std::filesystem::path dataFilePath =
        std::filesystem::path(rootPath) / currentDbName / (table.name + ".dat");
    std::ofstream dataFile(dataFilePath);
    if (!dataFile.is_open()) {
      std::cerr << "Error: Could not open data file for writing: "
                << dataFilePath << std::endl;
      return false;
    }

    for (const auto &row : table.rows) {
      for (size_t i = 0; i < row.size(); ++i) {
        dataFile << row[i];
        if (i < row.size() - 1) {
          dataFile << ","; // CSV 格式
        }
      }
      dataFile << "\n";
    }
    dataFile.close();
    std::cout << "Table '" << tableName << "' data saved to file." << std::endl;
    return true;
  }

  // 从文件加载表数据到内存
  bool loadTableFromFile(const std::string &tableName, TableData &outTable) {
    if (currentDbName.empty()) {
      std::cerr << "Error: No database selected to load table." << std::endl;
      return false;
    }
    std::filesystem::path metaFilePath =
        std::filesystem::path(rootPath) / currentDbName / (tableName + ".meta");
    std::filesystem::path dataFilePath =
        std::filesystem::path(rootPath) / currentDbName / (tableName + ".dat");

    if (!std::filesystem::exists(metaFilePath)) {
      std::cerr << "Error: Table metadata file '" << tableName
                << ".meta' not found." << std::endl;
      return false;
    }

    outTable.name = tableName;
    outTable.columns.clear();
    outTable.rows.clear();

    // 加载元数据
    std::ifstream metaFile(metaFilePath);
    std::string line;
    while (std::getline(metaFile, line)) {
      std::stringstream ss(line);
      std::string name, type_str, is_pk_str;
      std::getline(ss, name, ',');
      std::getline(ss, type_str, ',');
      std::getline(ss, is_pk_str);

      DataType type;
      // 假设 DataType 的 int 值与实际一致
      int type_int = std::stoi(type_str);
      if (type_int == static_cast<int>(DataType::INT))
        type = DataType::INT;
      else if (type_int == static_cast<int>(DataType::DOUBLE))
        type = DataType::DOUBLE;
      else if (type_int == static_cast<int>(DataType::STRING))
        type = DataType::STRING;
      else if (type_int == static_cast<int>(DataType::BOOL))
        type = DataType::BOOL;
      else {
        std::cerr << "Error: Unknown data type in meta file: " << type_str
                  << std::endl;
        return false;
      }
      bool is_pk = (is_pk_str == "1");
      outTable.columns.emplace_back(name, type, is_pk);
    }
    metaFile.close();

    // 加载数据
    if (std::filesystem::exists(dataFilePath)) {
      std::ifstream dataFile(dataFilePath);
      while (std::getline(dataFile, line)) {
        if (line.empty())
          continue; // 跳过空行
        Row row;
        std::stringstream ss(line);
        std::string cell;
        while (std::getline(ss, cell, ',')) {
          row.push_back(cell);
        }
        outTable.rows.push_back(row);
      }
      dataFile.close();
    }

    std::cout << "Table '" << tableName << "' loaded from files." << std::endl;
    return true;
  }

  // 认证用户
  bool authenticateInternal(const std::string &username,
                            const std::string &password) {
    auto it = users_.find(username);
    if (it != users_.end()) {
      // 简单哈希模拟，实际应使用更安全的哈希算法
      std::string hashedPassword = password + "_hashed"; // 模拟哈希
      return it->second.hashedPassword == hashedPassword;
    }
    return false;
  }

  // 检查用户权限
  bool checkPermissionInternal(const std::string &username,
                               PermissionType permission,
                               const std::string &objectType,
                               const std::string &objectName = "") {
    if (username.empty()) {
      std::cerr << "Error: No user logged in to check permissions."
                << std::endl;
      return false;
    }

    // 默认管理员拥有所有权限（简化处理）
    if (username == "admin") {
      return true;
    }

    auto it = users_.find(username);
    if (it != users_.end()) {
      return it->second.hasPermission(permission, objectType, objectName);
    }
    std::cerr << "Error: User '" << username
              << "' not found for permission check." << std::endl;
    return false;
  }

  // 创建用户
  bool createUserInternal(const std::string &username,
                          const std::string &password) {
    if (users_.count(username)) {
      std::cerr << "Error: User '" << username << "' already exists."
                << std::endl;
      return false;
    }
    User newUser;
    newUser.username = username;
    newUser.hashedPassword = password + "_hashed"; // 模拟哈希
    // 默认不赋予任何权限，需通过 grantPermissionInternal 赋予
    users_[username] = newUser;
    saveUsers(); // 及时保存用户数据
    std::cout << "User '" << username << "' created." << std::endl;
    return true;
  }

  // 删除用户
  bool dropUserInternal(const std::string &username) {
    if (username == "admin") {
      std::cerr << "Error: Cannot delete default admin user." << std::endl;
      return false;
    }
    if (!users_.count(username)) {
      std::cerr << "Error: User '" << username << "' does not exist."
                << std::endl;
      return false;
    }
    users_.erase(username);
    saveUsers(); // 及时保存用户数据
    // 如果删除的是当前登录用户，则强制登出
    if (currentUser_ == username) {
      currentUser_.clear();
      std::cout << "Current user '" << username
                << "' logged out due to deletion." << std::endl;
    }
    std::cout << "User '" << username << "' deleted." << std::endl;
    return true;
  }

  // 授予权限
  bool grantPermissionInternal(const std::string &username,
                               PermissionType permission,
                               const std::string &objectType,
                               const std::string &objectName = "") {
    auto it = users_.find(username);
    if (it == users_.end()) {
      std::cerr << "Error: User '" << username
                << "' not found to grant permission." << std::endl;
      return false;
    }
    User &user = it->second;
    PermissionEntry newPerm{permission, objectType, objectName};
    // 检查是否已存在该权限
    if (std::find(user.permissions.begin(), user.permissions.end(), newPerm) ==
        user.permissions.end()) {
      user.permissions.push_back(newPerm);
      saveUsers();
      std::cout << "Permission granted to user '" << username << "'."
                << std::endl;
      return true;
    }
    std::cerr << "Warning: Permission already exists for user '" << username
              << "'." << std::endl;
    return false;
  }

  // 撤销权限
  bool revokePermissionInternal(const std::string &username,
                                PermissionType permission,
                                const std::string &objectType,
                                const std::string &objectName) {
    auto it = users_.find(username);
    if (it == users_.end()) {
      std::cerr << "Error: User '" << username
                << "' not found to revoke permission." << std::endl;
      return false;
    }
    User &user = it->second;
    PermissionEntry targetPerm{permission, objectType, objectName};
    auto remove_it = std::remove_if(
        user.permissions.begin(), user.permissions.end(),
        [&](const PermissionEntry &p) { return p == targetPerm; });
    if (remove_it != user.permissions.end()) {
      user.permissions.erase(remove_it, user.permissions.end());
      saveUsers();
      std::cout << "Permission revoked from user '" << username << "'."
                << std::endl;
      return true;
    }
    std::cerr << "Warning: Permission not found for user '" << username
              << "' to revoke." << std::endl;
    return false;
  }

  // 获取当前用户
  const std::string &getCurrentUser() const { return currentUser_; }

  // 设置当前用户
  void setCurrentUser(const std::string &username) { currentUser_ = username; }

  // 记录操作到事务日志
  void logOperation(const LogEntry &entry) {
    if (!isTransactionActive) {
      std::cerr << "Error: Cannot log operation. No transaction is active."
                << std::endl;
      return;
    }
    std::ofstream logFile(transactionLogPath, std::ios::app);
    if (!logFile.is_open()) {
      std::cerr << "Error: Could not open transaction log file for logging."
                << std::endl;
      return;
    }

    logFile << entry.transactionId << ";";
    // 写入操作类型
    if (entry.type == LogEntryType::INSERT)
      logFile << "INSERT;";
    else if (entry.type == LogEntryType::UPDATE_OLD_VALUE)
      logFile << "UPDATE;";
    else if (entry.type == LogEntryType::DELETE)
      logFile << "DELETE;";
    else
      logFile << "UNKNOWN;";

    logFile << entry.tableName << ";";

    // 写入旧行数据 (UPDATE 和 DELETE)
    for (size_t i = 0; i < entry.oldRow.size(); ++i) {
      logFile << entry.oldRow[i];
      if (i < entry.oldRow.size() - 1)
        logFile << ",";
    }
    logFile << ";";

    // 写入新行数据 (INSERT 和 UPDATE)
    for (size_t i = 0; i < entry.newRow.size(); ++i) {
      logFile << entry.newRow[i];
      if (i < entry.newRow.size() - 1)
        logFile << ",";
    }
    logFile << ";";

    // 写入行索引 (UPDATE)
    logFile << entry.rowIndex << "\n";

    logFile.close();
    std::cout << "Operation logged to transaction log: "
              << static_cast<int>(entry.type) << " on table " << entry.tableName
              << std::endl;
  }

  // 从日志中应用单条操作 (在 commit 时调用)
  bool applyLogEntryInternal(const LogEntry &entry) {
    TableData *table = getMutableTableData(entry.tableName);
    if (!table) {
      std::cerr << "Error: Cannot apply log entry, table '" << entry.tableName
                << "' not found." << std::endl;
      return false;
    }

    if (entry.type == LogEntryType::INSERT) {
      table->rows.push_back(entry.newRow);
      std::cout << "Applied INSERT from log to table '" << entry.tableName
                << "'." << std::endl;
    } else if (entry.type == LogEntryType::UPDATE_OLD_VALUE) {
      // 寻找要更新的行。简化实现：根据旧行内容找到并更新。
      // 实际可能需要一个主键或行号来精准定位。
      // 目前 DML::update 已经通过 rowIndex 提供了索引，这里使用它
      if (entry.rowIndex != -1 && entry.rowIndex < table->rows.size()) {
        table->rows[entry.rowIndex] = entry.newRow;
        std::cout << "Applied UPDATE from log to table '" << entry.tableName
                  << "' at index " << entry.rowIndex << "." << std::endl;
      } else {
        std::cerr << "Error: Cannot apply UPDATE from log, invalid row index "
                     "or row not found for table '"
                  << entry.tableName << "'." << std::endl;
        return false;
      }
    } else if (entry.type == LogEntryType::DELETE) {
      // 寻找要删除的行。简化实现：根据旧行内容找到并删除。
      auto it = std::remove_if(table->rows.begin(), table->rows.end(),
                               [&](const Row &r) { return r == entry.oldRow; });
      if (it != table->rows.end()) {
        table->rows.erase(it, table->rows.end());
        std::cout << "Applied DELETE from log to table '" << entry.tableName
                  << "'." << std::endl;
      } else {
        std::cerr
            << "Error: Cannot apply DELETE from log, row not found for table '"
            << entry.tableName << "'." << std::endl;
        return false;
      }
    } else {
      std::cerr << "Error: Unknown log entry type." << std::endl;
      return false;
    }
    // 成功应用后，将修改后的表数据保存到文件
    return saveTableToFile(entry.tableName);
  }

private:
  // 将用户数据持久化到文件
  bool saveUsers() {
    std::filesystem::path usersFilePath =
        std::filesystem::path(rootPath) / "users.meta";
    std::ofstream outFile(usersFilePath);
    if (!outFile.is_open()) {
      std::cerr << "Error: Could not open users file for writing: "
                << usersFilePath << std::endl;
      return false;
    }

    for (const auto &pair : users_) {
      const User &user = pair.second;
      outFile << "USER:" << user.username << ":" << user.hashedPassword << "\n";
      for (const auto &perm : user.permissions) {
        outFile << "PERM:" << static_cast<int>(perm.type) << ":"
                << perm.objectType << ":" << perm.objectName << "\n";
      }
    }
    outFile.close();
    std::cout << "Users data saved." << std::endl;
    return true;
  }

  // 从文件加载用户数据
  bool loadUsers() {
    std::filesystem::path usersFilePath =
        std::filesystem::path(rootPath) / "users.meta";
    if (!std::filesystem::exists(usersFilePath)) {
      std::cout << "Users file not found. Starting with no users." << std::endl;
      return false;
    }

    std::ifstream inFile(usersFilePath);
    if (!inFile.is_open()) {
      std::cerr << "Error: Could not open users file for reading: "
                << usersFilePath << std::endl;
      return false;
    }

    std::string line;
    User *currentUser = nullptr;
    while (std::getline(inFile, line)) {
      std::stringstream ss(line);
      std::string type;
      std::getline(ss, type, ':');

      if (type == "USER") {
        std::string username, hashedPassword;
        std::getline(ss, username, ':');
        std::getline(ss, hashedPassword);
        users_[username] = User{username, hashedPassword};
        currentUser = &users_[username];
      } else if (type == "PERM" && currentUser) {
        std::string perm_type_str, obj_type, obj_name;
        std::getline(ss, perm_type_str, ':');
        std::getline(ss, obj_type, ':');
        std::getline(ss, obj_name);

        PermissionType p_type =
            static_cast<PermissionType>(std::stoi(perm_type_str));
        currentUser->permissions.push_back({p_type, obj_type, obj_name});
      }
    }
    inFile.close();
    std::cout << "Users data loaded." << std::endl;
    return true;
  }
};

// ========================================================================
// Database 公共接口的实现
// ========================================================================

Database::Database(const std::string &dbPath)
    : pImpl(std::make_unique<Impl>(dbPath)) {
  // 使用 pImpl 中的内部实现来构造公开的操作类
  ddl_ops =
      std::make_unique<DDLOperations>(pImpl->ddl_impl_.get()); // 传递裸指针
  dml_ops = std::make_unique<DMLOperations>(pImpl->dml_impl_.get()); // 新增
  tx_manager =
      std::make_unique<TransactionManager>(pImpl->tx_impl_.get()); // 传递裸指针
  ac_manager = std::make_unique<AccessControl>(pImpl->ac_impl_.get()); // 新增
}

Database::~Database() = default;

DDLOperations &Database::getDDLOperations() { return *ddl_ops; }

DMLOperations &Database::getDMLOperations() {
  return *dml_ops; // 已修改
}

TransactionManager &Database::getTransactionManager() { return *tx_manager; }

AccessControl &Database::getAccessControl() {
  return *ac_manager; // 已修改
}