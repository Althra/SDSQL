// DatabaseAPI.hpp
#ifndef DATABASE_API_HPP
#define DATABASE_API_HPP

#include <string>
#include <vector>
#include <map>
#include <memory> // For std::unique_ptr
#include <stdexcept>

/**
 * @brief 数据库中支持的数据类型枚举
 */
enum class DataType {
    INT,      // 整型数据
    DOUBLE,   // 浮点型数据
    STRING,   // 字符串类型数据
    BOOL      // 布尔类型数据
};

/**
 * @brief 定义表的列结构
 */
struct ColumnDefinition {
    std::string name;   // 列名
    DataType type;      // 列的数据类型
    bool isPrimaryKey = false; // 标记该列是否为主键

    // 默认构造函数
    ColumnDefinition() = default;

    // 构造函数
    ColumnDefinition(const std::string& name, DataType type, bool is_primary = false)
        : name(name), type(type), isPrimaryKey(is_primary) {}
};

/**
 * @brief 权限类型枚举
 */
enum class PermissionType {
    SELECT,         // 查询权限
    INSERT,         // 插入权限
    UPDATE,         // 更新权限
    DELETE,         // 删除权限
    CREATE_DATABASE, // 新增：创建数据库权限
    DROP_DATABASE,   // 新增：删除数据库权限
    CREATE_TABLE,   // 创建表权限
    DROP_TABLE,     // 删除表权限
    ALTER_TABLE,    // 新增：修改表权限 (虽然目前DDL没有，但API里有提及)
    CREATE_USER,    // 创建用户权限
    DROP_USER,      // 删除用户权限
    GRANT_PERMISSION, // 授予权限权限
    REVOKE_PERMISSION // 撤销权限权限
};

// 前向声明 QueryResult 类
class QueryResult;

// --- 核心内部数据结构和类型定义 ---

// 假设一行数据由字符串向量表示
using Row = std::vector<std::string>;

/**
 * @brief 表的元数据和实际数据存储结构（简化版，内存存储）
 */
struct TableData {
    std::string tableName;
    std::vector<ColumnDefinition> columns; // 列定义
    std::vector<Row> rows;                 // 实际存储的行数据

    // 辅助函数，根据列名获取列索引
    int getColumnIndex(const std::string& colName) const {
        for (size_t i = 0; i < columns.size(); ++i) {
            if (columns[i].name == colName) {
                return static_cast<int>(i);
            }
        }
        return -1; // 未找到
    }

    // 辅助函数，根据列索引获取列名
    std::string getColumnName(int index) const {
        if (index >= 0 && index < columns.size()) {
            return columns[index].name;
        }
        return ""; // 越界
    }

    // 辅助函数，根据列索引获取列类型
    DataType getColumnType(int index) const {
        if (index >= 0 && index < columns.size()) {
            return columns[index].type;
        }
        return DataType::STRING; // 默认或错误类型
    }
};

// --- 事务相关定义 ---

/**
 * @brief 事务状态枚举
 */
enum class TransactionState {
    ACTIVE,    // 事务正在进行中
    COMMITTED, // 事务已提交
    ABORTED    // 事务已中止（回滚）
};

/**
 * @brief 事务日志条目类型
 */
enum class LogEntryType {
    INSERT,
    DELETE,
    UPDATE_OLD_VALUE, // 记录更新前的旧值
    UPDATE_NEW_VALUE, // 记录更新后的新值 (用于redo)
    BEGIN_TRANSACTION,
    COMMIT_TRANSACTION,
    ROLLBACK_TRANSACTION
};

/**
 * @brief 事务日志条目结构
 * 简化版，用于在内存中记录操作，以便回滚。
 */
struct LogEntry {
    long long transactionId;
    LogEntryType type; // 直接使用 LogEntryType 枚举
    std::string tableName;
    std::vector<std::string> oldRowValues; // 记录旧值或被删除的行
    std::vector<std::string> newRowValues; // 记录新值或插入的行
    int rowIndex; // 如果是特定行操作，记录行在TableData::rows中的索引（简化）

    LogEntry() = default;

    LogEntry(long long txId, LogEntryType t, const std::string& table = "",
             const std::vector<std::string>& old_vals = {}, const std::vector<std::string>& new_vals = {}, int r_idx = -1)
        : transactionId(txId), type(t), tableName(table), oldRowValues(old_vals), newRowValues(new_vals), rowIndex(r_idx) {}
};


// --- 权限控制相关定义 ---

/**
 * @brief 用户信息结构
 */
struct User {
    std::string username;
    std::string passwordHash; // 简化为明文密码，实际应为哈希值
};

/**
 * @brief 权限条目结构
 */
struct PermissionEntry {
    std::string username;       // 拥有权限的用户名
    PermissionType permission;  // 权限类型 (SELECT, INSERT, CREATE_TABLE等)
    std::string objectType;     // 对象类型 (例如 "DATABASE", "TABLE", "SYSTEM")
    std::string objectName;     // 具体对象名称 (例如 "my_db", "Users", "")
};


// --- 异常类定义 ---

/**
 * @brief 数据库操作中所有自定义异常的基类。
 */
class DatabaseException : public std::runtime_error {
public:
    explicit DatabaseException(const std::string& message) : std::runtime_error(message) {}
};

/**
 * @brief 当执行的SQL或条件语句存在语法错误时抛出的异常。
 */
class SyntaxException : public DatabaseException {
public:
    explicit SyntaxException(const std::string& message) : DatabaseException(message) {}
};

/**
 * @brief 当用户没有足够权限执行某个操作时抛出的异常。
 */
class PermissionDeniedException : public DatabaseException {
public:
    explicit PermissionDeniedException(const std::string& message) : DatabaseException(message) {}
};

/**
 * @brief 当尝试操作不存在的表或列时抛出的异常。
 */
class TableNotFoundException : public DatabaseException {
public:
    explicit TableNotFoundException(const std::string& message) : DatabaseException(message) {}
};

/**
 * @brief 抽象基类，表示数据库查询的结果集。
 * 客户端通过此接口遍历和访问查询结果。
 */
class QueryResult {
public:
    virtual ~QueryResult() = default;
    virtual int getRowCount() const = 0;
    virtual int getColumnCount() const = 0;
    virtual std::string getColumnName(int index) const = 0;
    virtual DataType getColumnType(int index) const = 0;
    virtual bool next() = 0; // 移动到下一行
    virtual std::string getString(int columnIndex) const = 0;
    virtual int getInt(int columnIndex) const = 0;
    virtual double getDouble(int columnIndex) const = 0;
};

// --- Database::Impl 的前向声明，并假设其包含所有核心模块的内部成员和方法 ---
// 这些成员和方法的具体定义将在 Database.cpp 中。

class Database::Impl {
public:
    std::string dbPath_; // 数据库系统的根目录
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
    explicit Impl(const std::string& dbPath);
    ~Impl();

    // --- DDL 内部辅助方法 ---
    // 这些是 DDLOperations::Impl 将调用的底层文件系统操作
    bool createDatabaseInternal(const std::string& dbName);
    bool dropDatabaseInternal(const std::string& dbName);
    bool useDatabaseInternal(const std::string& dbName);
    bool createTableFileInternal(const std::string& tableName, const std::vector<ColumnDefinition>& columns);
    bool dropTableFileInternal(const std::string& tableName);
    bool alterTableAddColumnInternal(const std::string& tableName, const ColumnDefinition& column);


    // --- DML 内部辅助方法 (DMLOperations.cpp 会调用这些) ---
    TableData* getMutableTableData(const std::string& tableName);
    const TableData* getTableData(const std::string& tableName) const;

    // --- 事务管理内部辅助方法 ---
    void beginTransactionInternal();
    void commitInternal();
    void rollbackInternal();
    void logOperation(const LogEntry& entry);

    // --- 权限控制内部辅助方法 ---
    bool authenticateInternal(const std::string& username, const std::string& password);
    bool checkPermissionInternal(const std::string& username, PermissionType permission, const std::string& objectType, const std::string& objectName = "");
    bool createUserInternal(const std::string& username, const std::string& password);
    bool dropUserInternal(const std::string& username);
    bool grantPermissionInternal(const std::string& username, PermissionType permission, const std::string& objectType, const std::string& objectName = "");
    bool revokePermissionInternal(const std::string& username, PermissionType permission, const std::string& objectType, const std::string& objectName = "");
    // 获取当前登录用户
    const std::string& getCurrentUser() const; // 声明为 const
    void setCurrentUser(const std::string& username);
};

// --- 各个操作模块的类声明 ---

/**
 * @brief 负责数据定义语言 (DDL) 操作，管理数据库和表。
 */
class DDLOperations {
public:
    class Impl;
    std::unique_ptr<Impl> pImpl;

    explicit DDLOperations(class Database::Impl* db_core_impl); // 构造函数现在接收 Database::Impl*
    ~DDLOperations();

    // 完整的 DDL API 声明
    bool createDatabase(const std::string& dbName);
    bool dropDatabase(const std::string& dbName);
    bool useDatabase(const std::string& dbName);
    bool createTable(const std::string& tableName, const std::vector<ColumnDefinition>& columns);
    bool dropTable(const std::string& tableName);
    bool alterTableAddColumn(const std::string& tableName, const ColumnDefinition& column); // 添加缺少的声明
};

/**
 * @brief 负责数据操作语言 (DML) 操作，如插入、更新、删除和查询数据。
 */
class DMLOperations {
public:
    class Impl;
    std::unique_ptr<Impl> pImpl;

    explicit DMLOperations(class Database::Impl* db_core_impl);
    ~DMLOperations();

    int insert(const std::string& tableName, const std::map<std::string, std::string>& values);
    int update(const std::string& tableName, const std::map<std::string, std::string>& updates, const std::string& whereClause);
    int remove(const std::string& tableName, const std::string& whereClause);
    std::unique_ptr<QueryResult> select(const std::string& tableName, const std::string& whereClause = "", const std::string& orderBy = "");
};

/**
 * @brief 负责事务管理，包括事务的开始、提交和回滚。
 */
class TransactionManager {
public:
    class Impl;
    std::unique_ptr<Impl> pImpl;

    explicit TransactionManager(class Database::Impl* db_core_impl);
    ~TransactionManager();

    void beginTransaction();
    void commit();
    void rollback();
};

/**
 * @brief 负责用户认证和权限管理。
 */
class AccessControl {
public:
    class Impl;
    std::unique_ptr<Impl> pImpl;

    explicit AccessControl(class Database::Impl* db_core_impl); // 构造函数接收 Database::Impl*
    ~AccessControl();

    bool login(const std::string& username, const std::string& password);
    bool logout();
    bool createUser(const std::string& username, const std::string& password);
    bool dropUser(const std::string& username);
    bool grantPermission(const std::string& username, PermissionType permission, const std::string& objectType, const std::string& objectName = "");
    bool revokePermission(const std::string& username, PermissionType permission, const std::string& objectType, const std::string& objectName = "");
};

/**
 * @brief 数据库操作核心类。
 * 作为所有数据库功能的统一入口。
 */
class Database {
public:
    explicit Database(const std::string& dbPath);
    ~Database();
    DDLOperations& getDDLOperations();
    DMLOperations& getDMLOperations();
    TransactionManager& getTransactionManager();
    AccessControl& getAccessControl();

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
    std::unique_ptr<DDLOperations> ddl_ops;
    std::unique_ptr<DMLOperations> dml_ops;
    std::unique_ptr<TransactionManager> tx_manager;
    std::unique_ptr<AccessControl> ac_manager;
};

#endif // DATABASE_API_HPP