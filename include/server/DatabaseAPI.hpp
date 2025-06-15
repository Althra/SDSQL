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

// ========================================================================
// --- 已修改部分 开始 ---
// ========================================================================

/**
 * @brief 定义表的列结构
 */
struct ColumnDefinition {
    std::string name;   // 列名
    DataType type;      // 列的数据类型
    bool isPrimaryKey = false; // ← 新增：标记该列是否为主键

    // 默认构造函数
    ColumnDefinition() = default;

    // 构造函数
    ColumnDefinition(const std::string& name, DataType type, bool is_primary = false)
        : name(name), type(type), isPrimaryKey(is_primary) {}
};

// ========================================================================
// --- 已修改部分 结束 ---
// ========================================================================

/**
 * @brief 权限类型枚举
 */
enum class PermissionType {
    SELECT,         // 查询权限
    INSERT,         // 插入权限
    UPDATE,         // 更新权限
    DELETE,         // 删除权限
    CREATE_TABLE,   // 创建表权限
    DROP_TABLE      // 删除表权限
};

// 前向声明 QueryResult 类
class QueryResult;

// -----------------------------------------------------------------------------
// 自定义异常类（建议实现，用于错误处理）
// -----------------------------------------------------------------------------

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
    /**
     * @brief 虚析构函数，确保派生类正确析构。
     */
    virtual ~QueryResult() = default;

    /**
     * @brief 获取结果集中的行数。
     * @return 行数。
     */
    virtual int getRowCount() const = 0;

    /**
     * @brief 获取结果集中的列数。
     * @return 列数。
     */
    virtual int getColumnCount() const = 0;

    /**
     * @brief 根据列索引获取列名。
     * @param index 列的索引（从0开始）。
     * @return 列名。
     */
    virtual std::string getColumnName(int index) const = 0;

    /**
     * @brief 根据列索引获取列的数据类型。
     * @param index 列的索引（从0开始）。
     * @return 列的数据类型。
     */
    virtual DataType getColumnType(int index) const = 0;

    /**
     * @brief 移动到结果集的下一行。
     * @return 如果成功移动到下一行（即还有更多行）则返回 true，否则返回 false。
     */
    virtual bool next() = 0;

    /**
     * @brief 获取当前行指定列的字符串值。
     * @param columnIndex 列的索引。
     * @return 列的字符串值。
     */
    virtual std::string getString(int columnIndex) const = 0;

    /**
     * @brief 获取当前行指定列的整型值。
     * @param columnIndex 列的索引。
     * @return 列的整型值。
     */
    virtual int getInt(int columnIndex) const = 0;

    /**
     * @brief 获取当前行指定列的浮点型值。
     * @param columnIndex 列的索引。
     * @return 列的浮点型值。
     */
    virtual double getDouble(int columnIndex) const = 0;

    // 可以添加更多获取不同类型数据的方法，例如 getBool, getBytes 等
};

/**
 * @brief 负责数据定义语言 (DDL) 操作，管理数据库和表。
 */
class DDLOperations {
public:
    // Pimpl Idiom: 隐藏内部实现细节
    class Impl;
    //std::unique_ptr<Impl> pImpl;
    Impl* pImpl;
    // 构造函数，需要一个指向数据库核心实现的指针
    DDLOperations(Impl* db_core_impl);
    ~DDLOperations();

    /**
     * @brief 创建一个新的数据库。
     * @param dbName 要创建的数据库的名称。
     * @return 成功创建返回 true，否则返回 false。
     */
    bool createDatabase(const std::string& dbName);

    /**
     * @brief 删除一个已存在的数据库。
     * @param dbName 要删除的数据库的名称。
     * @return 成功删除返回 true，否则返回 false。
     */
    bool dropDatabase(const std::string& dbName);

    /**
     * @brief 切换到指定的数据库上下文。
     * @param dbName 要切换到的数据库的名称。
     * @return 成功切换返回 true，否则返回 false。
     */
    bool useDatabase(const std::string& dbName);

    /**
     * @brief 在当前数据库中创建一个新表。
     * @note 如果列被指定为 primary，将为该列创建索引。
     * @param tableName 要创建的表的名称。
     * @param columns 包含列定义（名称、类型、是否主键）的向量。
     * @return 如果成功创建表则返回 true，否则返回 false。
     */
    bool createTable(const std::string& tableName, const std::vector<ColumnDefinition>& columns);

    /**
     * @brief 从当前数据库中删除一个表。
     * @note 删除表的同时，也会删除其对应的索引文件（如果存在）。
     * @param tableName 要删除的表的名称。
     * @return 如果成功删除表则返回 true，否则返回 false。
     */
    bool dropTable(const std::string& tableName);
};

/**
 * @brief 负责数据操作语言 (DML) 操作，如插入、更新、删除和查询数据。
 */
class DMLOperations {
public:
    // Pimpl Idiom: 隐藏内部实现细节
    class Impl;
    //std::unique_ptr<Impl> pImpl;
    Impl* pImpl;
    // 构造函数，需要一个指向数据库核心实现的指针
    DMLOperations(Impl* db_core_impl);
    ~DMLOperations();

    /**
     * @brief 向表中插入一条记录。
     * @param tableName 要插入记录的表的名称。
     * @param values 键值对，表示列名及其对应的值。
     * @return 成功插入的行数（通常为1），如果失败则返回0。
     */
    int insert(const std::string& tableName, const std::map<std::string, std::string>& values);

    /**
     * @brief 更新表中符合条件的记录。
     * @param tableName 要更新记录的表的名称。
     * @param updates 键值对，表示要更新的列名及其新值。
     * @param whereClause 用于筛选记录的条件字符串（例如："age > 30 AND city = 'New York'"）。
     * @return 受影响的行数。
     */
    int update(const std::string& tableName, const std::map<std::string, std::string>& updates, const std::string& whereClause);

    /**
     * @brief 删除表中符合条件的记录。
     * @param tableName 要删除记录的表的名称。
     * @param whereClause 用于筛选记录的条件字符串。
     * @return 被删除的行数。
     */
    int remove(const std::string& tableName, const std::string& whereClause);

    /**
     * @brief 从表中查询记录。
     * @param tableName 要查询的表的名称。
     * @param whereClause 用于筛选记录的条件字符串（可选）。
     * @param orderBy 用于结果排序的列名（可选）。
     * @return 指向 QueryResult 对象的 unique_ptr，包含查询结果集。
     * 客户端应通过 QueryResult 迭代结果。
     */
    std::unique_ptr<QueryResult> select(const std::string& tableName, const std::string& whereClause = "", const std::string& orderBy = "");
};

/**
 * @brief 负责事务管理，包括事务的开始、提交和回滚。
 */
class TransactionManager {
public:
    // Pimpl Idiom: 隐藏内部实现细节
    class Impl;
    //std::unique_ptr<Impl> pImpl;
    Impl* pImpl;
    // 构造函数，需要一个指向数据库核心实现的指针
    TransactionManager(Impl* db_core_impl);
    ~TransactionManager();

    /**
     * @brief 开始一个新事务。
     */
    void beginTransaction();

    /**
     * @brief 提交当前事务，使所有更改永久化。
     */
    void commit();

    /**
     * @brief 回滚当前事务，撤销所有未提交的更改。
     */
    void rollback();
};

/**
 * @brief 负责用户认证和权限管理。
 */
class AccessControl {
public:
    // Pimpl Idiom: 隐藏内部实现细节
    class Impl;
    //std::unique_ptr<Impl> pImpl;
    Impl* pImpl;
    // 构造函数，需要一个指向数据库核心实现的指针
    AccessControl(Impl* db_core_impl);
    ~AccessControl();

    /**
     * @brief 用户登录。
     * @param username 用户名。
     * @param password 密码。
     * @return 如果认证成功则返回 true，否则返回 false。
     */
    bool login(const std::string& username, const std::string& password);

    /**
     * @brief 用户登出。
     * @return 始终返回 true。
     */
    bool logout();

    /**
     * @brief 创建一个新用户。
     * @param username 新用户的用户名。
     * @param password 新用户的密码。
     * @return 如果成功创建用户则返回 true，否则返回 false。
     */
    bool createUser(const std::string& username, const std::string& password);

    /**
     * @brief 删除一个用户。
     * @param username 要删除的用户的用户名。
     * @return 如果成功删除用户则返回 true，否则返回 false。
     */
    bool dropUser(const std::string& username);

    /**
     * @brief 授予用户特定权限。
     * @param username 被授予权限的用户名。
     * @param permission 要授予的权限类型。
     * @param objectType 权限作用的对象类型（例如："TABLE", "DATABASE"）。
     * @param objectName 权限作用的具体对象名称（例如表名），可选。
     * @return 如果成功授予权限则返回 true，否则返回 false。
     */
    bool grantPermission(const std::string& username, PermissionType permission, const std::string& objectType, const std::string& objectName = "");

    /**
     * @brief 撤销用户的特定权限。
     * @param username 被撤销权限的用户名。
     * @param permission 要撤销的权限类型。
     * @param objectType 权限作用的对象类型。
     * @param objectName 权限作用的具体对象名称，可选。
     * @return 如果成功撤销权限则返回 true，否则返回 false。
     */
    bool revokePermission(const std::string& username, PermissionType permission, const std::string& objectType, const std::string& objectName = "");
};

/**
 * @brief 数据库操作核心类。
 * 作为所有数据库功能的统一入口。
 */
class Database {
public:
    /**
     * @brief 构造函数，初始化数据库连接或打开数据库文件。
     * @param dbPath 数据库文件或存储目录的路径。
     */
    explicit Database(const std::string& dbPath);

    /**
     * @brief 析构函数，负责关闭数据库连接和清理资源。
     */
    ~Database();

    /**
     * @brief 获取 DDL 操作接口。
     * @return DDLOperations 对象的引用。
     */
    DDLOperations& getDDLOperations();

    /**
     * @brief 获取 DML 操作接口。
     * @return DMLOperations 对象的引用。
     */
    DMLOperations& getDMLOperations();

    /**
     * @brief 获取事务管理接口。
     * @return TransactionManager 对象的引用。
     */
    TransactionManager& getTransactionManager();

    /**
     * @brief 获取权限控制接口。
     * @return AccessControl 对象的引用。
     */
    AccessControl& getAccessControl();

private:
    // Pimpl Idiom for the main Database class:
    // This Impl class will hold the core database state and internal
    // implementations of DDLOperations, DMLOperations, etc.
    class Impl;
    std::unique_ptr<Impl> pImpl;

    // References to the public facing operation classes
    std::unique_ptr<DDLOperations> ddl_ops;
    //std::unique_ptr<DMLOperations> dml_ops;
    std::unique_ptr<TransactionManager> tx_manager;
    //std::unique_ptr<AccessControl> ac_manager;
};


#endif // DATABASE_API_HPP