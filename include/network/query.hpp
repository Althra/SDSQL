#pragma once

#include <string>
#include <vector>
#include <optional>
#include <expected>
#include "serializer.hpp"
#include "protocol.hpp"
#include "../client/Parser.hpp"
#include "socket_client.hpp"

namespace NET {

// 操作类型枚举
enum class OperationType : uint8_t {
    // DDL操作
    CREATE_DATABASE = 0x01,
    DROP_DATABASE = 0x02,
    USE_DATABASE = 0x03,
    CREATE_TABLE = 0x04,
    DROP_TABLE = 0x05,
    
    // DML操作
    INSERT = 0x10,
    SELECT = 0x11,
    UPDATE = 0x12,
    DELETE = 0x13
};

// 数据类型枚举（与服务端保持一致）
enum class DataType : uint8_t {
    INT = 0x01,
    DOUBLE = 0x02,
    STRING = 0x03,
    BOOL = 0x04
};

// 字面量值
struct LiteralValue {
    DataType type;
    std::string value;
    
    LiteralValue() = default;
    LiteralValue(DataType t, std::string v) : type(t), value(std::move(v)) {}
    
    void serialize(Serializer& serializer) const;
    static std::expected<LiteralValue, ProtocolError> deserialize(Deserializer& deserializer);
};

// 列定义
struct ColumnDefinition {
    std::string name;
    DataType type;
    bool is_primary_key = false;
    
    ColumnDefinition() = default;
    ColumnDefinition(std::string n, DataType t, bool primary = false) 
        : name(std::move(n)), type(t), is_primary_key(primary) {}
    
    void serialize(Serializer& serializer) const;
    static std::expected<ColumnDefinition, ProtocolError> deserialize(Deserializer& deserializer);
};

// WHERE条件
struct WhereCondition {
    std::string column;
    std::string operator_str; // "=", ">", "<", ">=", "<=", "!="
    LiteralValue value;
    
    WhereCondition() = default;
    WhereCondition(std::string col, std::string op, LiteralValue val)
        : column(std::move(col)), operator_str(std::move(op)), value(std::move(val)) {}
    
    void serialize(Serializer& serializer) const;
    static std::expected<WhereCondition, ProtocolError> deserialize(Deserializer& deserializer);
};

// SET子句（用于UPDATE）
struct SetClause {
    std::string column;
    LiteralValue value;
    
    SetClause() = default;
    SetClause(std::string col, LiteralValue val) : column(std::move(col)), value(std::move(val)) {}
    
    void serialize(Serializer& serializer) const;
    static std::expected<SetClause, ProtocolError> deserialize(Deserializer& deserializer);
};

// 结构化查询请求
class QueryRequest : public Message {
private:
    OperationType operation;
    std::string session_token;
    
    // DDL参数
    std::string database_name;      // 用于数据库操作
    std::string table_name;         // 用于表操作
    std::vector<ColumnDefinition> columns; // 用于CREATE TABLE
    
    // DML参数
    std::vector<std::string> select_columns;  // 用于SELECT，空表示SELECT *
    std::vector<LiteralValue> insert_values;  // 用于INSERT
    std::vector<SetClause> update_clauses;    // 用于UPDATE
    std::optional<WhereCondition> where_condition; // 用于SELECT/UPDATE/DELETE

public:
    explicit QueryRequest(OperationType op = OperationType::SELECT);
    
    // 设置基本信息
    void setSessionToken(const std::string& token) { session_token = token; }
    const std::string& getSessionToken() const { return session_token; }
    
    OperationType getOperation() const { return operation; }
    void setOperation(OperationType op) { operation = op; }
    
    // DDL操作设置
    void setDatabaseName(const std::string& name) { database_name = name; }
    const std::string& getDatabaseName() const { return database_name; }
    
    void setTableName(const std::string& name) { table_name = name; }
    const std::string& getTableName() const { return table_name; }
    
    void setColumns(const std::vector<ColumnDefinition>& cols) { columns = cols; }
    const std::vector<ColumnDefinition>& getColumns() const { return columns; }
    
    // DML操作设置
    void setSelectColumns(const std::vector<std::string>& cols) { select_columns = cols; }
    const std::vector<std::string>& getSelectColumns() const { return select_columns; }
    
    void setInsertValues(const std::vector<LiteralValue>& values) { insert_values = values; }
    const std::vector<LiteralValue>& getInsertValues() const { return insert_values; }
    
    void setUpdateClauses(const std::vector<SetClause>& clauses) { update_clauses = clauses; }
    const std::vector<SetClause>& getUpdateClauses() const { return update_clauses; }
    
    void setWhereCondition(const WhereCondition& condition) { where_condition = condition; }
    const std::optional<WhereCondition>& getWhereCondition() const { return where_condition; }
    void clearWhereCondition() { where_condition.reset(); }
    
    // 实现基类方法
    void serializePayload(Serializer& serializer) const override;
    std::expected<void, ProtocolError> deserializePayload(Deserializer& deserializer) override;
};

// 将客户端解析的命令转换为结构化查询请求
class QueryBuilder {
public:
    // DDL命令转换
    static QueryRequest buildCreateDatabase(const CreateDatabaseCommand& cmd);
    static QueryRequest buildDropDatabase(const DropDatabaseCommand& cmd);
    static QueryRequest buildUseDatabase(const UseDatabaseCommand& cmd);
    static QueryRequest buildCreateTable(const CreateTableCommand& cmd);
    static QueryRequest buildDropTable(const DropTableCommand& cmd);
    
    // DML命令转换
    static QueryRequest buildInsert(const InsertCommand& cmd);
    static QueryRequest buildSelect(const SelectCommand& cmd);
    static QueryRequest buildUpdate(const UpdateCommand& cmd);
    static QueryRequest buildDelete(const DeleteCommand& cmd);

private:
    // 类型转换辅助函数
    static DataType convertTokenType(TokenType token_type);
    static LiteralValue convertLiteralValue(const ::LiteralValue& client_value);
    static ColumnDefinition convertColumnDef(const ColumnDef& client_col);
    static WhereCondition convertWhereClause(const WhereClause& client_where);
    static SetClause convertSetClause(const ::SetClause& client_set);
};

class NetworkQueryExecutor {
private:
    SocketClient& client;
    std::string session_token;

public:
    explicit NetworkQueryExecutor(SocketClient& client_ref);
    
    void setSessionToken(const std::string& token);
    bool isAuthenticated() const;
    void clearAuthentication();
    
    std::expected<std::unique_ptr<QueryResponse>, SocketError> 
    executeQuery(const QueryRequest& request);
};

} // namespace NET