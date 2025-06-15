#include "../../include/network/query.hpp"

namespace NET {

// ========== LiteralValue Implementation ==========

void LiteralValue::serialize(Serializer& serializer) const {
    serializer.writeU8(static_cast<uint8_t>(type));
    serializer.writeString(value);
}

std::expected<LiteralValue, ProtocolError> LiteralValue::deserialize(Deserializer& deserializer) {
    auto type_result = deserializer.readU8();
    if (!type_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    
    auto value_result = deserializer.readString();
    if (!value_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    
    return LiteralValue(static_cast<DataType>(type_result.value()), std::move(value_result.value()));
}

// ========== ColumnDefinition Implementation ==========

void ColumnDefinition::serialize(Serializer& serializer) const {
    serializer.writeString(name);
    serializer.writeU8(static_cast<uint8_t>(type));
    serializer.writeU8(is_primary_key ? 1 : 0);
}

std::expected<ColumnDefinition, ProtocolError> ColumnDefinition::deserialize(Deserializer& deserializer) {
    auto name_result = deserializer.readString();
    if (!name_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    
    auto type_result = deserializer.readU8();
    if (!type_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    
    auto primary_result = deserializer.readU8();
    if (!primary_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    
    return ColumnDefinition(std::move(name_result.value()), 
                           static_cast<DataType>(type_result.value()),
                           primary_result.value() != 0);
}

// ========== WhereCondition Implementation ==========

void WhereCondition::serialize(Serializer& serializer) const {
    serializer.writeString(column);
    serializer.writeString(operator_str);
    value.serialize(serializer);
}

std::expected<WhereCondition, ProtocolError> WhereCondition::deserialize(Deserializer& deserializer) {
    auto column_result = deserializer.readString();
    if (!column_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    
    auto op_result = deserializer.readString();
    if (!op_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    
    auto value_result = LiteralValue::deserialize(deserializer);
    if (!value_result.has_value()) {
        return std::unexpected(value_result.error());
    }
    
    return WhereCondition(std::move(column_result.value()), 
                         std::move(op_result.value()),
                         std::move(value_result.value()));
}

// ========== SetClause Implementation ==========

void SetClause::serialize(Serializer& serializer) const {
    serializer.writeString(column);
    value.serialize(serializer);
}

std::expected<SetClause, ProtocolError> SetClause::deserialize(Deserializer& deserializer) {
    auto column_result = deserializer.readString();
    if (!column_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    
    auto value_result = LiteralValue::deserialize(deserializer);
    if (!value_result.has_value()) {
        return std::unexpected(value_result.error());
    }
    
    return SetClause(std::move(column_result.value()), std::move(value_result.value()));
}

// ========== QueryRequest Implementation ==========

QueryRequest::QueryRequest(OperationType op) 
    : Message(MessageType::QUERY_REQUEST), operation(op) {}

void QueryRequest::serializePayload(Serializer& serializer) const {
    // 序列化基本信息
    serializer.writeU8(static_cast<uint8_t>(operation));
    serializer.writeString(session_token);
    
    // 序列化DDL参数
    serializer.writeString(database_name);
    serializer.writeString(table_name);
    
    // 序列化列定义
    serializer.writeU32(static_cast<uint32_t>(columns.size()));
    for (const auto& col : columns) {
        col.serialize(serializer);
    }
    
    // 序列化DML参数
    serializer.writeU32(static_cast<uint32_t>(select_columns.size()));
    for (const auto& col : select_columns) {
        serializer.writeString(col);
    }
    
    serializer.writeU32(static_cast<uint32_t>(insert_values.size()));
    for (const auto& val : insert_values) {
        val.serialize(serializer);
    }
    
    serializer.writeU32(static_cast<uint32_t>(update_clauses.size()));
    for (const auto& clause : update_clauses) {
        clause.serialize(serializer);
    }
    
    // 序列化WHERE条件
    serializer.writeU8(where_condition.has_value() ? 1 : 0);
    if (where_condition.has_value()) {
        where_condition.value().serialize(serializer);
    }
}

std::expected<void, ProtocolError> QueryRequest::deserializePayload(Deserializer& deserializer) {
    // 反序列化基本信息
    auto op_result = deserializer.readU8();
    if (!op_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    operation = static_cast<OperationType>(op_result.value());
    
    auto token_result = deserializer.readString();
    if (!token_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    session_token = std::move(token_result.value());
    
    // 反序列化DDL参数
    auto db_name_result = deserializer.readString();
    if (!db_name_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    database_name = std::move(db_name_result.value());
    
    auto table_name_result = deserializer.readString();
    if (!table_name_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    table_name = std::move(table_name_result.value());
    
    // 反序列化列定义
    auto columns_count_result = deserializer.readU32();
    if (!columns_count_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    
    columns.clear();
    columns.reserve(columns_count_result.value());
    for (uint32_t i = 0; i < columns_count_result.value(); ++i) {
        auto col_result = ColumnDefinition::deserialize(deserializer);
        if (!col_result.has_value()) {
            return std::unexpected(col_result.error());
        }
        columns.push_back(std::move(col_result.value()));
    }
    
    // 反序列化DML参数
    auto select_count_result = deserializer.readU32();
    if (!select_count_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    
    select_columns.clear();
    select_columns.reserve(select_count_result.value());
    for (uint32_t i = 0; i < select_count_result.value(); ++i) {
        auto col_result = deserializer.readString();
        if (!col_result.has_value()) {
            return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
        }
        select_columns.push_back(std::move(col_result.value()));
    }
    
    auto insert_count_result = deserializer.readU32();
    if (!insert_count_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    
    insert_values.clear();
    insert_values.reserve(insert_count_result.value());
    for (uint32_t i = 0; i < insert_count_result.value(); ++i) {
        auto val_result = LiteralValue::deserialize(deserializer);
        if (!val_result.has_value()) {
            return std::unexpected(val_result.error());
        }
        insert_values.push_back(std::move(val_result.value()));
    }
    
    auto update_count_result = deserializer.readU32();
    if (!update_count_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    
    update_clauses.clear();
    update_clauses.reserve(update_count_result.value());
    for (uint32_t i = 0; i < update_count_result.value(); ++i) {
        auto clause_result = SetClause::deserialize(deserializer);
        if (!clause_result.has_value()) {
            return std::unexpected(clause_result.error());
        }
        update_clauses.push_back(std::move(clause_result.value()));
    }
    
    // 反序列化WHERE条件
    auto has_where_result = deserializer.readU8();
    if (!has_where_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    
    if (has_where_result.value() != 0) {
        auto where_result = WhereCondition::deserialize(deserializer);
        if (!where_result.has_value()) {
            return std::unexpected(where_result.error());
        }
        where_condition = std::move(where_result.value());
    } else {
        where_condition.reset();
    }
    
    return {};
}

QueryRequest QueryBuilder::buildCreateDatabase(const CreateDatabaseCommand& cmd) {
    QueryRequest request(OperationType::CREATE_DATABASE);
    request.setDatabaseName(cmd.db_name);
    return request;
}

QueryRequest QueryBuilder::buildDropDatabase(const DropDatabaseCommand& cmd) {
    QueryRequest request(OperationType::DROP_DATABASE);
    request.setDatabaseName(cmd.db_name);
    return request;
}

QueryRequest QueryBuilder::buildUseDatabase(const UseDatabaseCommand& cmd) {
    QueryRequest request(OperationType::USE_DATABASE);
    request.setDatabaseName(cmd.db_name);
    return request;
}

QueryRequest QueryBuilder::buildCreateTable(const CreateTableCommand& cmd) {
    QueryRequest request(OperationType::CREATE_TABLE);
    request.setTableName(cmd.table_name);
    
    std::vector<ColumnDefinition> columns;
    for (const auto& col : cmd.columns) {
        columns.push_back(convertColumnDef(col));
    }
    request.setColumns(columns);
    
    return request;
}

QueryRequest QueryBuilder::buildDropTable(const DropTableCommand& cmd) {
    QueryRequest request(OperationType::DROP_TABLE);
    request.setTableName(cmd.table_name);
    return request;
}

QueryRequest QueryBuilder::buildInsert(const InsertCommand& cmd) {
    QueryRequest request(OperationType::INSERT);
    request.setTableName(cmd.table_name);
    
    std::vector<LiteralValue> values;
    for (const auto& val : cmd.values) {
        values.push_back(convertLiteralValue(val));
    }
    request.setInsertValues(values);
    
    return request;
}

QueryRequest QueryBuilder::buildSelect(const SelectCommand& cmd) {
    QueryRequest request(OperationType::SELECT);
    request.setTableName(cmd.table_name);
    
    if (!cmd.select_all) {
        request.setSelectColumns(cmd.columns);
    }
    // 如果select_all为true，则select_columns保持为空，表示SELECT *
    
    if (cmd.where_clause.has_value()) {
        request.setWhereCondition(convertWhereClause(cmd.where_clause.value()));
    }
    
    return request;
}

QueryRequest QueryBuilder::buildUpdate(const UpdateCommand& cmd) {
    QueryRequest request(OperationType::UPDATE);
    request.setTableName(cmd.table_name);
    
    std::vector<SetClause> clauses;
    for (const auto& set : cmd.set_clauses) {
        clauses.push_back(convertSetClause(set));
    }
    request.setUpdateClauses(clauses);
    
    if (cmd.where_clause.has_value()) {
        request.setWhereCondition(convertWhereClause(cmd.where_clause.value()));
    }
    
    return request;
}

QueryRequest QueryBuilder::buildDelete(const DeleteCommand& cmd) {
    QueryRequest request(OperationType::DELETE);
    request.setTableName(cmd.table_name);
    
    if (cmd.where_clause.has_value()) {
        request.setWhereCondition(convertWhereClause(cmd.where_clause.value()));
    }
    
    return request;
}

// 类型转换辅助函数
DataType QueryBuilder::convertTokenType(TokenType token_type) {
    switch (token_type) {
        case TokenType::KEYWORD_INT:
            return DataType::INT;
        case TokenType::KEYWORD_STRING:
            return DataType::STRING;
        case TokenType::NUMERIC_LITERAL:
            return DataType::DOUBLE;
        case TokenType::STRING_LITERAL:
            return DataType::STRING;
        default:
            return DataType::STRING;
    }
}

LiteralValue QueryBuilder::convertLiteralValue(const ::LiteralValue& client_value) {
    DataType type = convertTokenType(client_value.type);
    return LiteralValue(type, client_value.value);
}

ColumnDefinition QueryBuilder::convertColumnDef(const ColumnDef& client_col) {
    DataType type = convertTokenType(client_col.type);
    return ColumnDefinition(client_col.name, type, client_col.is_primary);
}

WhereCondition QueryBuilder::convertWhereClause(const WhereClause& client_where) {
    LiteralValue value = convertLiteralValue(client_where.value);
    return WhereCondition(client_where.column, client_where.op, std::move(value));
}

SetClause QueryBuilder::convertSetClause(const ::SetClause& client_set) {
    LiteralValue value = convertLiteralValue(client_set.value);
    return SetClause(client_set.column, std::move(value));
}

NetworkQueryExecutor::NetworkQueryExecutor(SocketClient& client_ref)
        : client(client_ref) {
    // 构造时session_token为空字符串，表示未认证状态
}

void NetworkQueryExecutor::setSessionToken(const std::string& token) {
    session_token = token;
}

bool NetworkQueryExecutor::isAuthenticated() const {
    // 只有当session_token不为空时才认为已认证
    return !session_token.empty();
}

void NetworkQueryExecutor::clearAuthentication() {
    // 清空session_token，将用户置为未认证状态
    session_token.clear();
}

std::expected<std::unique_ptr<QueryResponse>, SocketError> 
NetworkQueryExecutor::executeQuery(const QueryRequest& request) {
    if (session_token.empty()) {
        return std::unexpected(SocketError::SEND_FAILED);
    }
    
    // 创建请求的副本并设置token
    QueryRequest query_request = request;
    query_request.setSessionToken(session_token);
    
    std::cout << "[DEBUG] Executing  query..." << std::endl;
    
    // 发送请求
    auto send_result = client.sendMessage(query_request);
    if (!send_result.has_value()) {
        std::cerr << "[ERROR] Failed to send  query request" << std::endl;
        return std::unexpected(send_result.error());
    }
    
    // 接收响应
    auto response_result = client.receiveMessage();
    if (!response_result.has_value()) {
        std::cerr << "[ERROR] Failed to receive query response" << std::endl;
        return std::unexpected(SocketError::RECV_FAILED);
    }
    
    auto message = std::move(response_result.value());
    
    // 检查响应类型
    if (message->getType() == MessageType::QUERY_RESPONSE) {
        auto* query_response = dynamic_cast<QueryResponse*>(message.release());
        return std::unique_ptr<QueryResponse>(query_response);
    } else if (message->getType() == MessageType::ERROR_RESPONSE) {
        auto* error_response = dynamic_cast<ErrorResponse*>(message.get());
        std::cerr << "[ERROR] Server error: " << error_response->getErrorMessage() << std::endl;
        return std::unexpected(SocketError::RECV_FAILED);
    }
    
    std::cerr << "[ERROR] Unexpected response type" << std::endl;
    return std::unexpected(SocketError::RECV_FAILED);
}

} // namespace NET