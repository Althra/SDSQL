#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <map>
#include <vector>

// 包含数据库API和网络层头文件
#include "../include/server/DatabaseAPI.hpp"
#include "../include/network/socket_server.hpp"
#include "../include/network/protocol.hpp"
#include "../include/network/query.hpp"

#define USERNAME "admin"
#define PASSWORD "123456"

void handleLogin(NET::SocketServer& server, int client_fd, const NET::LoginRequest& request);
void handleQuery(NET::SocketServer& server, int client_fd, const NET::QueryRequest& request);

// 全局变量
std::string current_token = "";
bool is_logged_in = false;
std::unique_ptr<Database> database_instance = nullptr;

// 初始化测试数据
void initializeTestData() {
    try {
        DDLOperations& ddl_ops = database_instance->getDDLOperations();
        DMLOperations& dml_ops = database_instance->getDMLOperations();
        
        // 创建测试数据库
        std::cout << "[INIT] Creating test database..." << std::endl;
        ddl_ops.createDatabase("test_db");
        ddl_ops.useDatabase("test_db");
        
        // 创建测试表
        std::vector<ColumnDefinition> columns = {
            {"id", DataType::INT, true},
            {"name", DataType::STRING},
            {"age", DataType::INT}
        };
        ddl_ops.createTable("users", columns);
        
        // 插入测试数据
        std::map<std::string, std::string> user1 = {
            {"id", "1"}, {"name", "Alice"}, {"age", "25"}
        };
        std::map<std::string, std::string> user2 = {
            {"id", "2"}, {"name", "Bob"}, {"age", "30"}
        };
        
        dml_ops.insert("users", user1);
        dml_ops.insert("users", user2);
        
        std::cout << "[INIT] Test data initialized successfully" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "[INIT] Failed to initialize test data: " << e.what() << std::endl;
    }
}

int main() {
    std::cout << "=== Database Server with Network Layer ===" << std::endl;
    std::cout << "Username: " << USERNAME << std::endl;
    std::cout << "Password: " << PASSWORD << std::endl;
    
    // 初始化数据库
    const std::string dbRoot = "./server_db_root";
    
    // 清理旧数据（可选）
    if (std::filesystem::exists(dbRoot)) {
        std::cout << "[INIT] Cleaning old database directory..." << std::endl;
        std::filesystem::remove_all(dbRoot);
    }
    
    try {
        // 创建数据库实例
        database_instance = std::make_unique<Database>(dbRoot);
        std::cout << "[INIT] Database initialized at: " << dbRoot << std::endl;
        
        // 初始化测试数据
        initializeTestData();
        
        // 启动网络服务器
        NET::SocketServer server;
        auto start_result = server.start("127.0.0.1", 4399);
        if (!start_result.has_value()) {
            std::cerr << "[ERROR] Failed to start server!" << std::endl;
            return 1;
        }
        
        std::cout << "[INFO] Server started successfully on 127.0.0.1:8080" << std::endl;
        std::cout << "[INFO] Waiting for client connections..." << std::endl;
        
        // 主服务循环
        while (true) {
            auto client_result = server.acceptClient();
            if (!client_result.has_value()) {
                continue;
            }
            
            int client_fd = client_result.value();
            std::cout << "\n[CONNECTION] Client connected: " << client_fd << std::endl;
            
            // 处理客户端消息
            while (true) {
                auto msg_result = server.receiveMessage(client_fd);
                if (!msg_result.has_value()) {
                    std::cout << "[CONNECTION] Client disconnected" << std::endl;
                    break;
                }
                
                auto message = std::move(msg_result.value());
                
                switch (message->getType()) {
                    case NET::MessageType::LOGIN_REQUEST: {
                        auto* login_req = dynamic_cast<NET::LoginRequest*>(message.get());
                        handleLogin(server, client_fd, *login_req);
                        break;
                    }
                    
                    case NET::MessageType::QUERY_REQUEST: {
                        auto* query_req = dynamic_cast<NET::QueryRequest*>(message.get());
                        handleQuery(server, client_fd, *query_req);
                        break;
                    }
                    
                    default:
                        std::cout << "[ERROR] Unsupported message type" << std::endl;
                        NET::ErrorResponse response("Unsupported message type", 400);
                        server.sendMessage(client_fd, response);
                        break;
                }
            }
            
            server.disconnectClient(client_fd);
        }
        
    } catch (const DatabaseException& e) {
        std::cerr << "[ERROR] Database exception: " << e.what() << std::endl;
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "[ERROR] General exception: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}

// ========== Tool Functions ==========

// 简单的token生成
std::string generateSimpleToken() {
    static int counter = 1000;
    return "token_" + std::to_string(++counter);
}

// 验证token
bool validateToken(const std::string& token) {
    return is_logged_in && token == current_token;
}

// 数据类型转换函数
DataType convertNetworkDataType(NET::DataType net_type) {
    switch (net_type) {
        case NET::DataType::INT: return DataType::INT;
        case NET::DataType::DOUBLE: return DataType::DOUBLE;
        case NET::DataType::STRING: return DataType::STRING;
        case NET::DataType::BOOL: return DataType::BOOL;
        default: return DataType::STRING;
    }
}

NET::DataType convertToNetworkDataType(DataType db_type) {
    switch (db_type) {
        case DataType::INT: return NET::DataType::INT;
        case DataType::DOUBLE: return NET::DataType::DOUBLE;
        case DataType::STRING: return NET::DataType::STRING;
        case DataType::BOOL: return NET::DataType::BOOL;
        default: return NET::DataType::STRING;
    }
}

// 执行查询的核心函数
NET::QueryResponse executeQuery(const NET::QueryRequest& request) {
    if (!database_instance) {
        return NET::QueryResponse("Database not initialized");
    }
    
    try {
        DDLOperations& ddl_ops = database_instance->getDDLOperations();
        DMLOperations& dml_ops = database_instance->getDMLOperations();
        
        switch (request.getOperation()) {
            case NET::OperationType::CREATE_DATABASE: {
                bool success = ddl_ops.createDatabase(request.getDatabaseName());
                if (success) {
                    std::cout << "[DDL] Created database: " << request.getDatabaseName() << std::endl;
                    return NET::QueryResponse({}, {}); // 成功，无返回数据
                } else {
                    return NET::QueryResponse("Failed to create database: " + request.getDatabaseName());
                }
            }
            
            case NET::OperationType::DROP_DATABASE: {
                bool success = ddl_ops.dropDatabase(request.getDatabaseName());
                if (success) {
                    std::cout << "[DDL] Dropped database: " << request.getDatabaseName() << std::endl;
                    return NET::QueryResponse({}, {});
                } else {
                    return NET::QueryResponse("Failed to drop database: " + request.getDatabaseName());
                }
            }
            
            case NET::OperationType::USE_DATABASE: {
                bool success = ddl_ops.useDatabase(request.getDatabaseName());
                if (success) {
                    std::cout << "[DDL] Using database: " << request.getDatabaseName() << std::endl;
                    return NET::QueryResponse({}, {});
                } else {
                    return NET::QueryResponse("Database not found: " + request.getDatabaseName());
                }
            }
            
            case NET::OperationType::CREATE_TABLE: {
                std::vector<ColumnDefinition> columns;
                for (const auto& net_col : request.getColumns()) {
                    columns.emplace_back(net_col.name, 
                                       convertNetworkDataType(net_col.type), 
                                       net_col.is_primary_key);
                }
                
                bool success = ddl_ops.createTable(request.getTableName(), columns);
                if (success) {
                    std::cout << "[DDL] Created table: " << request.getTableName() << std::endl;
                    return NET::QueryResponse({}, {});
                } else {
                    return NET::QueryResponse("Failed to create table: " + request.getTableName());
                }
            }
            
            case NET::OperationType::DROP_TABLE: {
                bool success = ddl_ops.dropTable(request.getTableName());
                if (success) {
                    std::cout << "[DDL] Dropped table: " << request.getTableName() << std::endl;
                    return NET::QueryResponse({}, {});
                } else {
                    return NET::QueryResponse("Failed to drop table: " + request.getTableName());
                }
            }
            
            case NET::OperationType::INSERT: {
                std::map<std::string, std::string> values;
                const auto& insert_values = request.getInsertValues();

                for (size_t i = 0; i < insert_values.size(); ++i) {
                    values["col_" + std::to_string(i)] = insert_values[i].value;
                }
                
                int affected_rows = dml_ops.insert(request.getTableName(), values);
                if (affected_rows > 0) {
                    std::cout << "[DML] Inserted " << affected_rows << " row(s) into " << request.getTableName() << std::endl;
                    
                    // 返回受影响行数
                    std::vector<std::string> columns = {"affected_rows"};
                    std::vector<NET::QueryResponse::Row> rows;
                    NET::QueryResponse::Row row;
                    row.columns = {std::to_string(affected_rows)};
                    rows.push_back(row);
                    
                    return NET::QueryResponse(columns, rows);
                } else {
                    return NET::QueryResponse("Failed to insert into table: " + request.getTableName());
                }
            }
            
            case NET::OperationType::SELECT: {
                std::string where_clause = "";
                if (request.getWhereCondition().has_value()) {
                    const auto& where = request.getWhereCondition().value();
                    where_clause = where.column + " " + where.operator_str + " '" + where.value.value + "'";
                }
                
                auto result = dml_ops.select(request.getTableName(), where_clause);
                if (result && result->getRowCount() > 0) {
                    std::cout << "[DML] Selected " << result->getRowCount() << " row(s) from " << request.getTableName() << std::endl;
                    
                    // 构建响应
                    std::vector<std::string> columns;
                    for (int i = 0; i < result->getColumnCount(); ++i) {
                        columns.push_back(result->getColumnName(i));
                    }
                    
                    std::vector<NET::QueryResponse::Row> rows;
                    while (result->next()) {
                        NET::QueryResponse::Row row;
                        for (int i = 0; i < result->getColumnCount(); ++i) {
                            row.columns.push_back(result->getString(i));
                        }
                        rows.push_back(row);
                    }
                    
                    return NET::QueryResponse(columns, rows);
                } else {
                    return NET::QueryResponse({}, {});
                }
            }
            
            case NET::OperationType::UPDATE: {
                std::map<std::string, std::string> updates;
                for (const auto& set_clause : request.getUpdateClauses()) {
                    updates[set_clause.column] = set_clause.value.value;
                }
                
                std::string where_clause = "";
                if (request.getWhereCondition().has_value()) {
                    const auto& where = request.getWhereCondition().value();
                    where_clause = where.column + " " + where.operator_str + " '" + where.value.value + "'";
                }
                
                int affected_rows = dml_ops.update(request.getTableName(), updates, where_clause);
                std::cout << "[DML] Updated " << affected_rows << " row(s) in " << request.getTableName() << std::endl;
                
                // 返回受影响行数
                std::vector<std::string> columns = {"affected_rows"};
                std::vector<NET::QueryResponse::Row> rows;
                NET::QueryResponse::Row row;
                row.columns = {std::to_string(affected_rows)};
                rows.push_back(row);
                
                return NET::QueryResponse(columns, rows);
            }
            
            case NET::OperationType::DELETE: {
                std::string where_clause = "";
                if (request.getWhereCondition().has_value()) {
                    const auto& where = request.getWhereCondition().value();
                    where_clause = where.column + " " + where.operator_str + " '" + where.value.value + "'";
                }
                
                int affected_rows = dml_ops.remove(request.getTableName(), where_clause);
                std::cout << "[DML] Deleted " << affected_rows << " row(s) from " << request.getTableName() << std::endl;
                
                // 返回受影响行数
                std::vector<std::string> columns = {"affected_rows"};
                std::vector<NET::QueryResponse::Row> rows;
                NET::QueryResponse::Row row;
                row.columns = {std::to_string(affected_rows)};
                rows.push_back(row);
                
                return NET::QueryResponse(columns, rows);
            }
            
            default:
                return NET::QueryResponse("Unsupported operation type");
        }
        
    } catch (const DatabaseException& e) {
        std::cerr << "[ERROR] Database exception: " << e.what() << std::endl;
        return NET::QueryResponse("Database error: " + std::string(e.what()));
    } catch (const std::exception& e) {
        std::cerr << "[ERROR] General exception: " << e.what() << std::endl;
        return NET::QueryResponse("Internal error: " + std::string(e.what()));
    }
}

// 处理登录请求
void handleLogin(NET::SocketServer& server, int client_fd, const NET::LoginRequest& request) {
    std::cout << "[LOGIN] User: " << request.getUsername() << std::endl;
    
    if (request.getUsername() == USERNAME && request.getPassword() == PASSWORD) {
        current_token = generateSimpleToken();
        is_logged_in = true;
        
        std::cout << "[LOGIN] Success, Token: " << current_token << std::endl;
        NET::LoginSuccess response(current_token, 1001);
        server.sendMessage(client_fd, response);
    } else {
        std::cout << "[LOGIN] Failed: Invalid credentials" << std::endl;
        NET::LoginFailure response("Invalid username or password");
        server.sendMessage(client_fd, response);
    }
}

// 处理结构化查询请求
void handleQuery(NET::SocketServer& server, int client_fd, const NET::QueryRequest& request) {
    std::cout << "[QUERY] Operation: " << static_cast<int>(request.getOperation()) << std::endl;
    
    if (!validateToken(request.getSessionToken())) {
        std::cout << "[QUERY] Token validation failed" << std::endl;
        NET::ErrorResponse response("Invalid or expired token", 401);
        server.sendMessage(client_fd, response);
        return;
    }
    
    std::cout << "[QUERY] Token validation successful" << std::endl;
    
    // 执行查询
    NET::QueryResponse response = executeQuery(request);
    server.sendMessage(client_fd, response);
    
    std::cout << "[QUERY] Response sent" << std::endl;
}
