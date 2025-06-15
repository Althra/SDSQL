#include "../../include/client/CliApp.hpp"
#include "../../include/client/Lexer.hpp"
#include <iostream>
#include <memory>

#include "../../include/network/socket_client.hpp"
#include "../../include/network/query.hpp"
#include <iomanip>

void CliApp::run() {
    std::string line;
    // --- 初始化当前数据库上下文 ---
    std::cout << "Type 'exit' or 'quit' to exit." << std::endl;

    while (!logged_in)
    {
        std::cout << "Enter username: ";
        std::getline(std::cin, username);
        std::cout << "Enter password: ";
        std::getline(std::cin, password);
        logged_in = login(username, password);
        if (logged_in){
        std::cout << "Logged in as " << username << std::endl; break; 
        }
        std::cout << "Login failed. Please try again." << std::endl;
    }
    
    while (logged_in) {
        // --- 提示符现在会显示当前的数据库上下文 ---
        std::cout << "DB_CLI";
        if (!current_database.empty()) {
            std::cout << " [" << current_database << "]";
        }
        std::cout << "> ";
        
        if (!std::getline(std::cin, line) || line == "exit" || line == "quit") break;
        execute(line);
    }
    logout();
    std::cout << "\nGoodbye!" << std::endl;
}

void CliApp::execute(const std::string& line) {
    if (line.empty()) return;
    try {
        Lexer lexer(line);
        auto tokens = lexer.tokenize();
        Parser parser(tokens);
        std::unique_ptr<Command> command_obj = parser.parse();

        if (!command_obj) return;

        //主命令分派逻辑
        if (auto* cmd = dynamic_cast<CreateDatabaseCommand*>(command_obj.get())) handle_create_database(*cmd);
        else if (auto* cmd = dynamic_cast<DropDatabaseCommand*>(command_obj.get())) handle_drop_database(*cmd);
        else if (auto* cmd = dynamic_cast<UseDatabaseCommand*>(command_obj.get())) handle_use_database(*cmd);
        else if (auto* cmd = dynamic_cast<CreateTableCommand*>(command_obj.get())) handle_create_table(*cmd);
        else if (auto* cmd = dynamic_cast<DropTableCommand*>(command_obj.get())) handle_drop_table(*cmd);
        else if (auto* cmd = dynamic_cast<InsertCommand*>(command_obj.get())) handle_insert(*cmd);
        else if (auto* cmd = dynamic_cast<SelectCommand*>(command_obj.get())) handle_select(*cmd);
        else if (auto* cmd = dynamic_cast<UpdateCommand*>(command_obj.get())) handle_update(*cmd);
        else if (auto* cmd = dynamic_cast<DeleteCommand*>(command_obj.get())) handle_delete(*cmd);
        else std::cerr << "Error: Unhandled command type." << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
}

// 辅助函数：TokenType转换为字符串
std::string tokenTypeToString(TokenType type) {
    switch (type) {
        case TokenType::KEYWORD_INT: return "INT";
        case TokenType::KEYWORD_STRING: return "STRING";
        default: return "UNKNOWN";
    }
}

// 通用的查询执行函数
bool CliApp::executeQuery(const NET::QueryRequest& request) {
    if (!query_executor.isAuthenticated()) {
        std::cerr << "Error: Not logged in. Please login first." << std::endl;
        return false;
    }
    
    auto result = query_executor.executeQuery(request);
    if (result.has_value()) {
        handleQueryResponse(*result.value());
        return true;
    } else {
        std::cerr << "Error: Failed to execute query." << std::endl;
        return false;
    }
}

// 格式化显示查询结果
void CliApp::handleQueryResponse(const NET::QueryResponse& response) {
    if (response.isSuccess()) {
        const auto& columns = response.getColumnNames();
        const auto& rows = response.getRows();
        
        if (columns.empty()) {
            // DDL操作或无结果集的DML操作
            std::cout << "✓ Command executed successfully." << std::endl;
            if (!rows.empty()) {
                std::cout << "Affected rows: " << rows.size() << std::endl;
            }
            return;
        }
        
        // 计算每列的最大宽度
        std::vector<size_t> column_widths;
        for (size_t i = 0; i < columns.size(); ++i) {
            size_t max_width = columns[i].length();
            for (const auto& row : rows) {
                if (i < row.columns.size()) {
                    max_width = std::max(max_width, row.columns[i].length());
                }
            }
            column_widths.push_back(std::max(max_width, size_t(8))); // 最小宽度为8
        }
        
        // 打印表格头部边框
        std::cout << "+";
        for (size_t width : column_widths) {
            std::cout << std::string(width + 2, '-') << "+";
        }
        std::cout << std::endl;
        
        // 打印列名
        std::cout << "|";
        for (size_t i = 0; i < columns.size(); ++i) {
            std::cout << " " << std::setw(column_widths[i]) << std::left << columns[i] << " |";
        }
        std::cout << std::endl;
        
        // 打印分隔线
        std::cout << "+";
        for (size_t width : column_widths) {
            std::cout << std::string(width + 2, '-') << "+";
        }
        std::cout << std::endl;
        
        // 打印数据行
        for (const auto& row : rows) {
            std::cout << "|";
            for (size_t i = 0; i < columns.size(); ++i) {
                std::string cell = (i < row.columns.size()) ? row.columns[i] : "";
                std::cout << " " << std::setw(column_widths[i]) << std::left << cell << " |";
            }
            std::cout << std::endl;
        }
        
        // 打印底部边框
        std::cout << "+";
        for (size_t width : column_widths) {
            std::cout << std::string(width + 2, '-') << "+";
        }
        std::cout << std::endl;
        
        std::cout << "(" << rows.size() << " row" << (rows.size() != 1 ? "s" : "") << ")" << std::endl;
    } else {
        std::cerr << "✗ Error: " << response.getErrorMessage() << std::endl;
    }
}

// --- DDL 处理函数 ---
void CliApp::handle_create_database(const CreateDatabaseCommand& cmd) {
    std::cout << "Creating database: " << cmd.db_name << std::endl;
    
    auto request = NET::QueryBuilder::buildCreateDatabase(cmd);
    request.setSessionToken(session_token);
    
    if (executeQuery(request)) {
        std::cout << "Database '" << cmd.db_name << "' created successfully." << std::endl;
    }
}

void CliApp::handle_drop_database(const DropDatabaseCommand& cmd) {
    std::cout << "Dropping database: " << cmd.db_name << std::endl;
    
    auto request = NET::QueryBuilder::buildDropDatabase(cmd);
    request.setSessionToken(session_token);
    
    if (executeQuery(request)) {
        std::cout << "Database '" << cmd.db_name << "' dropped successfully." << std::endl;
        
        // 如果删除的是当前使用的数据库，清除上下文
        if (cmd.db_name == current_database) {
            current_database.clear();
            std::cout << "Note: The current active database has been dropped." << std::endl;
        }
    }
}

void CliApp::handle_use_database(const UseDatabaseCommand& cmd) {
    std::cout << "Switching to database: " << cmd.db_name << std::endl;
    
    auto request = NET::QueryBuilder::buildUseDatabase(cmd);
    request.setSessionToken(session_token);
    
    if (executeQuery(request)) {
        // 服务器验证成功后，设置当前数据库上下文
        current_database = cmd.db_name;
        std::cout << "✓ Database context changed to '" << current_database << "'." << std::endl;
    }
}

void CliApp::handle_create_table(const CreateTableCommand& cmd) {
    // --- 执行前检查数据库上下文 ---
    if (current_database.empty()) {
        std::cerr << "Error: No database selected. Use 'USE <database_name>;' first." << std::endl;
        return;
    }
    
    std::cout << "Creating table '" << cmd.table_name << "' in database '" << current_database << "':" << std::endl;
    
    // 显示表结构信息
    for (const auto& col : cmd.columns) {
        std::cout << "  • Column: " << col.name 
                  << ", Type: " << tokenTypeToString(col.type)
                  << (col.is_primary ? " [PRIMARY KEY]" : "") << std::endl;
    }
    
    auto request = NET::QueryBuilder::buildCreateTable(cmd);
    request.setSessionToken(session_token);
    
    if (executeQuery(request)) {
        std::cout << "✓ Table '" << cmd.table_name << "' created successfully." << std::endl;
    }
}

void CliApp::handle_drop_table(const DropTableCommand& cmd) {
    // --- 执行前检查数据库上下文 ---
    if (current_database.empty()) {
        std::cerr << "Error: No database selected. Use 'USE <database_name>;' first." << std::endl;
        return;
    }
    
    std::cout << "Dropping table '" << cmd.table_name << "' from database '" << current_database << "'." << std::endl;
    
    auto request = NET::QueryBuilder::buildDropTable(cmd);
    request.setSessionToken(session_token);
    
    if (executeQuery(request)) {
        std::cout << "✓ Table '" << cmd.table_name << "' dropped successfully." << std::endl;
    }
}

// --- DML 处理函数 ---
void CliApp::handle_insert(const InsertCommand& cmd) {
    // --- 执行前检查数据库上下文 ---
    if (current_database.empty()) {
        std::cerr << "Error: No database selected. Use 'USE <database_name>;' first." << std::endl;
        return;
    }
    
    std::cout << "Inserting into table '" << cmd.table_name << "' in database '" << current_database << "':" << std::endl;
    
    // 显示要插入的值
    std::cout << "  Values: (";
    for (size_t i = 0; i < cmd.values.size(); ++i) {
        std::cout << cmd.values[i].value;
        if (i < cmd.values.size() - 1) {
            std::cout << ", ";
        }
    }
    std::cout << ")" << std::endl;
    
    auto request = NET::QueryBuilder::buildInsert(cmd);
    request.setSessionToken(session_token);
    
    if (executeQuery(request)) {
        std::cout << "✓ Record inserted successfully." << std::endl;
    }
}

void CliApp::handle_select(const SelectCommand& cmd) {
    // --- 执行前检查数据库上下文 ---
    if (current_database.empty()) {
        std::cerr << "Error: No database selected. Use 'USE <database_name>;' first." << std::endl;
        return;
    }
    
    std::cout << "Selecting from table '" << cmd.table_name << "' in database '" << current_database << "':" << std::endl;
    
    // 显示查询信息
    if (cmd.select_all) {
        std::cout << "  Columns: *" << std::endl;
    } else {
        std::cout << "  Columns: ";
        for (size_t i = 0; i < cmd.columns.size(); ++i) {
            std::cout << cmd.columns[i];
            if (i < cmd.columns.size() - 1) {
                std::cout << ", ";
            }
        }
        std::cout << std::endl;
    }
    
    if (cmd.where_clause.has_value()) {
        const auto& where = cmd.where_clause.value();
        std::cout << "  WHERE: " << where.column << " " << where.op << " " << where.value.value << std::endl;
    }
    
    auto request = NET::QueryBuilder::buildSelect(cmd);
    request.setSessionToken(session_token);
    
    executeQuery(request);
}

void CliApp::handle_update(const UpdateCommand& cmd) {
    // --- 执行前检查数据库上下文 ---
    if (current_database.empty()) {
        std::cerr << "Error: No database selected. Use 'USE <database_name>;' first." << std::endl;
        return;
    }
    
    std::cout << "Updating table '" << cmd.table_name << "' in database '" << current_database << "':" << std::endl;
    
    // 显示SET子句
    std::cout << "  SET: ";
    for (size_t i = 0; i < cmd.set_clauses.size(); ++i) {
        const auto& set_clause = cmd.set_clauses[i];
        std::cout << set_clause.column << " = " << set_clause.value.value;
        if (i < cmd.set_clauses.size() - 1) {
            std::cout << ", ";
        }
    }
    std::cout << std::endl;
    
    if (cmd.where_clause.has_value()) {
        const auto& where = cmd.where_clause.value();
        std::cout << "  WHERE: " << where.column << " " << where.op << " " << where.value.value << std::endl;
    }
    
    auto request = NET::QueryBuilder::buildUpdate(cmd);
    request.setSessionToken(session_token);
    
    if (executeQuery(request)) {
        std::cout << "✓ Records updated successfully." << std::endl;
    }
}

void CliApp::handle_delete(const DeleteCommand& cmd) {
    // --- 执行前检查数据库上下文 ---
    if (current_database.empty()) {
        std::cerr << "Error: No database selected. Use 'USE <database_name>;' first." << std::endl;
        return;
    }
    
    std::cout << "Deleting from table '" << cmd.table_name << "' in database '" << current_database << "':" << std::endl;
    
    if (cmd.where_clause.has_value()) {
        const auto& where = cmd.where_clause.value();
        std::cout << "  WHERE: " << where.column << " " << where.op << " " << where.value.value << std::endl;
    } else {
        std::cout << "  WARNING: This will delete ALL records from the table!" << std::endl;
    }
    
    auto request = NET::QueryBuilder::buildDelete(cmd);
    request.setSessionToken(session_token);
    
    if (executeQuery(request)) {
        std::cout << "✓ Records deleted successfully." << std::endl;
    }
}

// --- 登录和登出功能 ---
bool CliApp::login(const std::string& username, const std::string& password) {
    // 连接服务器
    auto connect_result = client.connect(server_ip, server_port);
    if (!connect_result.has_value()) {
        std::cerr << "Failed to connect to server." << std::endl;
        return false;
    }
    
    // 发送登录请求
    NET::LoginRequest request(username, password);
    auto send_result = client.sendMessage(request);
    if (!send_result.has_value()) {
        std::cerr << "Failed to send login request." << std::endl;
        return false;
    }
    
    // 接收登录响应
    auto response_result = client.receiveMessage();
    if (!response_result.has_value()) {
        std::cerr << "Failed to receive login response." << std::endl;
        return false;
    }
    
    auto response = std::move(response_result.value());
    if (response->getType() == NET::MessageType::LOGIN_SUCCESS) {
        auto* login_success = dynamic_cast<NET::LoginSuccess*>(response.get());
        session_token = login_success->getSessionToken();
        query_executor.setSessionToken(session_token);
        std::cout << "✓ Login successful! Welcome, " << username << "!" << std::endl;
        return true;
    } else {
        auto* login_failure = dynamic_cast<NET::LoginFailure*>(response.get());
        std::cerr << "✗ Login failed: " << login_failure->getErrorMessage() << std::endl;
        return false;
    }
}

void CliApp::logout() {
    logged_in = false;
    current_database.clear();
    session_token.clear();
    query_executor.clearAuthentication();
    client.disconnect();
    
    std::cout << "Logged out successfully." << std::endl; 
}
