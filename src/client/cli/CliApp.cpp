#include "CliApp.hpp"
#include "Lexer.hpp"
#include <iostream>
#include <memory>

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
        if (!logged_in) {
            std::cout << "Login failed. Please try again." << std::endl;
        }
        std::cout << "Logged in as " << username << std::endl; break; 
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

// --- DDL 处理函数 ---
void CliApp::handle_create_database(const CreateDatabaseCommand& cmd) {
    std::cout << "[Placeholder] Creating database: " << cmd.db_name << std::endl;
    // TODO: 序列化命令并发送到服务器
}

void CliApp::handle_drop_database(const DropDatabaseCommand& cmd) {
    std::cout << "[Placeholder] Dropping database: " << cmd.db_name << std::endl;


    // TODO: 序列化命令并发送到服务器
    if (cmd.db_name == current_database) {
        current_database.clear();
        std::cout << "Note: The current active database has been dropped." << std::endl;
    }
    
}

void CliApp::handle_use_database(const UseDatabaseCommand& cmd) {
    // TODO: 与服务器通信以验证数据库是否存在

    // --- 设置当前数据库上下文 ---
    current_database = cmd.db_name;
    std::cout << "Database context changed to '" << current_database << "'." << std::endl;
    
}

void CliApp::handle_create_table(const CreateTableCommand& cmd) {
    // --- 执行前检查数据库上下文 ---
    if (current_database.empty()) {
        std::cerr << "Error: No database selected. Use 'USE <database_name>;' first." << std::endl;
        return;
    }
    std::cout << "[Placeholder] Creating table '" << cmd.table_name << "' in database '" << current_database << "'." << std::endl;
    for (const auto& col : cmd.columns) {
        std::cout << "  Column: " << col.name << ", Type: " << to_string(col.type) 
                  << (col.is_primary ? ", PRIMARY KEY" : "") << std::endl;
    }
    // TODO: 序列化命令并发送到服务器
}

void CliApp::handle_drop_table(const DropTableCommand& cmd) {
    // --- 执行前检查数据库上下文 ---
    if (current_database.empty()) {
        std::cerr << "Error: No database selected. Use 'USE <database_name>;' first." << std::endl;
        return;
    }
    std::cout << "[Placeholder] Dropping table '" << cmd.table_name << "' from database '" << current_database << "'." << std::endl;
    // TODO: 序列化命令并发送到服务器
}

// --- DML 处理函数 ---
void CliApp::handle_insert(const InsertCommand& cmd) {
    // --- 执行前检查数据库上下文 ---
    if (current_database.empty()) {
        std::cerr << "Error: No database selected. Use 'USE <database_name>;' first." << std::endl;
        return;
    }
    std::cout << "[Placeholder] Inserting into table '" << cmd.table_name << "' in database '" << current_database << "'." << std::endl;
    // TODO: 序列化命令并发送到服务器
}

void CliApp::handle_select(const SelectCommand& cmd) {
    // --- 执行前检查数据库上下文 ---
    if (current_database.empty()) {
        std::cerr << "Error: No database selected. Use 'USE <database_name>;' first." << std::endl;
        return;
    }
    std::cout << "[Placeholder] Selecting from table '" << cmd.table_name << "' in database '" << current_database << "'." << std::endl;
    if(cmd.where_clause) std::cout << "  With WHERE clause." << std::endl;
    // TODO: 序列化命令并发送到服务器
}

void CliApp::handle_update(const UpdateCommand& cmd) {
    // --- 执行前检查数据库上下文 ---
    if (current_database.empty()) {
        std::cerr << "Error: No database selected. Use 'USE <database_name>;' first." << std::endl;
        return;
    }
    std::cout << "[Placeholder] Updating table '" << cmd.table_name << "' in database '" << current_database << "'." << std::endl;
    // TODO: 序列化命令并发送到服务器
}

void CliApp::handle_delete(const DeleteCommand& cmd) {
    // --- 执行前检查数据库上下文 ---
    if (current_database.empty()) {
        std::cerr << "Error: No database selected. Use 'USE <database_name>;' first." << std::endl;
        return;
    }
    std::cout << "[Placeholder] Deleting from table '" << cmd.table_name << "' in database '" << current_database << "'." << std::endl;
    // TODO: 序列化命令并发送到服务器
}

// --- 登录和登出功能 ---
bool CliApp::login(const std::string& username, const std::string& password) {
    //登陆成功返回true、失败返回false
    // TODO: 登录请求发送到服务器

}

void CliApp::logout() {
    logged_in = false;
    current_database.clear();
    // TODO: disconnect from server if applicable

    std::cout << "Logged out successfully." << std::endl; 
}
