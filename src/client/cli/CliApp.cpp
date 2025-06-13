#include "CliApp.hpp"
#include "Lexer.hpp"
#include <iostream>
#include <memory>

void CliApp::run() {
    std::string line;
    while (true) {
        std::cout << "DB_CLI> ";
        if (!std::getline(std::cin, line) || line == "exit" || line == "quit") break;
        execute(line);
    }
    std::cout << "Goodbye!" << std::endl;
}

void CliApp::execute(const std::string& line) {
    if (line.empty()) return;
    try {
        Lexer lexer(line);
        auto tokens = lexer.tokenize();
        Parser parser(tokens);
        std::unique_ptr<Command> command_obj = parser.parse();

        if (!command_obj) return;

        // --- 主命令分派逻辑 ---
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

// --- DDL 处理函数框架 ---
void CliApp::handle_create_database(const CreateDatabaseCommand& cmd) {
    std::cout << "[Placeholder] Creating database: " << cmd.db_name << std::endl;
    // TODO: 序列化命令并发送到服务器
}
void CliApp::handle_drop_database(const DropDatabaseCommand& cmd) {
    std::cout << "[Placeholder] Dropping database: " << cmd.db_name << std::endl;
    // TODO: 序列化命令并发送到服务器
}
void CliApp::handle_use_database(const UseDatabaseCommand& cmd) {
    std::cout << "[Placeholder] Using database: " << cmd.db_name << std::endl;
    // TODO: 序列化命令并发送到服务器
}
void CliApp::handle_create_table(const CreateTableCommand& cmd) {
    std::cout << "[Placeholder] Creating table: " << cmd.table_name << std::endl;
    for (const auto& col : cmd.columns) {
        std::cout << "  Column: " << col.name << ", Type: " << to_string(col.type) 
                  << (col.is_primary ? ", PRIMARY KEY" : "") << std::endl;
    }
    // TODO: 序列化命令并发送到服务器
}
void CliApp::handle_drop_table(const DropTableCommand& cmd) {
    std::cout << "[Placeholder] Dropping table: " << cmd.table_name << std::endl;
    // TODO: 序列化命令并发送到服务器
}

// --- DML 处理函数框架 ---
void CliApp::handle_insert(const InsertCommand& cmd) {
    std::cout << "[Placeholder] Inserting into table: " << cmd.table_name << std::endl;
    // TODO: 序列化命令并发送到服务器
}
void CliApp::handle_select(const SelectCommand& cmd) {
    std::cout << "[Placeholder] Selecting from table: " << cmd.table_name << std::endl;
    if(cmd.where_clause) std::cout << "  With WHERE clause." << std::endl;
    // TODO: 序列化命令并发送到服务器
}
void CliApp::handle_update(const UpdateCommand& cmd) {
    std::cout << "[Placeholder] Updating table: " << cmd.table_name << std::endl;
    // TODO: 序列化命令并发送到服务器
}
void CliApp::handle_delete(const DeleteCommand& cmd) {
    std::cout << "[Placeholder] Deleting from table: " << cmd.table_name << std::endl;
    // TODO: 序列化命令并发送到服务器
}