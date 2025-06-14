#ifndef CLI_APP_HPP
#define CLI_APP_HPP

#include "Parser.hpp"
#include <string>

class CliApp {
public:
    void run();

private:
    //当前数据库的上下文 
    std::string current_database;

    void execute(const std::string& line);

    // DDL Handlers
    void handle_create_database(const CreateDatabaseCommand& cmd);
    void handle_drop_database(const DropDatabaseCommand& cmd);
    void handle_use_database(const UseDatabaseCommand& cmd);
    void handle_create_table(const CreateTableCommand& cmd);
    void handle_drop_table(const DropTableCommand& cmd);

    // DML Handlers
    void handle_insert(const InsertCommand& cmd);
    void handle_select(const SelectCommand& cmd);
    void handle_update(const UpdateCommand& cmd);
    void handle_delete(const DeleteCommand& cmd);
};
#endif // CLI_APP_HPP