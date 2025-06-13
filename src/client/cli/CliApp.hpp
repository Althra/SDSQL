#ifndef CLI_APP_HPP
#define CLI_APP_HPP

#include "Parser.hpp"

class CliApp {
public:
    void run();

private:
    bool is_connected = false;
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