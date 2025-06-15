#ifndef CLI_APP_HPP
#define CLI_APP_HPP

#include "Parser.hpp"
#include <string>

#include "../network/socket_client.hpp"
#include "../network/query.hpp"

class CliApp {
private:
    //当前数据库的上下文 
    std::string current_database;

    // 用户名和密码
    std::string username;
    std::string password;
    // 登录状态  
    bool logged_in = false;

    std::string server_ip = "127.0.0.1";
    short server_port = 4399;

    // 网络相关
    NET::SocketClient client;
    NET::NetworkQueryExecutor query_executor;
    std::string session_token;

    void execute(const std::string& line);

    bool login(const std::string& username, const std::string& password);
    void logout();

    void main_loop(int logged_in);

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

    
    // 辅助方法
    bool executeQuery(const NET::QueryRequest& request);
    bool handleQueryResponse(const NET::QueryResponse& response);

public:
    CliApp() : query_executor(client) {}

    void run();
};
#endif // CLI_APP_HPP