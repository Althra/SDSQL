#include "../include/net/socket_server.hpp"
#include <iostream>

#define USERNAME "user"
#define PASSWORD "123456"

int main() {
    NET::SocketServer server;
    if (!server.start("0.0.0.0", 8888)) {
        std::cout << "[SDSQL SERVER] Server start failed!" << std::endl;
        return 1;
    }
    std::cout << "[SDSQL SERVER] Listening on 0.0.0.0:8888" << std::endl;

    while (server.isRunning()) {
        auto client_result = server.acceptClient();
        if (!client_result.has_value()) continue;
        int client_fd = client_result.value();

        // 接收登录消息
        auto msg_result = server.receiveMessage(client_fd);
        if (!msg_result.has_value()) {
            server.disconnectClient(client_fd);
            continue;
        }
        auto message = std::move(msg_result.value());

        if (message->getType() == NET::MessageType::LOGIN_REQUEST) {
            auto* login_req = dynamic_cast<NET::LoginRequest*>(message.get());
            std::string username = login_req->getUsername();
            std::string password = login_req->getPassword();

            if (username == USERNAME && password == PASSWORD) {
                NET::LoginSuccess resp("session_token_xxx", 1);
                server.sendMessage(client_fd, resp);
            } else {
                NET::LoginFailure resp("用户名或密码错误");
                server.sendMessage(client_fd, resp);
            }
            // ============================
        }

        server.disconnectClient(client_fd); // 简单例子：处理完就断开
    }
    return 0;
}