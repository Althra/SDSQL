#pragma once

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <expected>

#include "protocol.hpp"
#include "socket_utils.hpp"

namespace NET {

class SocketClient {
private:
    int socket_fd = -1;
    sockaddr_in server_addr{};
    bool connected = false;

public:
    SocketClient();
    ~SocketClient();

    // 连接到服务器
    std::expected<void, SocketError> connect(const std::string& ip, uint16_t port);
    
    // 断开连接
    void disconnect();
    
    // 发送消息
    std::expected<void, SocketError> sendMessage(const Message& message);
    
    // 接收消息
    std::expected<std::unique_ptr<Message>, SocketError> receiveMessage();
    
    // 检查连接状态
    bool isConnected() const { return connected; }

private:
    std::expected<std::vector<std::byte>, SocketError> receiveBytes(size_t size);
    std::expected<void, SocketError> sendBytes(const std::vector<std::byte>& data);
};

} // namespace NET