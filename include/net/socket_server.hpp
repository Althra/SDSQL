#pragma once

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>

#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <expected>

#include "protocol.hpp"
#include "socket_utils.hpp"

namespace NET {

class SocketServer {
private:
    int server_fd = -1;
    sockaddr_in server_addr{};
    bool running = false;

public:
    SocketServer();
    ~SocketServer();

    // 启动服务器
    std::expected<void, SocketError> start(const std::string& ip, uint16_t port);
    
    // 停止服务器
    void stop();
    
    // 等待客户端连接
    std::expected<int, SocketError> acceptClient();
    
    // 接收消息
    std::expected<std::unique_ptr<Message>, SocketError> receiveMessage(int client_fd);
    
    // 发送消息
    std::expected<void, SocketError> sendMessage(int client_fd, const Message& message);
    
    // 断开客户端
    void disconnectClient(int client_fd);
    
    // 检查是否运行
    bool isRunning() const { return running; }

private:
    std::expected<std::vector<std::byte>, SocketError> receiveBytes(int fd, size_t size);
    std::expected<void, SocketError> sendBytes(int fd, const std::vector<std::byte>& data);
};

} // namespace NET