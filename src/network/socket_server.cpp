#include "../../include/network/socket_server.hpp"
#include <cstring>

namespace NET {

SocketServer::SocketServer() = default;

SocketServer::~SocketServer() {
    stop();
}

std::expected<void, SocketError> SocketServer::start(const std::string& ip, uint16_t port) {
    if (running) {
        return {}; // 已经在运行
    }

    // 创建socket
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        return std::unexpected(SocketError::SOCKET_CREATE_FAILED);
    }

    // 设置地址重用
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // 设置服务器地址
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    
    if (inet_pton(AF_INET, ip.c_str(), &server_addr.sin_addr) != 1) {
        close(server_fd);
        server_fd = -1;
        return std::unexpected(SocketError::INVALID_ADDRESS);
    }

    // 绑定
    if (bind(server_fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) < 0) {
        close(server_fd);
        server_fd = -1;
        return std::unexpected(SocketError::BIND_FAILED);
    }

    // 监听
    if (listen(server_fd, 10) < 0) {
        close(server_fd);
        server_fd = -1;
        return std::unexpected(SocketError::LISTEN_FAILED);
    }

    running = true;
    return {};
}

void SocketServer::stop() {
    if (!running) {
        return;
    }

    running = false;
    if (server_fd != -1) {
        close(server_fd);
        server_fd = -1;
    }
}

std::expected<int, SocketError> SocketServer::acceptClient() {
    if (!running) {
        return std::unexpected(SocketError::SOCKET_CREATE_FAILED);
    }

    sockaddr_in client_addr{};
    socklen_t addr_len = sizeof(client_addr);

    int client_fd = accept(server_fd, reinterpret_cast<sockaddr*>(&client_addr), &addr_len);
    if (client_fd < 0) {
        return std::unexpected(SocketError::ACCEPT_FAILED);
    }

    return client_fd;
}

std::expected<std::unique_ptr<Message>, SocketError> SocketServer::receiveMessage(int client_fd) {
    // 先接收消息头
    auto header_bytes = receiveBytes(client_fd, MessageHeader::HEADER_SIZE);
    if (!header_bytes.has_value()) {
        return std::unexpected(header_bytes.error());
    }

    // 解析头部
    Deserializer deserializer(header_bytes.value());
    auto header_result = MessageHeader::deserialize(deserializer);
    if (!header_result.has_value()) {
        return std::unexpected(SocketError::RECV_FAILED);
    }

    MessageHeader header = header_result.value();
    uint32_t payload_size = header.getPayloadSize();

    // 接收载荷数据
    std::vector<std::byte> full_message = std::move(header_bytes.value());
    if (payload_size > 0) {
        auto payload_bytes = receiveBytes(client_fd, payload_size);
        if (!payload_bytes.has_value()) {
            return std::unexpected(payload_bytes.error());
        }
        full_message.insert(full_message.end(), payload_bytes.value().begin(), payload_bytes.value().end());
    }

    // 反序列化完整消息
    auto message = Message::deserialize(full_message);
    if (!message.has_value()) {
        return std::unexpected(SocketError::RECV_FAILED);
    }
    
    return std::move(message.value());
}

std::expected<void, SocketError> SocketServer::sendMessage(int client_fd, const Message& message) {
    auto serialized = message.serialize();
    return sendBytes(client_fd, serialized);
}

void SocketServer::disconnectClient(int client_fd) {
    if (client_fd >= 0) {
        close(client_fd);
    }
}

std::expected<std::vector<std::byte>, SocketError> SocketServer::receiveBytes(int fd, size_t size) {
    std::vector<std::byte> buffer(size);
    size_t total_received = 0;

    while (total_received < size) {
        ssize_t received = recv(fd, 
                               reinterpret_cast<char*>(buffer.data()) + total_received, 
                               size - total_received, 0);
        
        if (received < 0) {
            return std::unexpected(SocketError::RECV_FAILED);
        }
        
        if (received == 0) {
            return std::unexpected(SocketError::CONNECTION_CLOSED);
        }
        
        total_received += received;
    }

    return buffer;
}

std::expected<void, SocketError> SocketServer::sendBytes(int fd, const std::vector<std::byte>& data) {
    size_t total_sent = 0;
    const char* char_data = reinterpret_cast<const char*>(data.data());

    while (total_sent < data.size()) {
        ssize_t sent = send(fd, char_data + total_sent, data.size() - total_sent, 0);
        
        if (sent < 0) {
            return std::unexpected(SocketError::SEND_FAILED);
        }
        
        total_sent += sent;
    }

    return {};
}

} // namespace NET