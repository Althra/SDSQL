#include "../../include/net/socket_client.hpp"
#include <cstring>

namespace NET {

SocketClient::SocketClient() = default;

SocketClient::~SocketClient() {
    disconnect();
}

std::expected<void, SocketError> SocketClient::connect(const std::string& ip, uint16_t port) {
    if (connected) {
        return {}; // 已经连接
    }

    // 创建socket
    socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd < 0) {
        return std::unexpected(SocketError::SOCKET_CREATE_FAILED);
    }

    // 设置服务器地址
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    
    if (inet_pton(AF_INET, ip.c_str(), &server_addr.sin_addr) != 1) {
        close(socket_fd);
        socket_fd = -1;
        return std::unexpected(SocketError::INVALID_ADDRESS);
    }

    // 连接到服务器
    if (::connect(socket_fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) < 0) {
        close(socket_fd);
        socket_fd = -1;
        return std::unexpected(SocketError::SEND_FAILED);
    }

    connected = true;
    return {};
}

void SocketClient::disconnect() {
    if (!connected) {
        return;
    }

    connected = false;
    if (socket_fd != -1) {
        close(socket_fd);
        socket_fd = -1;
    }
}

std::expected<void, SocketError> SocketClient::sendMessage(const Message& message) {
    if (!connected) {
        return std::unexpected(SocketError::SEND_FAILED);
    }

    auto serialized = message.serialize();
    return sendBytes(serialized);
}

std::expected<std::unique_ptr<Message>, SocketError> SocketClient::receiveMessage() {
    if (!connected) {
        return std::unexpected(SocketError::RECV_FAILED);
    }

    // 先接收消息头
    auto header_bytes = receiveBytes(MessageHeader::HEADER_SIZE);
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
        auto payload_bytes = receiveBytes(payload_size);
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

std::expected<std::vector<std::byte>, SocketError> SocketClient::receiveBytes(size_t size) {
    std::vector<std::byte> buffer(size);
    size_t total_received = 0;

    while (total_received < size) {
        ssize_t received = recv(socket_fd, 
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

std::expected<void, SocketError> SocketClient::sendBytes(const std::vector<std::byte>& data) {
    size_t total_sent = 0;
    const char* char_data = reinterpret_cast<const char*>(data.data());

    while (total_sent < data.size()) {
        ssize_t sent = send(socket_fd, char_data + total_sent, data.size() - total_sent, 0);
        
        if (sent < 0) {
            return std::unexpected(SocketError::SEND_FAILED);
        }
        
        total_sent += sent;
    }

    return {};
}

} // namespace NET