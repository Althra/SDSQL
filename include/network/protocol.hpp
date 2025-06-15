/**
* @brief 网络层的协议定义
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <expected>
#include "serializer.hpp"

namespace NET {

enum class MessageType : uint8_t {
    // From Client to Server
    LOGIN_REQUEST = 0x10,
    QUERY_REQUEST = 0x20,
    PING_REQUEST = 0x30,

    // From Server to Client
    LOGIN_SUCCESS = 0x11,
    LOGIN_FAILURE = 0x12,
    QUERY_RESPONSE = 0x21,
    PONG_RESPONSE = 0x31,
    ERROR_RESPONSE = 0x99,
};

enum class ProtocolError {
    INVALID_MAGIC_NUMBER,
    INVALID_MESSAGE_TYPE,
    PAYLOAD_SIZE_MISMATCH,
    DESERIALIZATION_FAILED
};

enum class SQL_QUERY_TYPE : uint8_t {
    // DDL
    CREATE_DATABASE,
    DROP_DATABASE,
    USE_DATABASE,
    CREATE_TABLE,
    DROP_TABLE,
    // DML
    SELECT,
    INSERT,
    UPDATE,
    DELETE,
};

// 9 Byte Header
#pragma pack(push, 1)
class MessageHeader {
public:
    static constexpr uint32_t MAGIC_NUMBER = 0xDEADBEEF;
    static constexpr size_t HEADER_SIZE = 9;

private:
    uint32_t magic = MAGIC_NUMBER;
    MessageType type;
    uint32_t payload_size = 0;

public:
    explicit MessageHeader(MessageType msg_type);
    
    void setPayloadSize(uint32_t length);
    uint32_t getPayloadSize() const { return payload_size; }
    MessageType getType() const { return type; }
    
    void serialize(Serializer& serializer) const;
    static std::expected<MessageHeader, ProtocolError> deserialize(Deserializer& deserializer);
    
    bool isValid() const;
};
#pragma pack(pop)

class Message {
protected:
    MessageHeader header;

public:
    explicit Message(MessageType type);
    virtual ~Message() = default;

    virtual void serializePayload(Serializer& serializer) const = 0;
    virtual std::expected<void, ProtocolError> deserializePayload(Deserializer& deserializer) = 0;
    
    // 完整消息序列化（头部 + 载荷）
    std::vector<std::byte> serialize() const;
    
    // 获取消息类型
    MessageType getType() const { return header.getType(); }
    
    // 静态反序列化工厂方法
    static std::expected<std::unique_ptr<Message>, ProtocolError> deserialize(std::span<const std::byte> data);
};

// ========== 具体消息类型 ==========

// 登录请求
class LoginRequest : public Message {
private:
    std::string username;
    std::string password;

public:
    LoginRequest();
    LoginRequest(std::string user, std::string pass);
    
    void serializePayload(Serializer& serializer) const override;
    std::expected<void, ProtocolError> deserializePayload(Deserializer& deserializer) override;
    
    const std::string& getUsername() const { return username; }
    const std::string& getPassword() const { return password; }
    
    void setCredentials(std::string user, std::string pass);
};

// 登录成功响应
class LoginSuccess : public Message {
private:
    std::string session_token;
    uint32_t user_id;

public:
    LoginSuccess();
    LoginSuccess(std::string token, uint32_t uid);
    
    void serializePayload(Serializer& serializer) const override;
    std::expected<void, ProtocolError> deserializePayload(Deserializer& deserializer) override;
    
    const std::string& getSessionToken() const { return session_token; }
    uint32_t getUserId() const { return user_id; }
    
    void setSessionInfo(std::string token, uint32_t uid);
};

// 登录失败响应
class LoginFailure : public Message {
private:
    std::string error_message;

public:
    LoginFailure();
    LoginFailure(std::string message);
    
    void serializePayload(Serializer& serializer) const override;
    std::expected<void, ProtocolError> deserializePayload(Deserializer& deserializer) override;
    
    const std::string& getErrorMessage() const { return error_message; }
};

// 查询响应
class QueryResponse : public Message {
public:
    struct Row {
        std::vector<std::string> columns;
    };

private:
    std::vector<std::string> column_names;
    std::vector<Row> rows;
    bool success;
    std::string error_message;

public:
    QueryResponse();
    QueryResponse(std::vector<std::string> columns, std::vector<Row> data);
    QueryResponse(std::string error); // 错误响应构造函数
    
    void serializePayload(Serializer& serializer) const override;
    std::expected<void, ProtocolError> deserializePayload(Deserializer& deserializer) override;
    
    const std::vector<std::string>& getColumnNames() const { return column_names; }
    const std::vector<Row>& getRows() const { return rows; }
    bool isSuccess() const { return success; }
    const std::string& getErrorMessage() const { return error_message; }
    
    void setResult(std::vector<std::string> columns, std::vector<Row> data);
    void setError(std::string error);
};

// Ping请求（心跳）
class PingRequest : public Message {
private:
    uint64_t timestamp;

public:
    PingRequest();
    explicit PingRequest(uint64_t ts);
    
    void serializePayload(Serializer& serializer) const override;
    std::expected<void, ProtocolError> deserializePayload(Deserializer& deserializer) override;
    
    uint64_t getTimestamp() const { return timestamp; }
    void setTimestamp(uint64_t ts) { timestamp = ts; }
};

// Pong响应（心跳响应）
class PongResponse : public Message {
private:
    uint64_t original_timestamp;
    uint64_t server_timestamp;

public:
    PongResponse();
    PongResponse(uint64_t orig_ts, uint64_t server_ts);
    
    void serializePayload(Serializer& serializer) const override;
    std::expected<void, ProtocolError> deserializePayload(Deserializer& deserializer) override;
    
    uint64_t getOriginalTimestamp() const { return original_timestamp; }
    uint64_t getServerTimestamp() const { return server_timestamp; }
    
    void setTimestamps(uint64_t orig_ts, uint64_t server_ts);
};

// 错误响应
class ErrorResponse : public Message {
private:
    std::string error_message;
    uint32_t error_code;

public:
    ErrorResponse();
    ErrorResponse(std::string message, uint32_t code);
    
    void serializePayload(Serializer& serializer) const override;
    std::expected<void, ProtocolError> deserializePayload(Deserializer& deserializer) override;
    
    const std::string& getErrorMessage() const { return error_message; }
    uint32_t getErrorCode() const { return error_code; }
    
    void setError(std::string message, uint32_t code);
};

// 消息工厂
class MessageFactory {
public:
    static std::expected<std::unique_ptr<Message>, ProtocolError> createMessage(MessageType type);
};

} // namespace NET