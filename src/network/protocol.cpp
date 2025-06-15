#include "../../include/network/protocol.hpp"
#include <chrono>

#include "../../include/network/query.hpp"

namespace NET {

// ========== MessageHeader Implementation ==========

MessageHeader::MessageHeader(MessageType msg_type) : type(msg_type) {}

void MessageHeader::setPayloadSize(uint32_t length) {
    payload_size = length;
}

void MessageHeader::serialize(Serializer& serializer) const {
    serializer.writeU32(magic);
    serializer.writeU8(static_cast<uint8_t>(type));
    serializer.writeU32(payload_size);
}

std::expected<MessageHeader, ProtocolError> MessageHeader::deserialize(Deserializer& deserializer) {
    auto magic_result = deserializer.readU32();
    if (!magic_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    
    if (magic_result.value() != MAGIC_NUMBER) {
        return std::unexpected(ProtocolError::INVALID_MAGIC_NUMBER);
    }
    
    auto type_result = deserializer.readU8();
    if (!type_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    
    auto payload_size_result = deserializer.readU32();
    if (!payload_size_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    
    MessageType msg_type = static_cast<MessageType>(type_result.value());
    MessageHeader header(msg_type);
    header.magic = magic_result.value();
    header.payload_size = payload_size_result.value();
    
    return header;
}

bool MessageHeader::isValid() const {
    return magic == MAGIC_NUMBER;
}

// ========== Message Implementation ==========

Message::Message(MessageType type) : header(type) {}

std::vector<std::byte> Message::serialize() const {
    // 先序列化载荷以获取大小
    Serializer payload_serializer;
    serializePayload(payload_serializer);
    
    // 设置头部载荷大小
    const_cast<MessageHeader&>(header).setPayloadSize(
        static_cast<uint32_t>(payload_serializer.size())
    );
    
    // 序列化完整消息
    Serializer full_serializer;
    full_serializer.reserve(MessageHeader::HEADER_SIZE + payload_serializer.size());
    
    header.serialize(full_serializer);
    full_serializer.writeBytes(payload_serializer.getBuffer());
    
    return std::vector<std::byte>(full_serializer.getBuffer().begin(), 
                                  full_serializer.getBuffer().end());
}

std::expected<std::unique_ptr<Message>, ProtocolError> 
Message::deserialize(std::span<const std::byte> data) {
    if (data.size() < MessageHeader::HEADER_SIZE) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    
    Deserializer deserializer(data);
    
    // 反序列化头部
    auto header_result = MessageHeader::deserialize(deserializer);
    if (!header_result.has_value()) {
        return std::unexpected(header_result.error());
    }
    
    MessageHeader header = header_result.value();
    
    // 检查数据完整性
    if (data.size() < MessageHeader::HEADER_SIZE + header.getPayloadSize()) {
        return std::unexpected(ProtocolError::PAYLOAD_SIZE_MISMATCH);
    }
    
    // 创建消息对象
    auto message_result = MessageFactory::createMessage(header.getType());
    if (!message_result.has_value()) {
        return std::unexpected(message_result.error());
    }
    
    auto message = std::move(message_result.value());
    
    // 反序列化载荷
    auto payload_result = message->deserializePayload(deserializer);
    if (!payload_result.has_value()) {
        return std::unexpected(payload_result.error());
    }
    
    return message;
}

// ========== LoginRequest Implementation ==========

LoginRequest::LoginRequest() : Message(MessageType::LOGIN_REQUEST) {}

LoginRequest::LoginRequest(std::string user, std::string pass) 
    : Message(MessageType::LOGIN_REQUEST), username(std::move(user)), password(std::move(pass)) {}

void LoginRequest::serializePayload(Serializer& serializer) const {
    serializer.writeString(username);
    serializer.writeString(password);
}

std::expected<void, ProtocolError> LoginRequest::deserializePayload(Deserializer& deserializer) {
    auto username_result = deserializer.readString();
    if (!username_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    
    auto password_result = deserializer.readString();
    if (!password_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    
    username = std::move(username_result.value());
    password = std::move(password_result.value());
    
    return {};
}

void LoginRequest::setCredentials(std::string user, std::string pass) {
    username = std::move(user);
    password = std::move(pass);
}

// ========== LoginSuccess Implementation ==========

LoginSuccess::LoginSuccess() : Message(MessageType::LOGIN_SUCCESS), user_id(0) {}

LoginSuccess::LoginSuccess(std::string token, uint32_t uid) 
    : Message(MessageType::LOGIN_SUCCESS), session_token(std::move(token)), user_id(uid) {}

void LoginSuccess::serializePayload(Serializer& serializer) const {
    serializer.writeString(session_token);
    serializer.writeU32(user_id);
}

std::expected<void, ProtocolError> LoginSuccess::deserializePayload(Deserializer& deserializer) {
    auto token_result = deserializer.readString();
    if (!token_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    
    auto uid_result = deserializer.readU32();
    if (!uid_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    
    session_token = std::move(token_result.value());
    user_id = uid_result.value();
    
    return {};
}

void LoginSuccess::setSessionInfo(std::string token, uint32_t uid) {
    session_token = std::move(token);
    user_id = uid;
}

// ========== LoginFailure Implementation ==========

LoginFailure::LoginFailure() : Message(MessageType::LOGIN_FAILURE) {}

LoginFailure::LoginFailure(std::string message) 
    : Message(MessageType::LOGIN_FAILURE), error_message(std::move(message)) {}

void LoginFailure::serializePayload(Serializer& serializer) const {
    serializer.writeString(error_message);
}

std::expected<void, ProtocolError> LoginFailure::deserializePayload(Deserializer& deserializer) {
    auto message_result = deserializer.readString();
    if (!message_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    
    error_message = std::move(message_result.value());
    
    return {};
}

// ========== QueryResponse Implementation ==========

QueryResponse::QueryResponse() : Message(MessageType::QUERY_RESPONSE), success(true) {}

QueryResponse::QueryResponse(std::vector<std::string> columns, std::vector<Row> data) 
    : Message(MessageType::QUERY_RESPONSE), column_names(std::move(columns)), 
      rows(std::move(data)), success(true) {}

QueryResponse::QueryResponse(std::string error) 
    : Message(MessageType::QUERY_RESPONSE), success(false), error_message(std::move(error)) {}

void QueryResponse::serializePayload(Serializer& serializer) const {
    serializer.writeU8(success ? 1 : 0);
    
    if (success) {
        // 序列化列名
        serializer.writeU32(static_cast<uint32_t>(column_names.size()));
        for (const auto& column : column_names) {
            serializer.writeString(column);
        }
        
        // 序列化行数据
        serializer.writeU32(static_cast<uint32_t>(rows.size()));
        for (const auto& row : rows) {
            serializer.writeU32(static_cast<uint32_t>(row.columns.size()));
            for (const auto& cell : row.columns) {
                serializer.writeString(cell);
            }
        }
    } else {
        serializer.writeString(error_message);
    }
}

std::expected<void, ProtocolError> QueryResponse::deserializePayload(Deserializer& deserializer) {
    auto success_result = deserializer.readU8();
    if (!success_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    
    success = (success_result.value() != 0);
    
    if (success) {
        // 反序列化列名
        auto column_count_result = deserializer.readU32();
        if (!column_count_result.has_value()) {
            return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
        }
        
        uint32_t column_count = column_count_result.value();
        column_names.clear();
        column_names.reserve(column_count);
        
        for (uint32_t i = 0; i < column_count; ++i) {
            auto column_result = deserializer.readString();
            if (!column_result.has_value()) {
                return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
            }
            column_names.push_back(std::move(column_result.value()));
        }
        
        // 反序列化行数据
        auto row_count_result = deserializer.readU32();
        if (!row_count_result.has_value()) {
            return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
        }
        
        uint32_t row_count = row_count_result.value();
        rows.clear();
        rows.reserve(row_count);
        
        for (uint32_t i = 0; i < row_count; ++i) {
            auto cell_count_result = deserializer.readU32();
            if (!cell_count_result.has_value()) {
                return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
            }
            
            uint32_t cell_count = cell_count_result.value();
            Row row;
            row.columns.reserve(cell_count);
            
            for (uint32_t j = 0; j < cell_count; ++j) {
                auto cell_result = deserializer.readString();
                if (!cell_result.has_value()) {
                    return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
                }
                row.columns.push_back(std::move(cell_result.value()));
            }
            
            rows.push_back(std::move(row));
        }
    } else {
        auto error_result = deserializer.readString();
        if (!error_result.has_value()) {
            return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
        }
        error_message = std::move(error_result.value());
    }
    
    return {};
}

void QueryResponse::setResult(std::vector<std::string> columns, std::vector<Row> data) {
    success = true;
    column_names = std::move(columns);
    rows = std::move(data);
    error_message.clear();
}

void QueryResponse::setError(std::string error) {
    success = false;
    error_message = std::move(error);
    column_names.clear();
    rows.clear();
}

// ========== PingRequest Implementation ==========

PingRequest::PingRequest() : Message(MessageType::PING_REQUEST) {
    timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

PingRequest::PingRequest(uint64_t ts) : Message(MessageType::PING_REQUEST), timestamp(ts) {}

void PingRequest::serializePayload(Serializer& serializer) const {
    serializer.writeU64(timestamp);
}

std::expected<void, ProtocolError> PingRequest::deserializePayload(Deserializer& deserializer) {
    auto ts_result = deserializer.readU64();
    if (!ts_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    
    timestamp = ts_result.value();
    return {};
}

// ========== PongResponse Implementation ==========

PongResponse::PongResponse() : Message(MessageType::PONG_RESPONSE), original_timestamp(0) {
    server_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

PongResponse::PongResponse(uint64_t orig_ts, uint64_t server_ts) 
    : Message(MessageType::PONG_RESPONSE), original_timestamp(orig_ts), server_timestamp(server_ts) {}

void PongResponse::serializePayload(Serializer& serializer) const {
    serializer.writeU64(original_timestamp);
    serializer.writeU64(server_timestamp);
}

std::expected<void, ProtocolError> PongResponse::deserializePayload(Deserializer& deserializer) {
    auto orig_ts_result = deserializer.readU64();
    if (!orig_ts_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    
    auto server_ts_result = deserializer.readU64();
    if (!server_ts_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    
    original_timestamp = orig_ts_result.value();
    server_timestamp = server_ts_result.value();
    
    return {};
}

void PongResponse::setTimestamps(uint64_t orig_ts, uint64_t server_ts) {
    original_timestamp = orig_ts;
    server_timestamp = server_ts;
}

// ========== ErrorResponse Implementation ==========

ErrorResponse::ErrorResponse() : Message(MessageType::ERROR_RESPONSE), error_code(0) {}

ErrorResponse::ErrorResponse(std::string message, uint32_t code) 
    : Message(MessageType::ERROR_RESPONSE), error_message(std::move(message)), error_code(code) {}

void ErrorResponse::serializePayload(Serializer& serializer) const {
    serializer.writeString(error_message);
    serializer.writeU32(error_code);
}

std::expected<void, ProtocolError> ErrorResponse::deserializePayload(Deserializer& deserializer) {
    auto message_result = deserializer.readString();
    if (!message_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    
    auto code_result = deserializer.readU32();
    if (!code_result.has_value()) {
        return std::unexpected(ProtocolError::DESERIALIZATION_FAILED);
    }
    
    error_message = std::move(message_result.value());
    error_code = code_result.value();
    
    return {};
}

void ErrorResponse::setError(std::string message, uint32_t code) {
    error_message = std::move(message);
    error_code = code;
}

// ========== MessageFactory Implementation ==========

std::expected<std::unique_ptr<Message>, ProtocolError>
MessageFactory::createMessage(MessageType type) {
    switch (type) {
        case MessageType::LOGIN_REQUEST:
            return std::make_unique<LoginRequest>();
        case MessageType::LOGIN_SUCCESS:
            return std::make_unique<LoginSuccess>();
        case MessageType::LOGIN_FAILURE:
            return std::make_unique<LoginFailure>();
        case MessageType::QUERY_REQUEST:
            return std::make_unique<QueryRequest>();
        case MessageType::QUERY_RESPONSE:
            return std::make_unique<QueryResponse>();
        case MessageType::PING_REQUEST:
            return std::make_unique<PingRequest>();
        case MessageType::PONG_RESPONSE:
            return std::make_unique<PongResponse>();
        case MessageType::ERROR_RESPONSE:
            return std::make_unique<ErrorResponse>();
        default:
            return std::unexpected(ProtocolError::INVALID_MESSAGE_TYPE);
    }
}

} // namespace NET