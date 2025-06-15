#include "../../include/network/serializer.hpp"
#include <arpa/inet.h>
#include <cstring>

namespace NET {

// ========== Serializer Implementation ==========

Serializer::Serializer() : buffer() {}

Serializer::Serializer(size_t initial_capacity) : buffer() {
    buffer.reserve(initial_capacity);
}

void Serializer::writeU8(uint8_t value) {
    buffer.push_back(static_cast<std::byte>(value));
}

void Serializer::writeU16(uint16_t value) {
    uint16_t network_value = htons(value);
    const std::byte* bytes = reinterpret_cast<const std::byte*>(&network_value);
    buffer.insert(buffer.end(), bytes, bytes + sizeof(uint16_t));
}

void Serializer::writeU32(uint32_t value) {
    uint32_t network_value = htonl(value);
    const std::byte* bytes = reinterpret_cast<const std::byte*>(&network_value);
    buffer.insert(buffer.end(), bytes, bytes + sizeof(uint32_t));
}

void Serializer::writeU64(uint64_t value) {
    uint32_t high = htonl(static_cast<uint32_t>(value >> 32));
    uint32_t low = htonl(static_cast<uint32_t>(value & 0xFFFFFFFF));
    
    const std::byte* high_bytes = reinterpret_cast<const std::byte*>(&high);
    const std::byte* low_bytes = reinterpret_cast<const std::byte*>(&low);
    
    buffer.insert(buffer.end(), high_bytes, high_bytes + sizeof(uint32_t));
    buffer.insert(buffer.end(), low_bytes, low_bytes + sizeof(uint32_t));
}

void Serializer::writeString(const std::string& str) {
    // 写入字符串长度（4字节）
    uint32_t length = static_cast<uint32_t>(str.length());
    writeU32(length);
    
    // 写入字符串内容
    const std::byte* str_bytes = reinterpret_cast<const std::byte*>(str.data());
    buffer.insert(buffer.end(), str_bytes, str_bytes + str.length());
}

void Serializer::writeBytes(const std::byte* data, size_t size) {
    if (data != nullptr && size > 0) {
        buffer.insert(buffer.end(), data, data + size);
    }
}

void Serializer::writeBytes(std::span<const std::byte> data) {
    buffer.insert(buffer.end(), data.begin(), data.end());
}

void Serializer::writeRaw(const void* data, size_t size) {
    if (data != nullptr && size > 0) {
        const std::byte* byte_data = reinterpret_cast<const std::byte*>(data);
        buffer.insert(buffer.end(), byte_data, byte_data + size);
    }
}

std::span<const std::byte> Serializer::getBuffer() const noexcept {
    return std::span<const std::byte>(buffer);
}

const std::byte* Serializer::data() const noexcept {
    return buffer.data();
}

size_t Serializer::size() const noexcept {
    return buffer.size();
}

bool Serializer::empty() const noexcept {
    return buffer.empty();
}

void Serializer::clear() noexcept {
    buffer.clear();
}

void Serializer::reserve(size_t capacity) {
    buffer.reserve(capacity);
}

void Serializer::shrink_to_fit() {
    buffer.shrink_to_fit();
}

// ========== Deserializer Implementation ==========

Deserializer::Deserializer(std::span<const std::byte> data) noexcept
    : buffer(data), position(0) {}

std::expected<uint8_t, SerializationError> Deserializer::readU8() noexcept {
    if (!hasRemaining(sizeof(uint8_t))) {
        return std::unexpected(SerializationError::INSUFFICIENT_DATA);
    }
    
    uint8_t value = static_cast<uint8_t>(buffer[position]);
    position += sizeof(uint8_t);
    return value;
}

std::expected<uint16_t, SerializationError> Deserializer::readU16() noexcept {
    if (!hasRemaining(sizeof(uint16_t))) {
        return std::unexpected(SerializationError::INSUFFICIENT_DATA);
    }
    
    uint16_t network_value;
    std::memcpy(&network_value, buffer.data() + position, sizeof(uint16_t));
    position += sizeof(uint16_t);
    
    return ntohs(network_value);
}

std::expected<uint32_t, SerializationError> Deserializer::readU32() noexcept {
    if (!hasRemaining(sizeof(uint32_t))) {
        return std::unexpected(SerializationError::INSUFFICIENT_DATA);
    }
    
    uint32_t network_value;
    std::memcpy(&network_value, buffer.data() + position, sizeof(uint32_t));
    position += sizeof(uint32_t);
    
    return ntohl(network_value);
}

std::expected<uint64_t, SerializationError> Deserializer::readU64() noexcept {
    if (!hasRemaining(sizeof(uint64_t))) {
        return std::unexpected(SerializationError::INSUFFICIENT_DATA);
    }
    
    // 读取高32位和低32位
    uint32_t high_network, low_network;
    std::memcpy(&high_network, buffer.data() + position, sizeof(uint32_t));
    std::memcpy(&low_network, buffer.data() + position + sizeof(uint32_t), sizeof(uint32_t));
    position += sizeof(uint64_t);
    
    uint32_t high = ntohl(high_network);
    uint32_t low = ntohl(low_network);
    
    return (static_cast<uint64_t>(high) << 32) | low;
}

std::expected<std::string, SerializationError> Deserializer::readString() {
    // 读取字符串长度
    auto length_result = readU32();
    if (!length_result.has_value()) {
        return std::unexpected(length_result.error());
    }
    
    uint32_t length = length_result.value();
    
    // 检查长度合理性（防止错误长度数据）
    constexpr uint32_t MAX_STRING_LENGTH = 1024 * 1024; // 1MB
    if (length > MAX_STRING_LENGTH) {
        return std::unexpected(SerializationError::STRING_TOO_LONG);
    }
    
    if (!hasRemaining(length)) {
        return std::unexpected(SerializationError::INSUFFICIENT_DATA);
    }
    
    // 读取字符串内容
    std::string result;
    result.resize(length);
    std::memcpy(result.data(), buffer.data() + position, length);
    position += length;
    
    return result;
}

std::expected<std::vector<std::byte>, SerializationError> Deserializer::readBytes(size_t count) {
    if (!hasRemaining(count)) {
        return std::unexpected(SerializationError::INSUFFICIENT_DATA);
    }
    
    std::vector<std::byte> result(count);
    std::memcpy(result.data(), buffer.data() + position, count);
    position += count;
    
    return result;
}

std::expected<std::span<const std::byte>, SerializationError> 
Deserializer::readBytesView(size_t count) noexcept {
    if (!hasRemaining(count)) {
        return std::unexpected(SerializationError::INSUFFICIENT_DATA);
    }
    
    std::span<const std::byte> result = buffer.subspan(position, count);
    position += count;
    
    return result;
}

std::expected<void, SerializationError> Deserializer::readRaw(void* dest, size_t size) noexcept {
    if (dest == nullptr || size == 0) {
        return std::unexpected(SerializationError::INVALID_FORMAT);
    }
    
    if (!hasRemaining(size)) {
        return std::unexpected(SerializationError::INSUFFICIENT_DATA);
    }
    
    std::memcpy(dest, buffer.data() + position, size);
    position += size;
    
    return {};
}

size_t Deserializer::remaining() const noexcept {
    return buffer.size() - position;
}

bool Deserializer::hasRemaining(size_t bytes) const noexcept {
    return remaining() >= bytes;
}

std::expected<void, SerializationError> Deserializer::skip(size_t bytes) noexcept {
    if (!hasRemaining(bytes)) {
        return std::unexpected(SerializationError::INSUFFICIENT_DATA);
    }
    
    position += bytes;
    return {};
}

std::expected<uint8_t, SerializationError> Deserializer::peekU8() const noexcept {
    if (!hasRemaining(sizeof(uint8_t))) {
        return std::unexpected(SerializationError::INSUFFICIENT_DATA);
    }
    
    return static_cast<uint8_t>(buffer[position]);
}

std::expected<uint32_t, SerializationError> Deserializer::peekU32() const noexcept {
    if (!hasRemaining(sizeof(uint32_t))) {
        return std::unexpected(SerializationError::INSUFFICIENT_DATA);
    }
    
    uint32_t network_value;
    std::memcpy(&network_value, buffer.data() + position, sizeof(uint32_t));
    
    return ntohl(network_value);
}

} // namespace NET