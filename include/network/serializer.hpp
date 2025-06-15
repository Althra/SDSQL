/**
* @brief 序列化器和反序列化器
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
#include <span>
#include <expected>

namespace NET {

enum class SerializationError {
    BUFFER_OVERFLOW,
    INSUFFICIENT_DATA,
    INVALID_FORMAT,
    STRING_TOO_LONG
};

class Serializer {
private:
    std::vector<std::byte> buffer;

public:
    Serializer();
    explicit Serializer(size_t initial_capacity);

    // 基础类型写入
    void writeU8(uint8_t value);
    void writeU16(uint16_t value);
    void writeU32(uint32_t value);
    void writeU64(uint64_t value);
    
    // 字符串写入（包含长度前缀）
    void writeString(const std::string& str);
    
    // 字节数组写入
    void writeBytes(const std::byte* data, size_t size);
    void writeBytes(std::span<const std::byte> data);
    
    // 原始数据写入
    void writeRaw(const void* data, size_t size);

    // 获取序列化结果
    std::span<const std::byte> getBuffer() const noexcept;
    const std::byte* data() const noexcept;
    size_t size() const noexcept;
    bool empty() const noexcept;

    // 缓冲区管理
    void clear() noexcept;
    void reserve(size_t capacity);
    void shrink_to_fit();
};

class Deserializer {
private:
    std::span<const std::byte> buffer;
    size_t position;

public:
    explicit Deserializer(std::span<const std::byte> data) noexcept;

    // 基础类型读取
    std::expected<uint8_t, SerializationError> readU8() noexcept;
    std::expected<uint16_t, SerializationError> readU16() noexcept;
    std::expected<uint32_t, SerializationError> readU32() noexcept;
    std::expected<uint64_t, SerializationError> readU64() noexcept;
    
    // 字符串读取（长度前缀格式）
    std::expected<std::string, SerializationError> readString();
    
    // 字节数组读取
    std::expected<std::vector<std::byte>, SerializationError> readBytes(size_t count);
    std::expected<std::span<const std::byte>, SerializationError> readBytesView(size_t count) noexcept;
    
    // 原始数据读取
    std::expected<void, SerializationError> readRaw(void* dest, size_t size) noexcept;

    // 位置管理
    size_t getPosition() const noexcept { return position; }
    size_t remaining() const noexcept;
    bool hasRemaining(size_t bytes) const noexcept;
    void reset() noexcept { position = 0; }
    
    // 跳过指定字节数
    std::expected<void, SerializationError> skip(size_t bytes) noexcept;
    
    // 查看但不消费数据
    std::expected<uint8_t, SerializationError> peekU8() const noexcept;
    std::expected<uint32_t, SerializationError> peekU32() const noexcept;
};

} // namespace NET