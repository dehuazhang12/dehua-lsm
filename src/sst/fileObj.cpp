#include "include/fileObj.h"
#include <cstring>
#include <stdexcept>
#include <iostream>
#include <cstdint>

namespace dehua_lsm {

FileObj::FileObj() : m_size(0) {}

FileObj::~FileObj() {
    if (file_.is_open()) {
        file_.close();
    }
}

FileObj::FileObj(FileObj &&other) noexcept 
    : m_size(other.m_size), file_(std::move(other.file_)), filename_(std::move(other.filename_)) {
    other.m_size = 0;
}

FileObj &FileObj::operator=(FileObj &&other) noexcept {
    if (this != &other) {
        if (file_.is_open()) {
            file_.close();
        }
        m_size = other.m_size;
        file_ = std::move(other.file_);
        filename_ = std::move(other.filename_);
        other.m_size = 0;
    }
    return *this;
}

size_t FileObj::size() const {
    return m_size;
}

void FileObj::set_size(size_t size) {
    m_size = size;
}

void FileObj::del_file() {
    if (file_.is_open()) {
        file_.close();
    }
    if (std::filesystem::exists(filename_)) {
        std::filesystem::remove(filename_);
    }
}

FileObj FileObj::create_and_write(const std::string &path, std::vector<uint8_t> buf) {
    FileObj obj;
    obj.filename_ = path;
    
    // 创建并打开文件用于写入
    obj.file_.open(path, std::ios::out | std::ios::binary | std::ios::trunc);
    if (!obj.file_.is_open()) {
        throw std::runtime_error("Failed to create file: " + path);
    }
    
    // 写入数据
    if (!buf.empty()) {
        obj.file_.write(reinterpret_cast<const char*>(buf.data()), buf.size());
        if (!obj.file_.good()) {
            throw std::runtime_error("Failed to write data to file: " + path);
        }
    }
    
    obj.file_.close();
    obj.m_size = buf.size();
    
    // 重新以读写模式打开
    obj.file_.open(path, std::ios::in | std::ios::out | std::ios::binary);
    if (!obj.file_.is_open()) {
        throw std::runtime_error("Failed to reopen file: " + path);
    }
    
    return obj;
}

FileObj FileObj::open(const std::string &path, bool create) {
    FileObj obj;
    obj.filename_ = path;
    
    if (create) {
        // 创建新文件或覆盖现有文件
        obj.file_.open(path, std::ios::in | std::ios::out | std::ios::binary | std::ios::trunc);
    } else {
        // 打开现有文件
        obj.file_.open(path, std::ios::in | std::ios::out | std::ios::binary);
    }
    
    if (!obj.file_.is_open()) {
        throw std::runtime_error("Failed to open file: " + path);
    }
    
    // 获取文件大小
    obj.file_.seekg(0, std::ios::end);
    obj.m_size = obj.file_.tellg();
    obj.file_.seekg(0, std::ios::beg);
    
    return obj;
}

std::vector<uint8_t> FileObj::read_to_slice(size_t offset, size_t length) {
    if (!file_.is_open()) {
        throw std::runtime_error("File is not open");
    }
    
    std::vector<uint8_t> buffer(length);
    file_.seekg(offset, std::ios::beg);
    file_.read(reinterpret_cast<char*>(buffer.data()), length);
    
    if (!file_.good() && !file_.eof()) {
        throw std::runtime_error("Failed to read from file");
    }
    
    // 如果读取的字节数少于请求的长度，调整buffer大小
    size_t bytes_read = file_.gcount();
    if (bytes_read < length) {
        buffer.resize(bytes_read);
    }
    
    return buffer;
}

uint8_t FileObj::read_uint8(size_t offset) {
    auto data = read_to_slice(offset, sizeof(uint8_t));
    if (data.size() < sizeof(uint8_t)) {
        throw std::runtime_error("Not enough data to read uint8_t");
    }
    return data[0];
}

uint16_t FileObj::read_uint16(size_t offset) {
    auto data = read_to_slice(offset, sizeof(uint16_t));
    if (data.size() < sizeof(uint16_t)) {
        throw std::runtime_error("Not enough data to read uint16_t");
    }
    uint16_t result;
    std::memcpy(&result, data.data(), sizeof(uint16_t));
    return result;
}

uint32_t FileObj::read_uint32(size_t offset) {
    auto data = read_to_slice(offset, sizeof(uint32_t));
    if (data.size() < sizeof(uint32_t)) {
        throw std::runtime_error("Not enough data to read uint32_t");
    }
    uint32_t result;
    std::memcpy(&result, data.data(), sizeof(uint32_t));
    return result;
}

uint64_t FileObj::read_uint64(size_t offset) {
    auto data = read_to_slice(offset, sizeof(uint64_t));
    if (data.size() < sizeof(uint64_t)) {
        throw std::runtime_error("Not enough data to read uint64_t");
    }
    uint64_t result;
    std::memcpy(&result, data.data(), sizeof(uint64_t));
    return result;
}

bool FileObj::write(size_t offset, std::vector<uint8_t> &buf) {
    if (!file_.is_open()) {
        return false;
    }
    
    file_.seekp(offset, std::ios::beg);
    file_.write(reinterpret_cast<const char*>(buf.data()), buf.size());
    
    if (!file_.good()) {
        return false;
    }
    
    // 更新文件大小（如果写入位置超过了当前大小）
    size_t new_end = offset + buf.size();
    if (new_end > m_size) {
        m_size = new_end;
    }
    
    return true;
}

bool FileObj::append(std::vector<uint8_t> &buf) {
    if (!file_.is_open()) {
        return false;
    }
    
    file_.seekp(0, std::ios::end);
    file_.write(reinterpret_cast<const char*>(buf.data()), buf.size());
    
    if (!file_.good()) {
        return false;
    }
    
    m_size += buf.size();
    return true;
}

bool FileObj::sync() {
    if (!file_.is_open()) {
        return false;
    }
    
    file_.flush();
    return file_.good();
}

} // namespace dehua_lsm
