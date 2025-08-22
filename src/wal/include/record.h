#ifndef RECORD_H
#define RECORD_H

#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace dehua_lsm {

enum class OperationType {
  CREATE,
  COMMIT,
  ROLLBACK,
  PUT,
  DELETE,
};

class Record {
private:
  // 构造函数
  Record() = default;

public:
  // 操作类型枚举

  static Record createRecord(uint64_t tranc_id);
  static Record commitRecord(uint64_t tranc_id);
  static Record rollbackRecord(uint64_t tranc_id);
  static Record putRecord(uint64_t tranc_id, const std::string &key,
                          const std::string &value);
  static Record deleteRecord(uint64_t tranc_id, const std::string &key);

  // 编码记录
  std::vector<uint8_t> encode() const;

  // 解码记录
  static std::vector<Record> decode(const std::vector<uint8_t> &data);

  // 获取记录的各个部分
  uint64_t getTrancId() const { return tranc_id_; }
  OperationType getOperationType() const { return operation_type_; }
  std::string getKey() const { return key_; }
  std::string getValue() const { return value_; }

  // 打印记录（用于调试）
  void print() const;

  bool operator==(const Record &other) const;
  bool operator!=(const Record &other) const;

private:
  uint64_t tranc_id_;
  OperationType operation_type_;
  std::string key_;
  std::string value_;
  uint16_t record_len_;
};
} // namespace dehua_lsm
#endif // RECORD_H