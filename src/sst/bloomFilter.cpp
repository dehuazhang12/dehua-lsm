#include "include/bloomFilter.h"
#include <algorithm>
#include <cstring>
#include <functional>
#include <string>
#include <vector>

namespace dehua_lsm {

BloomFilter::BloomFilter() : expected_elements_(0), false_positive_rate_(0.0), num_bits_(0), num_hashes_(0) {}

BloomFilter::BloomFilter(size_t expected_elements, double false_positive_rate)
    : expected_elements_(expected_elements), false_positive_rate_(false_positive_rate) {
  // 计算最优的位数组大小
  num_bits_ = static_cast<size_t>(-expected_elements * std::log(false_positive_rate) / (std::log(2) * std::log(2)));
  
  // 计算最优的哈希函数数量
  num_hashes_ = static_cast<size_t>(num_bits_ * std::log(2) / expected_elements);
  if (num_hashes_ == 0) num_hashes_ = 1;
  
  // 初始化位数组，每个字节包含8位
  bits_.resize((num_bits_ + 7) / 8, 0);
}

BloomFilter::BloomFilter(size_t expected_elements, double false_positive_rate, size_t num_bits)
    : expected_elements_(expected_elements), false_positive_rate_(false_positive_rate), num_bits_(num_bits) {
  // 计算最优的哈希函数数量
  num_hashes_ = static_cast<size_t>(num_bits_ * std::log(2) / expected_elements);
  if (num_hashes_ == 0) num_hashes_ = 1;
  
  // 初始化位数组，每个字节包含8位
  bits_.resize((num_bits_ + 7) / 8, 0);
}

void BloomFilter::add(const std::string &key) {
  for (size_t i = 0; i < num_hashes_; ++i) {
    size_t bit_pos = hash(key, i) % num_bits_;
    // 设置对应的位为1
    size_t byte_index = bit_pos / 8;
    size_t bit_offset = bit_pos % 8;
    bits_[byte_index] |= (1 << bit_offset);
  }
}

bool BloomFilter::possibly_contains(const std::string &key) const {
  for (size_t i = 0; i < num_hashes_; ++i) {
    size_t bit_pos = hash(key, i) % num_bits_;
    // 检查对应的位是否为1
    size_t byte_index = bit_pos / 8;
    size_t bit_offset = bit_pos % 8;
    if ((bits_[byte_index] & (1 << bit_offset)) == 0) {
      return false; // 如果任何一位为0，则key肯定不存在
    }
  }
  return true; // 所有位都为1，key可能存在
}

void BloomFilter::clear() {
  std::fill(bits_.begin(), bits_.end(), 0);
}

std::vector<uint8_t> BloomFilter::encode() {
  std::vector<uint8_t> result;
  
  // 编码格式：expected_elements(8字节) + false_positive_rate(8字节) + num_bits(8字节) + num_hashes(8字节) + bits数据
  result.resize(4 * sizeof(uint64_t) + bits_.size());
  
  size_t offset = 0;
  
  // 写入expected_elements
  uint64_t temp = expected_elements_;
  std::memcpy(result.data() + offset, &temp, sizeof(uint64_t));
  offset += sizeof(uint64_t);
  
  // 写入false_positive_rate（转换为uint64_t存储）
  uint64_t rate_bits;
  std::memcpy(&rate_bits, &false_positive_rate_, sizeof(double));
  std::memcpy(result.data() + offset, &rate_bits, sizeof(uint64_t));
  offset += sizeof(uint64_t);
  
  // 写入num_bits
  temp = num_bits_;
  std::memcpy(result.data() + offset, &temp, sizeof(uint64_t));
  offset += sizeof(uint64_t);
  
  // 写入num_hashes
  temp = num_hashes_;
  std::memcpy(result.data() + offset, &temp, sizeof(uint64_t));
  offset += sizeof(uint64_t);
  
  // 写入位数组数据
  std::memcpy(result.data() + offset, bits_.data(), bits_.size());
  
  return result;
}

BloomFilter BloomFilter::decode(const std::vector<uint8_t> &data) {
  if (data.size() < 4 * sizeof(uint64_t)) {
    return BloomFilter(); // 返回空的布隆过滤器
  }
  
  size_t offset = 0;
  
  // 读取expected_elements
  uint64_t expected_elements;
  std::memcpy(&expected_elements, data.data() + offset, sizeof(uint64_t));
  offset += sizeof(uint64_t);
  
  // 读取false_positive_rate
  uint64_t rate_bits;
  std::memcpy(&rate_bits, data.data() + offset, sizeof(uint64_t));
  double false_positive_rate;
  std::memcpy(&false_positive_rate, &rate_bits, sizeof(double));
  offset += sizeof(uint64_t);
  
  // 读取num_bits
  uint64_t num_bits;
  std::memcpy(&num_bits, data.data() + offset, sizeof(uint64_t));
  offset += sizeof(uint64_t);
  
  // 读取num_hashes
  uint64_t num_hashes;
  std::memcpy(&num_hashes, data.data() + offset, sizeof(uint64_t));
  offset += sizeof(uint64_t);
  
  // 创建布隆过滤器实例
  BloomFilter filter(expected_elements, false_positive_rate, num_bits);
  filter.num_hashes_ = num_hashes;
  
  // 读取位数组数据
  size_t bits_size = data.size() - offset;
  filter.bits_.resize(bits_size);
  std::memcpy(filter.bits_.data(), data.data() + offset, bits_size);
  
  return filter;
}

size_t BloomFilter::hash1(const std::string &key) const {
  std::hash<std::string> hasher;
  return hasher(key);
}

size_t BloomFilter::hash2(const std::string &key) const {
  size_t hash = 0;
  for (char c : key) {
    hash = hash * 31 + c;
  }
  return hash;
}

size_t BloomFilter::hash(const std::string &key, size_t idx) const {
  // 使用双哈希技术: h(k,i) = h1(k) + i * h2(k)
  return hash1(key) + idx * hash2(key);
}

} // namespace dehua_lsm