#include "block.h"
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>

namespace dehua_lsm {
Block::Block(size_t capacity) : capacity(capacity) {}

std::vector<uint8_t> Block::encode() {
  // 计算总大小：数据段 + 偏移数组(每个偏移2字节) + 元素个数(2字节)
  size_t total_bytes = data.size() * sizeof(uint8_t) +
                       offsets.size() * sizeof(uint16_t) + sizeof(uint16_t);
  std::vector<uint8_t> encoded(total_bytes, 0);

  // 1. 复制数据段
  memcpy(encoded.data(), data.data(), data.size() * sizeof(uint8_t));

  // 2. 复制偏移数组
  size_t offset_pos = data.size() * sizeof(uint8_t);
  memcpy(encoded.data() + offset_pos,
         offsets.data(),                   // vector 的连续内存起始位置
         offsets.size() * sizeof(uint16_t) // 总字节数
  );

  // 3. 写入元素个数
  size_t num_pos =
      data.size() * sizeof(uint8_t) + offsets.size() * sizeof(uint16_t);
  uint16_t num_elements = offsets.size();
  memcpy(encoded.data() + num_pos, &num_elements, sizeof(uint16_t));

  return encoded;
}

std::shared_ptr<Block> Block::decode(const std::vector<uint8_t> &encoded,
                                     bool with_hash) {
  // 使用 make_shared 创建对象
  auto block = std::make_shared<Block>();

  // 1. 安全性检查
  if (encoded.size() < sizeof(uint16_t)) {
    throw std::runtime_error("Encoded data too small");
  }

  // 2. 读取元素个数
  uint16_t num_elements;
  size_t num_elements_pos = encoded.size() - sizeof(uint16_t);
  if (with_hash) {
    num_elements_pos -= sizeof(uint32_t);
    auto hash_pos = encoded.size() - sizeof(uint32_t);
    uint32_t hash_value;
    memcpy(&hash_value, encoded.data() + hash_pos, sizeof(uint32_t));

    uint32_t compute_hash = std::hash<std::string_view>{}(
        std::string_view(reinterpret_cast<const char *>(encoded.data()),
                         encoded.size() - sizeof(uint32_t)));
    if (hash_value != compute_hash) {
      throw std::runtime_error("Block hash verification failed");
    }
  }
  memcpy(&num_elements, encoded.data() + num_elements_pos, sizeof(uint16_t));

  // 3. 验证数据大小
  size_t required_size = sizeof(uint16_t) + num_elements * sizeof(uint16_t);
  if (encoded.size() < required_size) {
    throw std::runtime_error("Invalid encoded data size");
  }

  // 4. 计算各段位置
  size_t offsets_section_start =
      num_elements_pos - num_elements * sizeof(uint16_t);

  // 5. 读取偏移数组
  block->offsets.resize(num_elements);
  memcpy(block->offsets.data(), encoded.data() + offsets_section_start,
         num_elements * sizeof(uint16_t));

  // 6. 复制数据段
  block->data.reserve(offsets_section_start); // 优化内存分配
  block->data.assign(encoded.begin(), encoded.begin() + offsets_section_start);

  return block;
}

std::string Block::get_first_key() {
  if (data.empty() || offsets.empty()) {
    return "";
  }

  // 读取第一个key的长度（前2字节）
  uint16_t key_len;
  memcpy(&key_len, data.data(), sizeof(uint16_t));

  // 读取key
  std::string key(reinterpret_cast<char *>(data.data() + sizeof(uint16_t)),
                  key_len);
  return key;
}

size_t Block::get_offset_at(size_t idx) const {
  if (idx > offsets.size()) {
    throw std::runtime_error("idx out of offsets range");
  }
  return offsets[idx];
}

bool Block::add_entry(const std::string &key, const std::string &value,
                      uint64_t tranc_id, bool force_write) {
  if (!force_write &&
      (cur_size() + key.size() + value.size() + 3 * sizeof(uint16_t) +
           sizeof(uint64_t) >
       capacity) &&
      !offsets.empty()) {
    return false;
  }
  // 计算entry大小：key长度(2B) + key + value长度(2B) + value
  size_t entry_size = sizeof(uint16_t) + key.size() + sizeof(uint16_t) +
                      value.size() + sizeof(uint64_t);
  size_t old_size = data.size();
  data.resize(old_size + entry_size);

  // 写入key长度
  uint16_t key_len = key.size();
  memcpy(data.data() + old_size, &key_len, sizeof(uint16_t));

  // 写入key
  memcpy(data.data() + old_size + sizeof(uint16_t), key.data(), key_len);

  // 写入value长度
  uint16_t value_len = value.size();
  memcpy(data.data() + old_size + sizeof(uint16_t) + key_len, &value_len,
         sizeof(uint16_t));

  // 写入value
  memcpy(data.data() + old_size + sizeof(uint16_t) + key_len + sizeof(uint16_t),
         value.data(), value_len);

  // 写入事务id
  memcpy(data.data() + old_size + sizeof(uint16_t) + key_len +
             sizeof(uint16_t) + value_len,
         &tranc_id, sizeof(uint64_t));

  // 记录偏移
  offsets.push_back(old_size);
  return true;
}

// 从指定偏移量获取entry的key
std::string Block::get_key_at(size_t offset) const {
  uint16_t key_len;
  memcpy(&key_len, data.data() + offset, sizeof(uint16_t));
  return std::string(
      reinterpret_cast<const char *>(data.data() + offset + sizeof(uint16_t)),
      key_len);
}

// 从指定偏移量获取entry的value
std::string Block::get_value_at(size_t offset) const {
  // 先获取key长度
  uint16_t key_len;
  memcpy(&key_len, data.data() + offset, sizeof(uint16_t));

  // 计算value长度的位置
  size_t value_len_pos = offset + sizeof(uint16_t) + key_len;
  uint16_t value_len;
  memcpy(&value_len, data.data() + value_len_pos, sizeof(uint16_t));

  // 返回value
  return std::string(reinterpret_cast<const char *>(
                         data.data() + value_len_pos + sizeof(uint16_t)),
                     value_len);
}

uint16_t Block::get_tranc_id_at(size_t offset) const {
  // 先获取key长度
  uint16_t key_len;
  memcpy(&key_len, data.data() + offset, sizeof(uint16_t));

  // 计算value长度的位置
  size_t value_len_pos = offset + sizeof(uint16_t) + key_len;
  uint16_t value_len;
  memcpy(&value_len, data.data() + value_len_pos, sizeof(uint16_t));

  // 计算事务id的位置
  size_t tranc_id_pos = value_len_pos + sizeof(uint16_t) + value_len;
  uint64_t tranc_id;
  memcpy(&tranc_id, data.data() + tranc_id_pos, sizeof(uint64_t));
  return tranc_id;
}

// 比较指定偏移量处的key与目标key
int Block::compare_key_at(size_t offset, const std::string &target) const {
  std::string key = get_key_at(offset);
  return key.compare(target);
}

// 相同的key连续分布, 且相同的key的事务id从大到小排布
// 这里的逻辑是找到最接近 tranc_id 的键值对的索引位置
int Block::adjust_idx_by_tranc_id(size_t idx, uint64_t tranc_id) {
  if (idx >= offsets.size()) {
    return -1; // 索引超出范围
  }

  auto target_key = get_key_at(offsets[idx]);

  if (tranc_id != 0) {
    auto cur_tranc_id = get_tranc_id_at(offsets[idx]);

    if (cur_tranc_id <= tranc_id) {
      // 当前记录可见，向前查找更接近的目标
      size_t prev_idx = idx;
      while (prev_idx > 0 && is_same_key(prev_idx - 1, target_key)) {
        prev_idx--;
        auto new_tranc_id = get_tranc_id_at(offsets[prev_idx]);
        if (new_tranc_id > tranc_id) {
          return prev_idx + 1; // 更新的记录不可见
        }
      }
      return prev_idx;
    } else {
      // 当前记录不可见，向后查找
      size_t next_idx = idx + 1;
      while (next_idx < offsets.size() && is_same_key(next_idx, target_key)) {
        auto new_tranc_id = get_tranc_id_at(offsets[next_idx]);
        if (new_tranc_id <= tranc_id) {
          return next_idx; // 找到可见记录
        }
        next_idx++;
      }
      return -1; // 没有找到满足条件的记录
    }
  } else {
    // 没有开启事务的话, 直接选择最大的事务id的记录返回
    size_t prev_idx = idx;
    while (prev_idx > 0 && is_same_key(prev_idx - 1, target_key)) {
      prev_idx--;
    }
    return prev_idx;
  }
}

bool Block::is_same_key(size_t idx, const std::string &target_key) const {
  if (idx >= offsets.size()) {
    return false; // 索引超出范围
  }
  return get_key_at(offsets[idx]) == target_key;
}

// 使用二分查找获取value
// 要求在插入数据时有序插入
std::optional<std::string> Block::get_value_binary(const std::string &key,
                                                   uint64_t tranc_id) {
  auto idx = get_idx_binary(key, tranc_id);
  if (!idx.has_value()) {
    return std::nullopt;
  }

  return get_value_at(offsets[*idx]);
}

std::optional<size_t> Block::get_idx_binary(const std::string &key,
                                            uint64_t tranc_id) {
  if (offsets.empty()) {
    return std::nullopt;
  }
  // 二分查找
  int left = 0;
  int right = offsets.size() - 1;

  while (left <= right) {
    int mid = left + (right - left) / 2;
    size_t mid_offset = offsets[mid];

    int cmp = compare_key_at(mid_offset, key);

    if (cmp == 0) {
      // 找到key，返回对应的value
      // 还需要判断事务id可见性
      auto new_mid = adjust_idx_by_tranc_id(mid, tranc_id);
      if (new_mid == -1) {
        return std::nullopt;
      }
      return new_mid;
    } else if (cmp < 0) {
      // 中间的key小于目标key，查找右半部分
      left = mid + 1;
    } else {
      // 中间的key大于目标key，查找左半部分
      right = mid - 1;
    }
  }

  return std::nullopt;
}

// 返回第一个满足谓词的位置和最后一个满足谓词的位置
// 如果不存在, 范围nullptr
// 谓词作用于key, 且保证满足谓词的结果只在一段连续的区间内, 例如前缀匹配的谓词
// 返回的区间是闭区间, 开区间需要手动对返回值自增
// predicate返回值:
//   0: 满足谓词
//   >0: 不满足谓词, 需要向右移动
//   <0: 不满足谓词, 需要向左移动
std::optional<
    std::pair<std::shared_ptr<BlockIterator>, std::shared_ptr<BlockIterator>>>
Block::get_monotony_predicate_iters(
    uint64_t tranc_id, std::function<int(const std::string &)> predicate) {
  if (offsets.empty()) {
    return std::nullopt;
  }

  // 第一次二分查找，找到第一个满足谓词的位置
  int left = 0;
  int right = offsets.size() - 1;
  int first = -1;

  while (left <= right) {
    int mid = left + (right - left) / 2;
    size_t mid_offset = offsets[mid];
    auto mid_key = get_key_at(mid_offset);
    int direction = predicate(mid_key);
    if (direction <= 0) { // 目标在 mid 左侧
      right = mid - 1;
    } else // 目标在mid右侧
      left = mid + 1;
  }

  if (left == -1) {
    return std::nullopt; // 没有找到满足谓词的元素
  }

  first = left; // 保留下找到的第一个的位置

  // 第二次二分查找，找到最后一个满足谓词的位置
  int last = -1;
  right = offsets.size() - 1;
  while (left <= right) {
    int mid = left + (right - left) / 2;
    size_t mid_offset = offsets[mid];
    auto mid_key = get_key_at(mid_offset);
    int direction = predicate(mid_key);
    if (direction < 0) {
      right = mid - 1;
    } else
      left = mid + 1;
  }
  last = left - 1;
  // 最后进行组合
  auto it_begin =
      std::make_shared<BlockIterator>(shared_from_this(), first, tranc_id);
  auto it_end =
      std::make_shared<BlockIterator>(shared_from_this(), last + 1, tranc_id);

  return std::make_optional<std::pair<std::shared_ptr<BlockIterator>,
                                      std::shared_ptr<BlockIterator>>>(it_begin,
                                                                       it_end);
}

Block::Entry Block::get_entry_at(size_t offset) const {
  Entry entry;
  entry.key = get_key_at(offset);
  entry.value = get_value_at(offset);
  entry.tranc_id = get_tranc_id_at(offset);
  return entry;
}

size_t Block::size() const { return offsets.size(); }

size_t Block::cur_size() const {
  return data.size() + offsets.size() * sizeof(uint16_t) + sizeof(uint16_t);
}

bool Block::is_empty() const { return offsets.empty(); }

BlockIterator Block::begin(uint64_t tranc_id) {
  return BlockIterator(shared_from_this(), 0, tranc_id);
}

std::optional<
    std::pair<std::shared_ptr<BlockIterator>, std::shared_ptr<BlockIterator>>>
Block::iters_preffix(uint64_t tranc_id, const std::string &preffix) {
  auto func = [&preffix](const std::string &key) {
    return -key.compare(0, preffix.size(), preffix);
  };
  return get_monotony_predicate_iters(tranc_id, func);
}

BlockIterator Block::end() {
  return BlockIterator(shared_from_this(), offsets.size(), 0);
}

BlockMeta::BlockMeta() : offset(0), first_key(""), last_key("") {}

BlockMeta::BlockMeta(size_t offset, const std::string &first_key,
                     const std::string &last_key)
    : offset(offset), first_key(first_key), last_key(last_key) {}

void BlockMeta::encode_meta_to_slice(std::vector<BlockMeta> &meta_entries,
                                     std::vector<uint8_t> &metadata) {
  // 1. 计算总大小：num_entries(32) + 所有entries的大小 + hash(32)
  uint32_t num_entries = meta_entries.size();
  size_t total_size = sizeof(uint32_t); // num_entries

  // 计算所有entries的大小
  for (const auto &meta : meta_entries) {
    total_size += sizeof(uint32_t) +      // offset
                  sizeof(uint16_t) +      // first_key_len
                  meta.first_key.size() + // first_key
                  sizeof(uint16_t) +      // last_key_len
                  meta.last_key.size();   // last_key
  }
  total_size += sizeof(uint32_t); // hash

  // 2. 分配空间
  metadata.resize(total_size);
  uint8_t *ptr = metadata.data();

  // 3. 写入元素个数
  memcpy(ptr, &num_entries, sizeof(uint32_t));
  ptr += sizeof(uint32_t);

  // 4. 写入每个entry
  for (const auto &meta : meta_entries) {
    // 写入 offset
    uint32_t offset32 = static_cast<uint32_t>(meta.offset);
    memcpy(ptr, &offset32, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    // 写入 first_key_len 和 first_key
    uint16_t first_key_len = meta.first_key.size();
    memcpy(ptr, &first_key_len, sizeof(uint16_t));
    ptr += sizeof(uint16_t);
    memcpy(ptr, meta.first_key.data(), first_key_len);
    ptr += first_key_len;

    // 写入 last_key_len 和 last_key
    uint16_t last_key_len = meta.last_key.size();
    memcpy(ptr, &last_key_len, sizeof(uint16_t));
    ptr += sizeof(uint16_t);
    memcpy(ptr, meta.last_key.data(), last_key_len);
    ptr += last_key_len;
  }

  // 5. 计算并写入hash
  const uint8_t *data_start = metadata.data() + sizeof(uint32_t);
  const uint8_t *data_end = ptr;
  size_t data_len = data_end - data_start;

  // 使用 std::hash 计算哈希值
  uint32_t hash = std::hash<std::string_view>{}(
      std::string_view(reinterpret_cast<const char *>(data_start), data_len));

  memcpy(ptr, &hash, sizeof(uint32_t));
}

std::vector<BlockMeta>
BlockMeta::decode_meta_from_slice(const std::vector<uint8_t> &metadata) {
  std::vector<BlockMeta> meta_entries;

  // 1. 验证最小长度
  if (metadata.size() < sizeof(uint32_t) * 2) { // 至少要有num_entries和hash
    throw std::runtime_error("Invalid metadata size");
  }

  // 2. 读取元素个数
  uint32_t num_entries;
  const uint8_t *ptr = metadata.data();
  memcpy(&num_entries, ptr, sizeof(uint32_t));
  ptr += sizeof(uint32_t);

  // 3. 读取entries
  for (uint32_t i = 0; i < num_entries; ++i) {
    BlockMeta meta;

    // 读取 offset
    uint32_t offset32;
    memcpy(&offset32, ptr, sizeof(uint32_t));
    meta.offset = offset32;
    ptr += sizeof(uint32_t);

    // 读取 first_key
    uint16_t first_key_len;
    memcpy(&first_key_len, ptr, sizeof(uint16_t));
    ptr += sizeof(uint16_t);
    meta.first_key.assign(reinterpret_cast<const char *>(ptr), first_key_len);
    ptr += first_key_len;

    // 读取 last_key
    uint16_t last_key_len;
    memcpy(&last_key_len, ptr, sizeof(uint16_t));
    ptr += sizeof(uint16_t);
    meta.last_key.assign(reinterpret_cast<const char *>(ptr), last_key_len);
    ptr += last_key_len;

    meta_entries.push_back(meta);
  }

  // 4. 验证hash
  uint32_t stored_hash;
  memcpy(&stored_hash, ptr, sizeof(uint32_t));

  const uint8_t *data_start = metadata.data() + sizeof(uint32_t);
  const uint8_t *data_end = ptr;
  size_t data_len = data_end - data_start;

  // 使用与编码时相同的 std::hash 计算哈希值
  uint32_t computed_hash = std::hash<std::string_view>{}(
      std::string_view(reinterpret_cast<const char *>(data_start), data_len));

  if (stored_hash != computed_hash) {
    throw std::runtime_error("Metadata hash mismatch");
  }

  return meta_entries;
}

BlockIterator::BlockIterator(std::shared_ptr<Block> b, size_t index,
                             uint64_t tranc_id)
    : block(b), current_index(index), tranc_id_(tranc_id),
      cached_value(std::nullopt) {
  skip_by_tranc_id();
}

BlockIterator::BlockIterator(std::shared_ptr<Block> b, const std::string &key,
                             uint64_t tranc_id)
    : block(b), tranc_id_(tranc_id), cached_value(std::nullopt) {
  auto key_idx_ops = block->get_idx_binary(key, tranc_id);
  if (key_idx_ops.has_value()) {
    current_index = key_idx_ops.value();
  } else {
    current_index = block->offsets.size();
  }
}

// BlockIterator::BlockIterator(std::shared_ptr<Block> b, uint64_t tranc_id)
//     : block(b), current_index(0), tranc_id_(tranc_id),
//       cached_value(std::nullopt) {
//   skip_by_tranc_id();
// }

BlockIterator::pointer BlockIterator::operator->() const {
  update_current();
  return &(*cached_value);
}

BlockIterator &BlockIterator::operator++() {
  if (block && current_index < block->size()) {
    auto prev_idx = current_index;
    auto prev_offset = block->get_offset_at(prev_idx);
    auto prev_entry = block->get_entry_at(prev_offset);

    ++current_index;

    // 跳过相同的key
    while (block && current_index < block->size()) {
      auto cur_offset = block->get_offset_at(current_index);
      auto cur_entry = block->get_entry_at(cur_offset);
      if (cur_entry.key != prev_entry.key) {
        break;
      }
      // 可能会连续出现多个key, 但由不同事务创建, 同样的key直接跳过
      ++current_index;
    }

    // 出现不同的key时, 还需要跳过不可见事务的键值对
    skip_by_tranc_id();
  }
  return *this;
}

bool BlockIterator::operator==(const BlockIterator &other) const {
  if (block == nullptr && other.block == nullptr) {
    return true;
  }
  if (block == nullptr || other.block == nullptr) {
    return false;
  }
  auto cmp = block == other.block && current_index == other.current_index;
  return cmp;
}

bool BlockIterator::operator!=(const BlockIterator &other) const {
  return !(*this == other);
}

BlockIterator::value_type BlockIterator::operator*() const {
  if (!block || current_index >= block->size()) {
    throw std::out_of_range("Iterator out of range");
  }

  // 使用缓存避免重复解析
  if (!cached_value.has_value()) {
    size_t offset = block->get_offset_at(current_index);
    cached_value =
        std::make_pair(block->get_key_at(offset), block->get_value_at(offset));
  }
  return *cached_value;
}

bool BlockIterator::is_end() { return current_index == block->offsets.size(); }

void BlockIterator::update_current() const {
  if (!cached_value && current_index < block->offsets.size()) {
    size_t offset = block->get_offset_at(current_index);
    cached_value =
        std::make_pair(block->get_key_at(offset), block->get_value_at(offset));
  }
}

void BlockIterator::skip_by_tranc_id() {
  if (tranc_id_ == 0) {
    // 没有开启事务功能
    cached_value = std::nullopt;
    return;
  }

  while (current_index < block->offsets.size()) {
    size_t offset = block->get_offset_at(current_index);
    auto tranc_id = block->get_tranc_id_at(offset);
    if (tranc_id <= tranc_id_) {
      // 位置合法
      break;
    }
    // 否则跳过不可见事务的键值对
    ++current_index;
  }
  cached_value = std::nullopt;
}
} // namespace dehua_lsm