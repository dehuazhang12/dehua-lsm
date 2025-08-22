#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

/***

-----------------------------------------------------------------------------
|             Data Section           |      Offset Section |     Extra      |
-----------------------------------------------------------------------------
|Entry#1|Entry#2|...|Entry#N|Offset#1|Offset#2|...|Offset#N|num_of_elements |
-----------------------------------------------------------------------------

---------------------------------------------------------------------
|                           Entry #1 |                          ... |
--------------------------------------------------------------|-----|
|key_len (2B)|key(keylen)|val_len(2B)|val(vallen)|tranc_id(8B)| ... |
---------------------------------------------------------------------

*/

namespace dehua_lsm {
class BlockIterator;

class Block : public std::enable_shared_from_this<Block> {
  friend BlockIterator;

private:
  std::vector<uint8_t> data;
  std::vector<uint16_t> offsets;
  size_t capacity;

  struct Entry {
    std::string key;
    std::string value;
    uint64_t tranc_id;
  };
  Entry get_entry_at(size_t offset) const;
  std::string get_key_at(size_t offset) const;
  std::string get_value_at(size_t offset) const;
  uint16_t get_tranc_id_at(size_t offset) const;
  int compare_key_at(size_t offset, const std::string &target) const;

  // 根据id的可见性调整位置
  int adjust_idx_by_tranc_id(size_t idx, uint64_t tranc_id);

  bool is_same_key(size_t idx, const std::string &target_key) const;

public:
  Block() = default;
  Block(size_t capacity);
  // ! 这里的编码函数不包括 hash
  std::vector<uint8_t> encode();
  // ! 这里的解码函数可指定切片是否包括 hash
  static std::shared_ptr<Block> decode(const std::vector<uint8_t> &encoded,
                                       bool with_hash = false);
  std::string get_first_key();
  size_t get_offset_at(size_t idx) const;
  bool add_entry(const std::string &key, const std::string &value,
                 uint64_t tranc_id, bool force_write);
  std::optional<std::string> get_value_binary(const std::string &key,
                                              uint64_t tranc_id);

  size_t size() const;
  size_t cur_size() const;
  bool is_empty() const;
  std::optional<size_t> get_idx_binary(const std::string &key,
                                       uint64_t tranc_id);

  // 按照谓词返回迭代器, 左闭右开
  std::optional<
      std::pair<std::shared_ptr<BlockIterator>, std::shared_ptr<BlockIterator>>>
  get_monotony_predicate_iters(uint64_t tranc_id,
                               std::function<int(const std::string &)> func);

  BlockIterator begin(uint64_t tranc_id = 0);

  std::optional<
      std::pair<std::shared_ptr<BlockIterator>, std::shared_ptr<BlockIterator>>>
  iters_preffix(uint64_t tranc_id, const std::string &preffix);

  BlockIterator end();
};
class BlockMeta {
  friend class BlockMetaTest;

public:
  size_t offset;         // 块在文件中的偏移量
  std::string first_key; // 块的第一个key
  std::string last_key;  // 块的最后一个key
  static void encode_meta_to_slice(std::vector<BlockMeta> &meta_entries,
                                   std::vector<uint8_t> &metadata);
  static std::vector<BlockMeta>
  decode_meta_from_slice(const std::vector<uint8_t> &metadata);
  BlockMeta();
  BlockMeta(size_t offset, const std::string &first_key,
            const std::string &last_key);
};



class BlockIterator {
public:
  // 标准迭代器类型定义
  using iterator_category = std::forward_iterator_tag;
  using value_type = std::pair<std::string, std::string>;
  using difference_type = std::ptrdiff_t;
  using pointer = const value_type *;
  using reference = const value_type &;

  // 构造函数
  BlockIterator(std::shared_ptr<Block> b, size_t index, uint64_t tranc_id);
  BlockIterator(std::shared_ptr<Block> b, const std::string &key,
                uint64_t tranc_id);
  // BlockIterator(std::shared_ptr<Block> b, uint64_t tranc_id);
  BlockIterator()
      : block(nullptr), current_index(0), tranc_id_(0) {} // end iterator

  // 迭代器操作
  pointer operator->() const;
  BlockIterator &operator++();
  BlockIterator operator++(int) = delete;
  bool operator==(const BlockIterator &other) const;
  bool operator!=(const BlockIterator &other) const;
  value_type operator*() const;
  bool is_end();

private:
  void update_current() const;
  // 跳过当前不可见事务的id (如果开启了事务功能)
  void skip_by_tranc_id();

private:
  std::shared_ptr<Block> block;                   // 指向所属的 Block
  size_t current_index;                           // 当前位置的索引
  uint64_t tranc_id_;                             // 当前事务 id
  mutable std::optional<value_type> cached_value; // 缓存当前值
};
} // namespace dehua_lsm
