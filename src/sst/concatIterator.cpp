#include "concatIterator.h"

namespace dehua_lsm {


ConcatIterator::ConcatIterator(std::vector<std::shared_ptr<SST>> ssts,
                               uint64_t tranc_id)
    : ssts(ssts), cur_iter(nullptr, tranc_id), cur_idx(0),
      max_tranc_id_(tranc_id) {
  if (!this->ssts.empty()) {
    cur_iter = ssts[0]->begin(max_tranc_id_);
  }
}


BaseIterator &ConcatIterator::operator++() {
  ++cur_iter;

  if (cur_iter.is_end() || !cur_iter.is_valid()) {
    cur_idx++;
    if (cur_idx < ssts.size()) {
      cur_iter = ssts[cur_idx]->begin(max_tranc_id_);
    } else {
      cur_iter = SstIterator(nullptr, max_tranc_id_);
    }
  }
  return *this;
}


bool ConcatIterator::operator==(const BaseIterator &other) const {
  if (other.get_type() != IteratorType::ConcatIterator) {
    return false;
  }
  auto other2 = dynamic_cast<const ConcatIterator &>(other);
  return other2.cur_iter == cur_iter;
}


bool ConcatIterator::operator!=(const BaseIterator &other) const {
  return !(*this == other);
}


ConcatIterator::value_type ConcatIterator::operator*() const {
  return *cur_iter;
}


IteratorType ConcatIterator::get_type() const {
  return IteratorType::ConcatIterator;
}


uint64_t ConcatIterator::get_tranc_id() const { return max_tranc_id_; }


bool ConcatIterator::is_end() const {
  return cur_iter.is_end() || !cur_iter.is_valid();
}


bool ConcatIterator::is_valid() const {
  return !cur_iter.is_end() && cur_iter.is_valid();
}


ConcatIterator::pointer ConcatIterator::operator->() const {
  return cur_iter.operator->();
}


std::string ConcatIterator::key() { return cur_iter.key(); }

std::string ConcatIterator::value() { return cur_iter.value(); }

} // namespace dehua_lsm