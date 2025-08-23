// include/wal/wal.h

#ifndef WAL_H
#define WAL_H

#include "fileObj.h"
#include "record.h"
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace dehua_lsm {

class WAL {
public:
  WAL(const std::string &log_dir, size_t buffer_size,
      uint64_t max_finished_tranc_id, uint64_t clean_interval,
      uint64_t file_size_limit);
  ~WAL();

  static std::map<uint64_t, std::vector<Record>>
  recover(const std::string &log_dir, uint64_t max_finished_tranc_id);

  // 将记录添加到缓冲区
  void log(const std::vector<Record> &records, bool force_flush = false);

  // 将缓冲区中的数据写入 WAL 文件
  void flush();

  void set_max_finished_tranc_id(uint64_t max_finished_tranc_id);

private:
  void cleaner();
  void cleanWALFile();
  void reset_file();

protected:
  std::string active_log_path_; // 当前活动的 WAL 文件路径
  FileObj log_file_;             // 当前活动的 WAL 文件对象
  size_t file_size_limit_;       // WAL 文件大小限制
  std::mutex mutex_;             // 保护 WAL 的互斥锁
  std::vector<Record> log_buffer_; // WAL 缓冲区
  size_t buffer_size_;           // WAL 缓冲区大小
  std::thread cleaner_thread_;   // 清理线程
  uint64_t max_finished_tranc_id_; // 最大已完成事务 ID
  std::atomic<bool> stop_cleaner_; // 停止清理线程标志
  uint64_t clean_interval_;
};
} // namespace dehua_lsm

#endif // WAL_H