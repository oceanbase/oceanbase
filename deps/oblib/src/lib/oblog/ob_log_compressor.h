/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_LOG_COMPRESSOR_H_
#define OB_LOG_COMPRESSOR_H_

#include "lib/container/ob_heap.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/thread/thread_mgr_interface.h"

namespace oceanbase {
namespace common {

class ObCompressor;

/* Log files are divided into blocks and then compressed. */
static const int32_t OB_SYSLOG_COMPRESS_BLOCK_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;
/* To prevent extreme cases where the files become larger after compression,
 * the size of the decompression buffer needs to be larger than the original data.
 * This size can refer to the ZSTD code implementation.
 */
static const int32_t OB_SYSLOG_COMPRESS_BUFFER_SIZE =
    OB_SYSLOG_COMPRESS_BLOCK_SIZE + OB_SYSLOG_COMPRESS_BLOCK_SIZE / 128 + 512 + 19;
static const int32_t OB_MAX_SYSLOG_FILE_NAME_SIZE = ObPLogFileStruct::MAX_LOG_FILE_NAME_SIZE;
static const int32_t OB_SYSLOG_COMPRESS_TYPE_COUNT = FD_TRACE_FILE + 1;
/* by default, syslog file size = 256MB, and most of the logs are observer.log
 * compressed file size ≈ 13MB, 13MB * 20 = 260MB, so OB_SYSLOG_DELETE_ARRAY_SIZE = 20 + 1
 * an extra position is reserved for newly inserted file name.
 */
static const int32_t OB_SYSLOG_DELETE_ARRAY_SIZE = 21;
static const int64_t OB_MIN_SYSLOG_DISK_SIZE = 2 * (1LL << 30);  // 2GB
static const int64_t OB_SYSLOG_COMPRESS_RESERVE_SIZE = 4 * (1LL << 30);  // 4GB
static const int64_t OB_SYSLOG_DELETE_RESERVE_SIZE = 2 * (1LL << 30);  // 2GB
static const int64_t OB_SYSLOG_COMPRESS_LOOP_INTERVAL = 5000000;  // 5s
static const char    OB_SYSLOG_DIR[] = "log";  // same as LOG_DIR in src/observer/main.cpp
static const char    OB_ALERT_LOG_DIR[] = "log/alert";  // same as ALERT_DIR in src/observer/main.cpp
static const char    OB_SYSLOG_COMPRESS_ZSTD_SUFFIX[] = ".zst";  // name suffix of file compressed by zstd
static const char    OB_UNCOMPRESSED_SYSLOG_FILE_PATTERN[] = "^[a-z]+\\.log\\.[0-9]+$";  // only uncompressed files
static const char    OB_COMPRESSED_SYSLOG_FILE_PATTERN[] = "^[a-z]+\\.log\\.[0-9]+\\.[a-z0-9]+$";  // only compressed files
static const char    OB_ARCHIVED_SYSLOG_FILE_PATTERN[] = "^[a-z]+\\.log\\.[0-9]+[.a-z0-9]*$"; // all syslog files (excluding wf logs)
static const char   *OB_SYSLOG_FILE_PREFIX[OB_SYSLOG_COMPRESS_TYPE_COUNT] =
{
  "observer.log",     // FD_SVR_FILE
  "rootservice.log",  // FD_RS_FILE
  "election.log",     // FD_ELEC_FILE
  "trace.log",        // FD_TRACE_FILE
  // no need to compress audit log and alert log
};
STATIC_ASSERT(MAX_FD_FILE == 6, "if you add a new log type, add it's prefix here !!!");

#define OB_LOG_COMPRESSOR ::oceanbase::common::ObLogCompressor::get_log_compressor()

struct ObSyslogFile
{
  ObSyslogFile() { reset(); }
  void reset()
  {
    mtime_ = INT64_MAX;
    memset(file_name_, 0, OB_MAX_SYSLOG_FILE_NAME_SIZE);
  }
  TO_STRING_KV(K_(mtime), K_(file_name));
  int64_t mtime_;
  char file_name_[OB_MAX_SYSLOG_FILE_NAME_SIZE];
};

struct ObSyslogCompareFunctor {
  bool operator()(const ObSyslogFile &l, const ObSyslogFile &r) {
    return l.mtime_ > r.mtime_;
  }
  int get_error_code() { return OB_SUCCESS; }
};

/* A priority array, order by modify time of files. */
class ObSyslogPriorityArray : public ObBinaryHeap<ObSyslogFile, ObSyslogCompareFunctor, OB_SYSLOG_DELETE_ARRAY_SIZE>
{
public:
  ObSyslogPriorityArray(ObSyslogCompareFunctor &cmp, common::ObIAllocator *allocator = NULL)
    : ObBinaryHeap<ObSyslogFile, ObSyslogCompareFunctor, OB_SYSLOG_DELETE_ARRAY_SIZE>(cmp, allocator)
  {}
  virtual ~ObSyslogPriorityArray() {}

  int push(const ObSyslogFile &element)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(array_.push_back(element))) {
    } else if (OB_FAIL(upheap(array_.count() - 1))) {
    } else if (array_.count() > OB_SYSLOG_DELETE_ARRAY_SIZE - 1) {
      ret = pop_back();
    }
    return ret;
  }

private:
  int pop_back()
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(array_.empty())) {
      ret = OB_EMPTY_RESULT;
    } else {
      int index = 0;
      const ObSyslogFile *max_element = &array_.at(0);
      for (int i = 1; i < array_.count(); i++) {
        if (cmp_(array_.at(i), *max_element)) {
          index = i;
          max_element = &array_.at(i);
        }
      }
      reset_root_cmp_cache();
      array_.at(index) = array_.at(array_.count() - 1);
      array_.pop_back();
      if (index < array_.count()) {
        if (index == get_root()) {
          ret = downheap(index);
        } else {
          int64_t parent = get_parent(index);
          if (cmp_(array_.at(parent), array_.at(index))) {
            ret = upheap(index);
          } else {
            ret = downheap(index);
          }
        }
      }
    }
    return ret;
  }

private:
  using ObBinaryHeapBase<ObSyslogFile, ObSyslogCompareFunctor, OB_SYSLOG_DELETE_ARRAY_SIZE>::array_;
  using ObBinaryHeapBase<ObSyslogFile, ObSyslogCompareFunctor, OB_SYSLOG_DELETE_ARRAY_SIZE>::cmp_;
  using ObBinaryHeapBase<ObSyslogFile, ObSyslogCompareFunctor, OB_SYSLOG_DELETE_ARRAY_SIZE>::get_root;
  using ObBinaryHeapBase<ObSyslogFile, ObSyslogCompareFunctor, OB_SYSLOG_DELETE_ARRAY_SIZE>::get_parent;
  using ObBinaryHeapBase<ObSyslogFile, ObSyslogCompareFunctor, OB_SYSLOG_DELETE_ARRAY_SIZE>::reset_root_cmp_cache;
  using ObBinaryHeap<ObSyslogFile, ObSyslogCompareFunctor, OB_SYSLOG_DELETE_ARRAY_SIZE>::upheap;
  using ObBinaryHeap<ObSyslogFile, ObSyslogCompareFunctor, OB_SYSLOG_DELETE_ARRAY_SIZE>::downheap;
};

class ObLogCompressor final : public lib::TGRunnable {
public:
  ObLogCompressor();
  virtual ~ObLogCompressor();
  int init();
  void stop();
  void wait();
  void awake();
  void destroy();
  int set_max_disk_size(int64_t max_disk_size);
  int set_compress_func(const char *compress_func_ptr);
  int set_min_uncompressed_count(int64_t min_uncompressed_count);
  inline bool is_enable_compress() { return NONE_COMPRESSOR != compress_func_; }
  static bool is_compressed_file(const char *file);
  static inline ObLogCompressor& get_log_compressor()
  {
    static ObLogCompressor log_compressor;
    return log_compressor;
  }
  TO_STRING_KV(K_(is_inited), K_(stopped), K_(loop_interval), K_(max_disk_size),
               K_(min_uncompressed_count), K_(compress_func));

private:
  void run1() override;
  void log_compress_loop_();
  int get_compressed_file_name_(const char *file_name, char compressed_file_name[]);
  int compress_single_block_(char *dest, size_t dest_size, const char *src, size_t src_size, size_t &return_size);
  int compress_single_file_(const char *file_name, char *src_buf, char *dest_buf);
  int set_last_modify_time_(const char *file_name, const time_t& newTime);
  int set_next_compressor_(ObCompressorType compress_func);
  int get_log_type_(const char *file_name);
  int64_t get_file_size_(const char *file_name);
  int64_t get_disk_remaining_size_();

private:
  bool is_inited_;
  bool stopped_;
  int64_t loop_interval_; // don't modify
  int64_t max_disk_size_;
  int64_t min_uncompressed_count_;
  char syslog_dir_[64];
  char alert_log_dir_[64];
  ObCompressorType compress_func_;
  ObCompressor *compressor_;
  ObCompressor *next_compressor_;
  ObThreadCond log_compress_cond_;
  ObSyslogCompareFunctor cmp_;
  ObSyslogPriorityArray oldest_files_;
};

}  // namespace common
}  // namespace oceanbase

#endif /* OB_LOG_COMPRESSOR_H_ */
