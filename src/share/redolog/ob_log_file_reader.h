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

#ifndef OB_LOG_FILE_READER_H_
#define OB_LOG_FILE_READER_H_

#include "lib/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_mutex.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/task/ob_timer.h"
#include "common/storage/ob_io_device.h"

namespace oceanbase
{
namespace share
{
struct ObLogReadFdKey final
{
public:
  ObLogReadFdKey();
  ~ObLogReadFdKey() = default;
  void reset();
  bool is_valid() const;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  bool operator==(const ObLogReadFdKey &other) const;
  bool operator!=(const ObLogReadFdKey &other) const;
  TO_STRING_KV(K(path_), "len", STRLEN(path_));
  char path_[common::MAX_PATH_SIZE];
};

struct ObLogReadFdCacheItem final
{
public:
  ObLogReadFdCacheItem();
  ~ObLogReadFdCacheItem() { reset(); };
  void inc_ref();
  void dec_ref();
  int64_t get_ref();
  void reset();
  TO_STRING_KV(K(key_), K(in_map_), K(io_fd_), K(ref_cnt_), K(timestamp_), KP(prev_), KP(next_));
public:
  ObLogReadFdKey key_;
  bool in_map_;
  common::ObIOFd io_fd_;
  int64_t ref_cnt_;
  int64_t timestamp_;
  ObLogReadFdCacheItem *prev_;
  ObLogReadFdCacheItem *next_;
};

class ObLogReadFdHandle final
{
public:
  ObLogReadFdHandle() : fd_item_(nullptr), is_local_(false) {};
  ~ObLogReadFdHandle() { reset(); };
  void reset();
  int set_read_fd(ObLogReadFdCacheItem *fd_item, const bool is_local);
  common::ObIOFd get_read_fd() const;
  OB_INLINE bool is_valid() const { return nullptr != fd_item_; };
  TO_STRING_KV(KP(fd_item_), K(is_local_));
private:
  ObLogReadFdCacheItem *fd_item_;
  bool is_local_; // indicate if the fd_item_ is only used for this handle
  DISALLOW_COPY_AND_ASSIGN(ObLogReadFdHandle);
};

class ObLogFileReader2 final
{
public:
  static ObLogFileReader2 & get_instance();

  int init();
  void destroy();

  int get_fd(const char* log_dir, const uint32_t file_id, ObLogReadFdHandle &fd_handle);
  int pread(
      const ObLogReadFdHandle &fd_handle,
      void *buf,
      const int64_t count,
      const int64_t offset,
      int64_t &read_size);
  int evict_fd(const char* log_dir, const uint32_t file_id);
  int evict_fd(const char* file_path);
private:
  ObLogFileReader2();
  ~ObLogFileReader2();

  int try_get_cache(const ObLogReadFdKey &fd_key, ObLogReadFdCacheItem *&ret_item);
  int evict_fd_from_map(const ObLogReadFdKey &fd_key);
  int put_new_item(
      const ObLogReadFdKey &fd_key,
      common::ObIOFd &open_io_fd,
      ObLogReadFdCacheItem *&ret_item,
      bool &is_tmp);
  int open_fd(const ObLogReadFdKey &fd_key, common::ObIOFd &ret_io_fd);

  int move_item_to_head(ObLogReadFdCacheItem &item);

  int do_clear_work();

private:
  class EvictTask : public common::ObTimerTask
  {
  public:
    explicit EvictTask(ObLogFileReader2 *reader) : reader_(reader) {}
    virtual ~EvictTask() = default;
    virtual void runTimerTask();
  private:
    ObLogFileReader2 *reader_;
    DISALLOW_COPY_AND_ASSIGN(EvictTask);
  };

private:
  typedef common::hash::ObHashMap<ObLogReadFdKey, ObLogReadFdCacheItem*, common::hash::NoPthreadDefendMode> FD_MAP;
  static const int64_t MAP_BUCKET_INIT_CNT = 53;
  static const int64_t CACHE_EVICT_TIME_IN_US = 60 * 1000 * 1000; // 1 minute
  // suppose CLOG read speed is 1GB/s. The maximum opened file count in 1 minute should
  // less than 960, far below MAX_OPEN_FILE_CNT
  static const int64_t MAX_OPEN_FILE_CNT = 1000;

  FD_MAP quick_map_;
  lib::ObMutex lock_;
  int64_t max_cache_fd_cnt_;
  int64_t cache_evict_time_in_us_;
  ObLogReadFdCacheItem *head_;
  ObLogReadFdCacheItem *tail_;
  common::ObTimer timer_;
  EvictTask evict_task_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObLogFileReader2);
};

#define OB_LOG_FILE_READER (::oceanbase::share::ObLogFileReader2::get_instance())
} // namespace share
} // namespace oceanbase
#endif /* OB_LOG_FILE_READER_H_ */
