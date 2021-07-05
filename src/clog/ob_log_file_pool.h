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

#ifndef OCEANBASE_CLOG_OB_LOG_FILE_POOL_
#define OCEANBASE_CLOG_OB_LOG_FILE_POOL_

#include <fcntl.h>
#include <stdint.h>
#include <dirent.h>
#include <sys/types.h>
#include <libaio.h>
#include "lib/allocator/ob_malloc.h"
#include "ob_log_define.h"
#include "ob_log_dir.h"

namespace oceanbase {
namespace clog {
class IFilePool {
public:
  IFilePool()
  {}
  virtual ~IFilePool()
  {}
  virtual int get_file_id_range(file_id_t& min_file_id, file_id_t& max_file_id) = 0;
  virtual int get_fd(const file_id_t file_id, int& fd) = 0;
  virtual int close_fd(const file_id_t file_id, const int fd) = 0;
  virtual const char* get_dir_name() const = 0;
};

class ObLogWriteFilePool : public IFilePool {
public:
  ObLogWriteFilePool();
  virtual ~ObLogWriteFilePool();

public:
  int init(ObILogDir* log_dir, const int64_t file_size, const ObLogWritePoolType type);
  void destroy();
  int get_fd(const file_id_t dest_file_id, int& fd);
  int close_fd(const file_id_t file_id, const int fd);
  int get_file_id_range(file_id_t& min_file_id, file_id_t& max_file_id);
  const char* get_dir_name() const
  {
    return (NULL == log_dir_ ? "" : log_dir_->get_dir_name());
  }
  void stat_fd_cache_hit_rate()
  {}
  int64_t get_free_quota() const
  {
    return limit_free_quota_;
  }
  bool free_quota_warn() const
  {
    return free_quota_ < 0;
  }
  int update_free_quota();
  file_id_t get_min_file_id() const
  {
    return ATOMIC_LOAD(&min_file_id_);
  }
  file_id_t get_min_using_file_id() const
  {
    return ATOMIC_LOAD(&min_using_file_id_);
  }
  void update_min_file_id(const file_id_t file_id);
  void update_max_file_id(const file_id_t file_id);
  void update_min_using_file_id(const file_id_t file_id);
  void try_recycle_file();
  int get_total_used_size(int64_t& total_size) const;
  TO_STRING_KV(K_(is_inited), K_(log_dir), K_(file_size), K_(free_quota), K_(limit_free_quota), K_(type),
      K_(min_file_id), K_(min_using_file_id), K_(max_file_id));

protected:
  int create_new_file(const int dest_dir_fd, const int dest_file_id, const char* dest_file, int& fd);
  int create_tmp_file(const file_id_t file_id, char* fname, const int64_t size, int& fd, int& dir_fd);
  int update_free_quota(const char* dir, const int64_t percent, const int64_t limit_percent);
  void remove_overquota_file();
  int init_raw_file(const int fd, const int64_t start_pos, const int64_t size);
  int get_recyclable_file(file_id_t& file_id);
  int create_file(const int dir_fd, const char* fname, const int64_t file_size, int& fd);
  void unlink_file(const char* fname, const int dir_fd);
  int fsync_dir(const int dir_fd);
  int get_dir_name(const char* fname, char* dir_name, const int64_t len);
  int rename_file(const int src_dir_fd, const char* srcfile, const int dest_dir_fd, const char* destfile);

private:
  static const int TASK_NUM = 1024;
  static const int OPEN_FLAG_WITH_SYNC = O_RDWR | O_DIRECT | O_SYNC;
  static const int OPEN_FLAG_WITHOUT_SYNC = O_RDWR | O_DIRECT;
  static const int OPEN_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  static const int CREATE_FLAG_WITH_SYNC = OPEN_FLAG_WITH_SYNC | O_CREAT;
  static const int64_t RESERVED_QUOTA = 4L * 1024L * 1024L * 1024L;
  // clog_disk_usage_limit_percentage set as 100,
  // and still keep a little more space(1GB)
  static const int64_t RESERVED_QUOTA_2 = 1L * 1024L * 1024L * 1024L;
  static const int64_t MIN_CLOG_CACHE_FILE_COUNT = 4;
  static const int64_t MIN_ILOG_CACHE_FILE_COUNT = 1;
  static const int64_t INIT_CACHE_FILE_COUNT = 1;

private:
  bool is_inited_;
  int64_t file_size_;
  common::ObSpinLock lock_;
  // Current remain space
  int64_t free_quota_;
  int64_t limit_free_quota_;
  ObILogDir* log_dir_;
  ObLogWritePoolType type_;
  file_id_t min_file_id_;
  file_id_t min_using_file_id_;
  file_id_t max_file_id_;

  io_context_t ctx_;
  struct iocb iocb_;
  struct io_event ioevent_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogWriteFilePool);
};
}  // end namespace clog
}  // end namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_LOG_FILE_POOL_
