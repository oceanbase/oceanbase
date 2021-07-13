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

#ifndef OB_LOG_FILE_STORE_H_
#define OB_LOG_FILE_STORE_H_

#include <sys/stat.h>
#include "ob_log_disk_manager.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"

namespace oceanbase {
namespace common {
class ObLogFileStore;
class ObLogFileReader;

struct ObLogFileIOInfo final {
  int64_t disk_id_;
  int fd_;
  char* buf_;
  int64_t size_;
  int64_t offset_;
  // indicate if IO complete or not
  // used in partial complete case
  bool complete_;
  int ret_;

  ObLogFileIOInfo()
  {
    reset();
  }

  OB_INLINE void reset()
  {
    disk_id_ = -1;
    fd_ = -1;
    buf_ = NULL;
    size_ = 0;
    offset_ = 0;
    complete_ = true;
    ret_ = OB_SUCCESS;
  }

  TO_STRING_KV(K_(disk_id), K_(fd), KP_(buf), K_(size), K_(offset), K_(complete), K_(ret));
};

class ObLogFileDescriptor final {
public:
  friend class ObLogFileStore;
  friend class ObLogFileReader;

  ObLogFileDescriptor();
  virtual ~ObLogFileDescriptor();

  int init(ObLogDiskManager* disk_mgr, const int8_t flag, const int64_t file_id);
  void reset();
  bool is_valid() const;
  // check disk manager about the latest disk state, update related ObLogFdInfo
  int sync(const int64_t offset = 0);

  OB_INLINE int64_t count() const
  {
    return fd_infos_.count();
  }

  TO_STRING_KV(K_(flag), KP_(disk_mgr), K_(file_id), K_(is_inited), "fd_cnt", fd_infos_.count());

  static const int8_t READ_FLAG = 1;
  static const int8_t WRITE_FLAG = 2;
  static const int8_t TEMP_FLAG = 4;

private:
  int get_log_fd(const int64_t disk_id);
  OB_INLINE bool is_tmp() const
  {
    return (flag_ & TEMP_FLAG) > 0;
  }
  OB_INLINE bool enable_write() const
  {
    return (flag_ & WRITE_FLAG) > 0;
  }
  OB_INLINE bool enable_read() const
  {
    return (flag_ & READ_FLAG) > 0;
  }
  bool is_valid_state(const ObLogDiskState state);
  static const int8_t FLAG_MASK = 0x0F;

private:
  int8_t flag_;

  // local fd
  ObLogDiskManager* disk_mgr_;
  int64_t file_id_;
  ObSEArray<ObLogFdInfo, ObLogDiskManager::MAX_DISK_COUNT> fd_infos_;

  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObLogFileDescriptor);
};

class ObLogFileReader final {
public:
  static int get_fd(
      const char* log_dir, const int64_t file_id, const ObRedoLogType log_type, ObLogFileDescriptor& log_fd);
  static int64_t pread(ObLogFileDescriptor& log_fd, void* buf, const int64_t count, const int64_t offset);
  static int close_fd(ObLogFileDescriptor& log_fd);
};

// This class is responsible for reading & writing log files on multiple disks.
// It exposes file_id to caller but hide the underlying multiple disks detail.
// Caller read or write log file by file_id.
class ObILogFileStore {
public:
  ObILogFileStore() : log_type_(OB_REDO_TYPE_INVALID)
  {}
  virtual ~ObILogFileStore()
  {}

  virtual int init(const char* log_dir, const int64_t file_size, const clog::ObLogWritePoolType type) = 0;
  virtual void destroy() = 0;

  // For single thread writing, NOT thread-safe
  virtual int open(const int64_t file_id) = 0;
  virtual int close() = 0;
  virtual int write(void* buf, int64_t count, int64_t offset) = 0;
  virtual int read(void* buf, int64_t count, int64_t offset, int64_t& read_size) = 0;
  virtual int fsync() = 0;
  virtual int write_file(const int64_t file_id, void* buf, const int64_t count) = 0;

  // Thread-safe
  virtual int delete_all_files() = 0;
  virtual int delete_file(const int64_t file_id) = 0;
  virtual bool is_opened() const = 0;
  virtual int ftruncate(const int64_t file_id, const int64_t length) = 0;
  virtual int exist(const int64_t file_id, bool& is_exist) const = 0;
  virtual int get_file_st_size(const int64_t file_id, int64_t& st_size) const = 0;
  virtual int get_file_st_time(const int64_t file_id, time_t& st_time) const = 0;

  virtual int get_file_id_range(uint32_t& min_file_id, uint32_t& max_file_id) const = 0;
  virtual const char* get_dir_name() const = 0;
  virtual int get_total_disk_space(int64_t& total_space) const = 0;
  virtual int get_total_used_size(int64_t& total_size) const = 0;

  virtual uint32_t get_min_using_file_id() const = 0;
  virtual uint32_t get_min_file_id() const = 0;
  virtual int64_t get_free_quota() const = 0;
  virtual void update_min_using_file_id(const uint32_t file_id) = 0;
  virtual void update_min_file_id(const uint32_t file_id) = 0;
  virtual void update_max_file_id(const uint32_t file_id) = 0;
  virtual void try_recycle_file() = 0;
  virtual int update_free_quota() = 0;
  virtual bool free_quota_warn() const = 0;
  virtual ObRedoLogType get_redo_log_type() const
  {
    return log_type_;
  };

  // help functions
  static bool is_valid_file_id(int64_t file_id);
  static int format_file_path(
      char* buf, const int64_t size, const char* log_dir, const int64_t file_id, const bool is_tmp);

public:
  static const int OPEN_FLAG_READ = O_RDONLY | O_DIRECT;
  static const int OPEN_FLAG_WRITE = O_WRONLY | O_DIRECT | O_SYNC | O_CREAT | O_APPEND;
  static const int OPEN_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  static const int64_t DEFAULT_DISK_USE_PERCENT = 80;  // default value for clog
  static const int64_t SLOG_DISK_USE_PERCENT = 80;

protected:
  static const int MAX_IO_COUNT = 1024;
  static const int64_t AIO_TIMEOUT_SECOND = 30;
  static const int64_t CLOG_AIO_TIMEOUT_SECOND = 300;
  static const int64_t AIO_RETRY_INTERVAL_US = 100 * 1000;  // 100ms
  static const int64_t MAX_DISK_COUNT = ObLogDiskManager::MAX_DISK_COUNT;
  static const int MAX_IO_RETRY = 3;

  ObRedoLogType log_type_;
};

class ObLogFileStore : public ObILogFileStore {
public:
  ObLogFileStore();
  virtual ~ObLogFileStore();

  virtual int init(const char* log_dir, const int64_t file_size, const clog::ObLogWritePoolType type) override;
  virtual void destroy() override;

  virtual int open(const int64_t file_id) override;
  virtual int close() override;
  virtual int write(void* buf, int64_t count, int64_t offset) override;
  virtual int read(void* buf, int64_t count, int64_t offset, int64_t& read_size) override;
  virtual int fsync() override;
  virtual int write_file(const int64_t file_id, void* buf, const int64_t count) override;

  virtual int delete_all_files() override;
  virtual int delete_file(const int64_t file_id) override;
  virtual int ftruncate(const int64_t file_id, const int64_t length) override;
  virtual int exist(const int64_t file_id, bool& is_exist) const override;
  virtual int get_file_st_size(const int64_t file_id, int64_t& st_size) const override;
  virtual int get_file_st_time(const int64_t file_id, time_t& st_time) const override;

  virtual int get_file_id_range(uint32_t& min_file_id, uint32_t& max_file_id) const override;
  virtual const char* get_dir_name() const override;
  virtual int get_total_disk_space(int64_t& total_space) const override;
  virtual int get_total_used_size(int64_t& total_size) const override;

  virtual bool is_opened() const override
  {
    return write_fd_.is_valid();
  }

  virtual uint32_t get_min_using_file_id() const override
  {
    return disk_mgr_->get_min_using_file_id();
  }
  virtual uint32_t get_min_file_id() const override
  {
    return disk_mgr_->get_min_file_id();
  }
  virtual int64_t get_free_quota() const override
  {
    return disk_mgr_->get_free_quota();
  }

  virtual void update_min_using_file_id(const uint32_t file_id) override
  {
    disk_mgr_->update_min_using_file_id(file_id);
  }

  virtual void update_min_file_id(const uint32_t file_id) override
  {
    disk_mgr_->update_min_file_id(file_id);
  }

  virtual void update_max_file_id(const uint32_t file_id) override;

  virtual void try_recycle_file() override
  {
    disk_mgr_->try_recycle_file();
  }

  virtual int update_free_quota() override
  {
    return disk_mgr_->update_free_quota();
  }

  virtual bool free_quota_warn() const override
  {
    return disk_mgr_->free_quota_warn();
  }

private:
  int inner_open(const int64_t file_id, const int8_t flag, ObLogFileDescriptor& log_fd);
  int inner_close(ObLogFileDescriptor& log_fd);
  int rename(const int64_t file_id);
  int prepare_write_info(void* buf, int64_t count, int64_t offset);
  int process_io_prep_pwrite(const int64 submitted, int64_t& req_cnt);
  int process_io_submit(io_context_t ctx, struct iocb** requests, const int64_t cnt, int64_t& submitted);
  int process_io_getevents(int64_t& submitted, io_context_t ctx, struct io_event* events);
  bool process_retry(const int result, int64_t& retry);
  int process_failed_write();
  int fstat(const int64_t file_id, struct stat* file_stat) const;

private:
  bool is_inited_;
  ObLogDiskManager* disk_mgr_;

  // current opened file for write
  ObLogFileDescriptor write_fd_;

  // AIO
  io_context_t io_ctx_;
  struct io_event io_events_[MAX_IO_COUNT];
  struct iocb io_reqs_[MAX_DISK_COUNT];
  struct iocb* io_req_ptrs_[MAX_DISK_COUNT];
  ObLogFileIOInfo pending_wr_[MAX_DISK_COUNT];

  DISALLOW_COPY_AND_ASSIGN(ObLogFileStore);
};
}  // namespace common
}  // namespace oceanbase

#endif /*OB_LOG_FILE_STORE_H_*/
