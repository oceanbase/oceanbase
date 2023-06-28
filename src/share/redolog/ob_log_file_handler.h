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

#ifndef OCEANBASE_COMMON_OB_LOG_FILE_HANDLER_H_
#define OCEANBASE_COMMON_OB_LOG_FILE_HANDLER_H_

#include <stdint.h>
#include "share/redolog/ob_log_file_group.h"
#include "share/redolog/ob_log_definition.h"
#include "common/storage/ob_io_device.h"
#include "share/redolog/ob_log_policy.h"
#include "share/ob_io_device_helper.h"
#include "lib/ob_define.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/hash/ob_hashset.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace common
{
#define CLOG_DIO_ALIGN_SIZE 4096
class ObLogOpenCallback;
class ObLogWriteCallback;

struct ObNormalRetryWriteParam
{
  ObNormalRetryWriteParam(const int64_t n = ObLogDefinition::DEFAULT_IO_RETRY_CNT)
    : retry_cnt_(n)
  {
    matched_ret_values_.create(8);
  }

  bool is_valid() const { return retry_cnt_ >= 0; }
  void set_retry_cnt(const int64_t n = ObLogDefinition::DEFAULT_IO_RETRY_CNT) { retry_cnt_ = n; }
  bool match(const int ret_value) const;
  void destroy();

  int64_t retry_cnt_;
  hash::ObHashSet<int> matched_ret_values_;
};

// log file handler is responsible for read/write logs on various log device(local, OFS, etc)
// it calls interfaces in ObIODevice, and stores ObIOFd in other place to accomplish file io
class ObLogFileHandler
{
public:
  ObLogFileHandler();
  virtual ~ObLogFileHandler();

  // interface
  int init(
      const char *log_dir,
      int64_t file_size,
      const uint64_t tenant_id = OB_SERVER_TENANT_ID);
  void destroy();

  int open(const int64_t file_id, const int flag = ObLogDefinition::LOG_WRITE_FLAG);
  int close();
  int exist(const int64_t file_id, bool &is_exist);

  bool is_opened() const;

  // TODO: optimize read
  int read(void *buf, int64_t count, const int64_t offset, int64_t &read_size);
  int write(void *buf, int64_t count, const int64_t offset);

  // Write the buf to the head of a new file
  // Update file_id_, io_fd_ and max_file_id cache if success
  int delete_file(const int64_t file_id);
  int get_total_disk_space(int64_t &total_space) const;
  int get_total_used_size(int64_t &using_space) const;
  int get_file_id_range(int64_t &min_file_id, int64_t &max_file_id);

  // file handler status
  static bool is_valid_file_id(int64_t file_id);

  static int open(const char *file_path, const int flags, const mode_t mode, ObIOFd &io_fd);
  static int unlink(const char* file_path);
private:
  // basic function, implemented with io device
  int inner_open(const int flag, const int64_t file_id, ObIOFd &io_fd);
  int inner_close(const ObIOFd &io_fd);
  int inner_read(const ObIOFd &io_fd, void *buf, const int64_t size, const int64_t offset,
      int64_t &read_size, int64_t retry_cnt = ObLogDefinition::DEFAULT_IO_RETRY_CNT);

public:
  // helper function
  static int format_file_path(char *buf, const int64_t buf_size,
      const char *log_dir, const int64_t file_id);
private:
  // function with certain strategy
  int normal_retry_write(void *buf, int64_t size, int64_t offset);
  int do_open(const int flag, const int64_t file_id, ObIOFd &io_fd);

  void set_disk_warning(bool disk_warning);
private:
  class TmpFileCleaner : public common::ObBaseDirEntryOperator
  {
  public:
    explicit TmpFileCleaner(const char* log_dir) : log_dir_(log_dir) {}
    virtual ~TmpFileCleaner() = default;
    virtual int func(const dirent *entry) override;
  private:
    bool is_tmp_filename(const char *filename) const;
    const char* log_dir_;
  };
private:
  static constexpr int64_t SLEEP_TIME_US = 100 * 1000; // 100ms
  static constexpr int64_t LOG_INTERVAL_US = 10 * 1000 * 1000; // 10s
  static constexpr int64_t IO_HANG_THRESHOLD = 60LL * 1000 * 1000; // 60 seconds

  bool is_inited_;

  const char *log_dir_;
  int64_t file_id_;
  ObIOFd io_fd_;
  ObLogFileGroup file_group_;
  int64_t file_size_;
  uint64_t tenant_id_;
};

OB_INLINE void ObNormalRetryWriteParam::destroy()
{
  retry_cnt_ = -1;
  matched_ret_values_.destroy();
}

OB_INLINE bool ObLogFileHandler::is_opened() const
{
  return io_fd_.is_normal_file();
}

OB_INLINE int ObLogFileHandler::get_total_disk_space(int64_t &total_space) const
{
  return file_group_.get_total_disk_space(total_space);
}

OB_INLINE int ObLogFileHandler::get_total_used_size(int64_t &using_space) const
{
  return file_group_.get_total_used_size(using_space);
}

OB_INLINE int ObLogFileHandler::get_file_id_range(int64_t &min_file_id, int64_t &max_file_id)
{
  return file_group_.get_file_id_range(min_file_id, max_file_id);
}

OB_INLINE bool ObLogFileHandler::is_valid_file_id(int64_t file_id)
{
  return (file_id > 0 && file_id < OB_INVALID_FILE_ID);
}
} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_COMMON_OB_LOG_FILE_HANDLER_H_
