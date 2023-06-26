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

#ifndef STORAGE_LOG_STREAM_BACKUP_FILE_WRITER_CTX_H_
#define STORAGE_LOG_STREAM_BACKUP_FILE_WRITER_CTX_H_
#include "common/storage/ob_io_device.h"
#include "storage/blocksstable/ob_data_buffer.h"
namespace oceanbase {

namespace backup {

struct ObBackupFileWriteCtx {
public:
  ObBackupFileWriteCtx();
  virtual ~ObBackupFileWriteCtx();
  int open(const int64_t max_file_size, const common::ObIOFd &io_fd, common::ObIODevice &device_handle);
  bool is_opened() const
  {
    return is_inited_;
  }
  int append_buffer(const blocksstable::ObBufferReader &buffer, const bool is_last_part = false);
  int64_t get_file_size() const
  {
    return file_size_;
  }
  int close();

private:
  int write_buffer_(const char *buf, const int64_t len, const bool is_last_part);
  bool check_can_flush_(const bool is_last_part) const;
  int flush_buffer_(const bool is_last_part);
  int commit_file_();

private:
  bool is_inited_;
  int64_t file_size_;
  int64_t max_file_size_;
  common::ObIOFd io_fd_;
  common::ObIODevice *dev_handle_;
  blocksstable::ObSelfBufferWriter data_buffer_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupFileWriteCtx);
};

}  // namespace backup
}  // namespace oceanbase

#endif
