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

#ifndef OB_BASE_LOG_BUFFER_H_
#define OB_BASE_LOG_BUFFER_H_

#include "lib/ob_errno.h"
#include <sys/shm.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "lib/lock/ob_mutex.h"

namespace oceanbase
{
namespace common
{
union ObAtomicFilePos
{
  ObAtomicFilePos() : atomic_(0) {}
  explicit ObAtomicFilePos(uint64_t atomic) : atomic_(atomic) {}
  bool operator==(const ObAtomicFilePos &other) const { return atomic_ == other.atomic_; }
  bool operator!=(const ObAtomicFilePos &other) const { return atomic_ != other.atomic_; }
  bool operator<(const ObAtomicFilePos &other) const { return file_id_ < other.file_id_ || (file_id_ == other.file_id_ && file_offset_ < other.file_offset_); }
  bool operator>(const ObAtomicFilePos &other) const { return file_id_ > other.file_id_ || (file_id_ == other.file_id_ && file_offset_ > other.file_offset_); }
  bool operator<=(const ObAtomicFilePos &other) const { return file_id_ < other.file_id_ || (file_id_ == other.file_id_ && file_offset_ <= other.file_offset_); }
  bool operator>=(const ObAtomicFilePos &other) const { return file_id_ > other.file_id_ || (file_id_ == other.file_id_ && file_offset_ >= other.file_offset_); }
  TO_STRING_KV(K_(file_id), K_(file_offset));
  volatile uint64_t atomic_;
  struct
  {
    uint32_t file_id_;
    uint32_t file_offset_;
  };
};

//DO NOT Change the struct size
struct ObBaseLogBuffer
{
  static const int32_t SELF_SIZE = 256 + 128;
  static const int32_t MAX_LOG_DIR_LENGTH = 128;
  static const int32_t STATIC_RESERVE_BYTES = 100;
  //static area
  char log_dir_[MAX_LOG_DIR_LENGTH];
  uint32_t buf_len_;
  ObAtomicFilePos file_flush_pos_;
  ObAtomicFilePos file_write_pos_;
  int64_t shm_stamp_;
  uint8_t static_reserved_[STATIC_RESERVE_BYTES];
};

struct ObBaseLogBufferCtrl
{
  ObBaseLogBufferCtrl() : base_buf_(NULL), data_buf_(NULL) {}
  ObBaseLogBuffer *base_buf_;
  char *data_buf_;
};

class ObBaseLogBufferMgr
{
public:
  void destroy();
  /**
   * Thread safe.
   * @param log_dir: input, should be full path, the same path identify the same log buf
   * @param log_ctrl: output, log buffer with share memory
   */
private:
  ObBaseLogBufferMgr();
  virtual ~ObBaseLogBufferMgr();
  int get_abs_dir(const char *log_dir, const char *&abs_log_dir);
  static const int32_t MAX_LOG_BUF_CNT = 128;
  static const int32_t MAX_LOG_BUFFER_SIZE = 2 * 1024 * 1024;
  static const int32_t SHM_HEADER_SIZE = 4 * 1024;
  static const int32_t SHM_BUFFER_SIZE = SHM_HEADER_SIZE + MAX_LOG_BUFFER_SIZE;
  int64_t log_buf_cnt_;
  ObBaseLogBufferCtrl log_ctrls_[MAX_LOG_BUF_CNT];
};

}
}

#endif /* OB_BASE_LOG_BUFFER_H_ */
