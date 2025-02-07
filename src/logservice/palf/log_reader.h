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

#ifndef OCEANBASE_LOGSERVICE_LOG_READER_
#define OCEANBASE_LOGSERVICE_LOG_READER_
#include <stdint.h>
#include "lib/ob_define.h"                      // OB_MAX_FILE_NAME_LENGTH
#include "lib/utility/ob_macro_utils.h"         //DISALLOW_COPY_AND_ASSIGN
#include "log_define.h"
#include "palf_iterator.h"
#include "common/storage/ob_io_device.h"        // ObIOFd
#include "log_io_context.h"                     // LogIOContext
namespace oceanbase
{
namespace common
{
class ObIOFd;
}
namespace palf
{
class ReadBuf;
class LogIOAdapter;
class LogReader
{
public:
  LogReader();
  ~LogReader();
  int init(const char *log_dir, const offset_t block_size, LogIOAdapter *io_adapter);
  //int init(LogDir *log_dir, const offset_t block_size);
  void destroy();
  int pread(const block_id_t block_id,
            const offset_t offset,
            int64_t in_read_size,
            ReadBuf &read_buf,
            int64_t &out_read_size,
            LogIOContext &io_ctx) const;
private:
  int limit_and_align_in_read_size_by_block_size_(
      offset_t aligned_start_offset,
      int64_t aligned_in_read_size,
      int64_t &limited_and_aligned_in_read_size) const;
  offset_t limit_read_end_offset_by_block_size_(
      offset_t start_offset,
      offset_t end_offset) const;
  int inner_pread_(const ObIOFd &read_io_fd,
                   offset_t start_offset,
                   int64_t in_read_size,
                   char *read_buf,
                   const int64_t read_buf_len,
                   int64_t &out_read_size,
                   LogIOContext &io_ctx) const;

private:
  offset_t block_size_;
  char log_dir_[OB_MAX_FILE_NAME_LENGTH];
  // LogDir *log_dir_;
  mutable int64_t last_accum_read_statistic_time_;
  mutable int64_t accum_read_io_count_;
  mutable int64_t accum_read_log_size_;
  mutable int64_t accum_read_cost_ts_;
  LogIOAdapter *io_adapter_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(LogReader);
  // TODO by runlin
  // 1). LogCache;
  // 2). Optimize open ObIOFd
};
} // end of logservice
} // end of oceanbase

#endif
