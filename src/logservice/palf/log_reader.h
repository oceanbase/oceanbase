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
namespace oceanbase
{
namespace common
{
  class ObIOFd;
}
namespace palf
{
class ReadBuf;
class LogReader
{
public:
  LogReader();
  ~LogReader();
  int init(const char *log_dir, const offset_t block_size);
  //int init(LogDir *log_dir, const offset_t block_size);
  void destroy();
  int pread(const block_id_t block_id,
            const offset_t offset,
            int64_t in_read_size,
            ReadBuf &read_buf,
            int64_t &out_read_size) const;
private:
  int limit_and_align_in_read_size_by_block_size_(
      offset_t aligned_start_offset,
      int64_t aligned_in_read_size,
      int64_t &limited_and_aligned_in_read_size) const;
  offset_t limit_read_end_offset_by_block_size_(
      offset_t start_offset,
      offset_t end_offset) const;
  int inner_pread_(const int read_io_fd,
                   offset_t start_offset,
                   int64_t in_read_size,
                   char *read_buf,
                   int64_t &out_read_size) const;

private:
  offset_t block_size_;
  char log_dir_[OB_MAX_FILE_NAME_LENGTH];
  // LogDir *log_dir_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(LogReader);
  // TODO by runlin
  // 1). LogCache;
  // 2). Optimize open ObIOFd
};
} // end of logservice
} // end of oceanbase

#endif
