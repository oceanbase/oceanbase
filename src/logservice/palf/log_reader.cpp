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

#include "log_reader.h"
#include "lib/stat/ob_session_stat.h"     // Session
#include "log_io_adapter.h"               // LogIOAdapter
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
using namespace common;
namespace palf
{

LogReader::LogReader()
    : last_accum_read_statistic_time_(OB_INVALID_TIMESTAMP),
      accum_read_io_count_(0), accum_read_log_size_(0), accum_read_cost_ts_(0),
      is_inited_(false) {}

LogReader::~LogReader()
{
}

int LogReader::init(const char *log_dir, const offset_t block_size, LogIOAdapter *io_adapter)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(io_adapter)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    block_size_ = block_size;
    MEMCPY(log_dir_, log_dir, OB_MAX_FILE_NAME_LENGTH);
    last_accum_read_statistic_time_ = ObTimeUtility::fast_current_time();
    io_adapter_ = io_adapter;
    is_inited_ = true;
  }
  if (false == is_inited_) {
    destroy();
  }
  return ret;
}

void LogReader::destroy()
{
  if (IS_INIT) {
    is_inited_ = false;
    block_size_ = 0;
    io_adapter_ = NULL;
    MEMSET(log_dir_, '\0', OB_MAX_FILE_NAME_LENGTH);
    last_accum_read_statistic_time_ = OB_INVALID_TIMESTAMP;
    accum_read_io_count_ = 0;
    accum_read_log_size_ = 0;
    accum_read_cost_ts_ = 0;
  }
}

int LogReader::pread(const block_id_t block_id,
                     const offset_t offset,
                     int64_t in_read_size,
                     ReadBuf &read_buf,
                     int64_t &out_read_size,
                     LogIOContext &io_ctx) const
{
  int ret = OB_SUCCESS;
  ObIOFd io_fd;
  out_read_size = 0;
  char block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "pread failed", K(block_id), K(offset), K(in_read_size), K(read_buf));
  } else if (!is_valid_block_id(block_id) || offset >= block_size_ || 0 >= in_read_size || !read_buf.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(block_id), K(offset), K(in_read_size), K(read_buf));
  } else if (OB_FAIL(convert_to_normal_block(log_dir_, block_id, block_path, OB_MAX_FILE_NAME_LENGTH))) {
    PALF_LOG(ERROR, "convert_to_normal_block failed", K(ret));
  } else if (OB_FAIL(io_adapter_->open(block_path, LOG_READ_FLAG, FILE_OPEN_MODE, io_fd))) {
    PALF_LOG(WARN, "LogReader open block failed", K(ret), K(block_path), K(io_fd));
  } else {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    int64_t remained_read_size = in_read_size;
    int64_t remained_read_buf_len = read_buf.buf_len_;
    int64_t step = MAX_LOG_BUFFER_SIZE;
    while (remained_read_size > 0 && OB_SUCC(ret)) {
      const int64_t curr_in_read_size = MIN(step, remained_read_size);
      char *curr_read_buf = read_buf.buf_ + in_read_size - remained_read_size;
      const offset_t curr_read_offset = offset + in_read_size - remained_read_size;
      const int64_t curr_read_buf_len = remained_read_buf_len - out_read_size;
      int64_t curr_out_read_size = 0;
      if (OB_FAIL(inner_pread_(io_fd, curr_read_offset, curr_in_read_size, curr_read_buf, curr_read_buf_len, curr_out_read_size, io_ctx))) {
        PALF_LOG(WARN, "LogReader inner_pread_ failed", K(ret), K(io_fd), K(block_id), K(offset),
            K(in_read_size), K(read_buf), K(curr_in_read_size), K(curr_read_offset), K(curr_out_read_size),
            K(remained_read_size), K(block_path));
      } else {
        out_read_size += curr_out_read_size;
        remained_read_size -= curr_out_read_size;
        PALF_LOG(TRACE, "inner_pread_ success", K(ret), K(io_fd), K(block_id), K(offset), K(in_read_size),
            K(out_read_size), K(read_buf), K(curr_in_read_size), K(curr_read_offset), K(curr_out_read_size),
            K(remained_read_size), K(block_path));
      }
    }

    if (OB_SUCC(ret)) {
      const int64_t cost_ts = ObTimeUtility::fast_current_time() - start_ts;
      io_ctx.inc_read_disk_cost_ts(cost_ts);
      EVENT_TENANT_INC(ObStatEventIds::PALF_READ_IO_COUNT_FROM_DISK, MTL_ID());
      EVENT_ADD(ObStatEventIds::PALF_READ_SIZE_FROM_DISK, out_read_size);
      EVENT_ADD(ObStatEventIds::PALF_READ_TIME_FROM_DISK, cost_ts);
      const int64_t accum_read_io_count = ATOMIC_AAF(&accum_read_io_count_, 1);
      const int64_t accum_read_log_size = ATOMIC_AAF(&accum_read_log_size_, out_read_size);
      const int64_t accum_read_cost_ts = ATOMIC_AAF(&accum_read_cost_ts_, cost_ts);
      if (palf_reach_time_interval(PALF_IO_STAT_PRINT_INTERVAL_US, last_accum_read_statistic_time_)) {
        const int64_t avg_pread_cost = accum_read_cost_ts / accum_read_io_count;
        PALF_LOG(INFO, "[PALF STAT READ LOG INFO FROM DISK]", K(accum_read_io_count),
                 K(accum_read_log_size), K(avg_pread_cost));
        ATOMIC_STORE(&accum_read_io_count_, 0);
        ATOMIC_STORE(&accum_read_log_size_, 0);
        ATOMIC_STORE(&accum_read_cost_ts_, 0);
      }
    }
  }

  if (io_fd.is_valid()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(io_adapter_->close(io_fd))) {
      PALF_LOG(WARN, "close io_fd failed", K(tmp_ret), K(io_fd));
    }
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
  }
  return ret;
}

int LogReader::inner_pread_(const ObIOFd &read_io_fd,
                            offset_t start_offset,
                            int64_t in_read_size,
                            char *read_buf,
                            const int64_t read_buf_len,
                            int64_t &out_read_size,
                            LogIOContext &io_ctx) const
{
  int ret = OB_SUCCESS;
  offset_t aligned_start_offset = lower_align(start_offset, LOG_DIO_ALIGN_SIZE);
  offset_t backoff = start_offset - aligned_start_offset;
  int64_t aligned_in_read_size = upper_align(in_read_size + backoff, LOG_DIO_ALIGN_SIZE);
  int64_t limited_and_aligned_in_read_size = 0;
  ObWaitEventGuard wait_event(ObWaitEventIds::PALF_READ,
      PALF_IO_WAIT_EVENT_TIMEOUT_MS, read_io_fd.second_id_, start_offset, aligned_in_read_size);
  if (OB_FAIL(limit_and_align_in_read_size_by_block_size_(
          aligned_start_offset, aligned_in_read_size,  limited_and_aligned_in_read_size))) {
    PALF_LOG(WARN, "limited_and_aligned_in_read_size failed, maybe read offset exceed block size",
        K(ret), K(start_offset), K(in_read_size), K(aligned_start_offset), K(aligned_in_read_size),
        K(limited_and_aligned_in_read_size));
  } else if (limited_and_aligned_in_read_size > read_buf_len) {
    ret = OB_BUF_NOT_ENOUGH;
    PALF_LOG(WARN, "buffer not enough to hold read result");
  } else {
    int64_t retry_interval = 10 * 1000L;
    do {
      if (OB_FAIL(io_adapter_->pread(read_io_fd, limited_and_aligned_in_read_size, aligned_start_offset, read_buf, out_read_size, io_ctx))) {
        PALF_LOG(WARN, "pread failed", K(ret), K(read_io_fd), K(aligned_start_offset), K(limited_and_aligned_in_read_size), K(backoff), K(errno), K(out_read_size));
        if (OB_ALLOCATE_MEMORY_FAILED == ret) {
          ret = OB_EAGAIN;
          ob_usleep(retry_interval);
        }
      }
    } while (OB_EAGAIN == ret);

    if (OB_SUCC(ret)) {
      io_ctx.inc_read_io_cnt();
      io_ctx.inc_read_io_size(out_read_size);

      out_read_size = MIN(out_read_size - static_cast<int32_t>(backoff), in_read_size);
      MEMMOVE(read_buf, read_buf + backoff, in_read_size);
      PALF_LOG(TRACE, "inner_read_ success", K(ret), K(read_io_fd), K(aligned_start_offset),
          K(limited_and_aligned_in_read_size), K(backoff), KP(read_buf),
          K(in_read_size), K(out_read_size));
    }
  }
  return ret;
}

int LogReader::limit_and_align_in_read_size_by_block_size_(
    offset_t aligned_start_offset,
    int64_t aligned_in_read_size,
    int64_t &limited_and_aligned_in_read_size) const
{
  int ret = OB_SUCCESS;
  // NB:
  // 1. block_size_ is aligned by 4K
  // 2. aligned_start_offset is aligned by 4K
  // 3. aligned_in_read_size is aligned by 4K
  // 4. aligned_start_offset + aligned_in_read_size is
  //    aligned by 4K
  //
  // Therefore, limit_and_aligned_in_read_size is aligned
  // by 4K
  offset_t limited_read_end_offset =
    limit_read_end_offset_by_block_size_(aligned_start_offset, aligned_start_offset + aligned_in_read_size);

  if (limited_read_end_offset <= aligned_start_offset) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    limited_and_aligned_in_read_size = limited_read_end_offset - aligned_start_offset;
  }
  PALF_LOG(TRACE, "limit_and_align_in_read_size_by_block_size success",
      K(ret), K(limited_and_aligned_in_read_size), K(limited_read_end_offset),
      K(aligned_start_offset), K(aligned_in_read_size));
  return ret;
}

offset_t LogReader::limit_read_end_offset_by_block_size_(
    offset_t start_offset,
    offset_t end_offset) const
{
  return start_offset/block_size_ == end_offset/block_size_ ? end_offset : block_size_;
}
} // end of logservice
} // end of oceanbase
