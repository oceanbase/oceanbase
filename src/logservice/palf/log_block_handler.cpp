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

#include "log_block_handler.h"
#include "lib/ob_define.h"                              // some constexpr
#include "lib/ob_errno.h"                               // OB_SUCCESS...
#include "lib/stat/ob_session_stat.h"         // Session
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_macro_utils.h"                 // some macros
#include "lib/utility/ob_tracepoint.h"                  // ERRSIM
#include "lib/utility/ob_utility.h"                     // ob_pwrite
#include "share/ob_errno.h"                             // errno
#include "share/rc/ob_tenant_base.h"                    // mtl_malloc
#include "log_writer_utils.h"                           // LogWriteBuf
#include "log_io_utils.h"                               // close_with_ret
namespace oceanbase
{
using namespace common;
using namespace share;
namespace palf
{
LogDIOAlignedBuf::LogDIOAlignedBuf(): buf_write_offset_(LOG_INVALID_LSN_VAL),
                                      buf_padding_size_(0),
                                      align_size_(-1),
                                      aligned_buf_size_(-1),
                                      aligned_data_buf_(NULL),
                                      aligned_used_ts_(0),
                                      truncate_used_ts_(0),
                                      is_inited_(false)
{
}

LogDIOAlignedBuf::~LogDIOAlignedBuf()
{
  destroy();
}

int LogDIOAlignedBuf::init(uint32_t align_size, uint32_t aligned_buf_size)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    PALF_LOG(ERROR,"LogDIOAlignedBuf has initted", K(ret), KPC(this));
  } else if (OB_ISNULL(aligned_data_buf_ = reinterpret_cast<char *>(
      mtl_malloc_align(align_size, aligned_buf_size, "LogDIOAligned")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    buf_write_offset_ = 0;
    align_size_ = align_size;
    aligned_buf_size_ = aligned_buf_size;
    memset(aligned_data_buf_, 0, aligned_buf_size_);
    is_inited_ = true;
    PALF_LOG(INFO, "LogDIOAlignedBuf init success", K(ret), K(align_size),
        K(aligned_buf_size), KP(aligned_data_buf_));
  }
  return ret;
}

void LogDIOAlignedBuf::destroy()
{
  if (IS_INIT) {
    is_inited_ = false;
    if (NULL != aligned_data_buf_) {
      mtl_free_align(aligned_data_buf_);
      aligned_data_buf_ = NULL;
    }
    PALF_LOG(INFO, "destroy LogDIOAlignedBuf success");
  }
}

int LogDIOAlignedBuf::align_buf(const char *input,
    const int64_t input_len,
    char *&output,
    int64_t &output_len,
    offset_t &offset)
{
  int ret = OB_SUCCESS;
  if (NULL == input || 0 >= input_len) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(input),
        K(input_len), K(offset));
  } else if (false == need_align_()) {
    PALF_LOG(TRACE, "no need align", K(ret));
  } else {
    int64_t start_ts = ObTimeUtility::fast_current_time();
    memcpy(static_cast<char*>(aligned_data_buf_) + buf_write_offset_, input, input_len);
    buf_write_offset_ += static_cast<offset_t>(input_len);
    align_buf_();
    output = aligned_data_buf_;
    output_len = buf_write_offset_;
    offset = lower_align(offset, align_size_);
    int64_t cost_ts = ObTimeUtility::fast_current_time() - start_ts;
    aligned_used_ts_ += cost_ts;
  }
  return ret;
}

void LogDIOAlignedBuf::truncate_buf()
{
  if (false == need_align_()) {
    reset_buf();
  } else {
    int64_t start_ts = ObTimeUtility::fast_current_time();
    offset_t tail_part_start = 0;
    tail_part_start = static_cast<offset_t>(lower_align(buf_write_offset_ - buf_padding_size_, align_size_));
    buf_write_offset_ = (buf_write_offset_ - buf_padding_size_) % align_size_;
    if (buf_write_offset_ > 0) {
      memmove(aligned_data_buf_, static_cast<char*>(aligned_data_buf_) + tail_part_start,
          buf_write_offset_);
    }
    int64_t cost_ts = ObTimeUtility::fast_current_time() - start_ts;
    truncate_used_ts_ += cost_ts;
    PALF_LOG(TRACE, "truncate_buf success", KPC(this), K(buf_write_offset_), K(buf_padding_size_), K(tail_part_start));
  }
}

void LogDIOAlignedBuf::reset_buf()
{
  buf_padding_size_ = 0;
  buf_write_offset_ = 0;
  memset(aligned_data_buf_, 0, aligned_buf_size_);
}

void LogDIOAlignedBuf::align_buf_()
{
  if (0 == (buf_write_offset_ % align_size_)) {
    buf_padding_size_ = 0;
  } else {
    buf_padding_size_ = upper_align(buf_write_offset_, LOG_DIO_ALIGN_SIZE) - buf_write_offset_;
    MEMSET(aligned_data_buf_+buf_write_offset_, 0, buf_padding_size_);
    buf_write_offset_ = upper_align(buf_write_offset_, LOG_DIO_ALIGN_SIZE);
  }
}

LogBlockHandler::LogBlockHandler()
  : dio_aligned_buf_(),
    log_block_size_(0),
    total_write_size_(0),
    total_write_size_after_dio_(0),
    ob_pwrite_used_ts_(0),
    count_(0),
    trace_time_(OB_INVALID_TIMESTAMP),
    dir_fd_(-1),
    io_fd_(-1),
    is_inited_(false)
{
}

LogBlockHandler::~LogBlockHandler()
{
  destroy();
}

int LogBlockHandler::init(const int dir_fd,
                          const int64_t log_block_size,
                          const int64_t align_size,
                          const int64_t align_buf_size)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
		ret = OB_INIT_TWICE;
	} else if (-1 == dir_fd
      || (0 != (log_block_size & (align_size - 1)))) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid argument", K(ret), K(log_block_size));
  } else if (OB_FAIL(dio_aligned_buf_.init(align_size,
          align_buf_size))) {
    PALF_LOG(ERROR, "init dio_aligned_buf_ failed", K(ret));
  } else {
    dir_fd_ = dir_fd;
    log_block_size_ = log_block_size;
    is_inited_ = true;
    PALF_LOG(INFO, "LogBlockHandler init success", K(ret), K(log_block_size_), K(align_size), K(align_buf_size));
  }
  return ret;
}

void LogBlockHandler::destroy()
{
  if (IS_INIT) {
    is_inited_ = false;
    dir_fd_ = -1;
    if (-1 != io_fd_) {
      close_with_ret(io_fd_);
      io_fd_ = -1;
    }
    log_block_size_ = 0;
    dio_aligned_buf_.destroy();
    PALF_LOG(INFO, "LogFileHandler destroy success");
  }
}

int LogBlockHandler::switch_next_block(const char *block_path)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(inner_close_())) {
    PALF_LOG(ERROR, "inner_close_", K(ret), K(block_path));
  } else if (OB_FAIL(open(block_path))) {
    PALF_LOG(WARN, "open failed", K(ret));
  } else {
    PALF_LOG(TRACE, "switch_next_block success", K(ret), KPC(this));
  }
  return ret;
}

int LogBlockHandler::open(const char *block_path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_open_(block_path))) {
    PALF_LOG(ERROR, "inner open block failed", K(ret), K(block_path));
  } else {
    PALF_LOG(INFO, "open block success", K(ret), KPC(this));
  }
  return ret;
}

int LogBlockHandler::close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_close_())) {
    PALF_LOG(WARN, "close current block failed", K(ret), KPC(this));
  } else {
    PALF_LOG(INFO, "LogFileHandler close success", K(ret), KPC(this));
  }
  return ret;
}

int LogBlockHandler::truncate(const offset_t offset)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_truncate_(offset))) {
    PALF_LOG(WARN, "inner_truncate_ failed", K(ret), K(offset), KPC(this));
  } else {
    PALF_LOG(INFO, "LogBlockHandler truncate success", K(ret), K(offset), KPC(this));
  }
  return ret;
}

int LogBlockHandler::load_data(const offset_t offset)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_load_data_(offset))) {
    PALF_LOG(WARN, "inner_load_data_ failed", K(ret), K(offset), KPC(this));
  } else {
    PALF_LOG(INFO, "LogBlockHandler load_data success", K(ret), K(offset), KPC(this));
  }
  return ret;
}

int LogBlockHandler::pwrite(const offset_t offset,
                            const char *buf,
                            const int64_t buf_len)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(inner_write_once_(offset, buf, buf_len))) {
    PALF_LOG(ERROR, "inner_write_once_ failed", K(ret));
  } else {
    PALF_LOG(TRACE, "pwrite success", K(ret), K(offset), KPC(this));
  }
  return ret;
}

int LogBlockHandler::writev(const offset_t offset,
                            const LogWriteBuf &write_buf)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_writev_once_(offset, write_buf))) {
    PALF_LOG(ERROR, "inner_write_once_ failed", K(ret), K(offset), K(write_buf));
  } else {
    PALF_LOG(TRACE, "writev success", K(ret), K(offset), K(write_buf), KPC(this));
  }
  return ret;
}

int LogBlockHandler::inner_close_()
{
  int ret = OB_SUCCESS;
  do {
    if (-1 == io_fd_) {
      PALF_LOG(INFO, "block has been closed or not eixst", K(ret));
    } else if (-1 == (::close(io_fd_))){
      ret = convert_sys_errno();
      PALF_LOG(ERROR, "close block failed", K(ret), K(errno), KPC(this));
      ob_usleep(RETRY_INTERVAL);
    } else {
      io_fd_ = -1;
      ret = OB_SUCCESS;
      break;
    }
  } while (OB_FAIL(ret));
  return ret;
}

int LogBlockHandler::inner_open_(const char *block_path)
{
  int ret = OB_SUCCESS;
  do {
    if (-1 == (io_fd_ = ::openat(dir_fd_, block_path, LOG_WRITE_FLAG, FILE_OPEN_MODE))) {
      ret = convert_sys_errno();
      PALF_LOG(ERROR, "open block failed", K(ret), K(errno), K(block_path), K(dir_fd_));
      ob_usleep(RETRY_INTERVAL);
    } else {
      dio_aligned_buf_.reset_buf();
      ret = OB_SUCCESS;
      break;
    }
  } while (OB_FAIL(ret));
  return ret;
}

int LogBlockHandler::inner_truncate_(const offset_t offset)
{
  int ret = OB_SUCCESS;
  // NB: the first phase make block size to 'offset', the second phase make block size to 'log_block_size_'
  //     meanwhile, the second phase will guarantee that the data after offset is inited as zero.
  //     fallocate support deallocate block,
  //
  //     refer to the man 0 fallocate:
  //     If the block previously was larger than this size, the extra data is lost. If the block previously
  //     was shorter, it is extended, and the extended part reads as null bytes ('\0').
  //
  // TODO by runlin, keep follow steps atomic, can use rename?
  do {
    if (0 != ftruncate(io_fd_, offset)) {
      ret = convert_sys_errno();
      PALF_LOG(ERROR, "ftruncate first phase failed", K(ret), K(errno), KPC(this), K(offset));
      ob_usleep(RETRY_INTERVAL);
    } else if (0 != ftruncate(io_fd_, log_block_size_)) {
      ret = convert_sys_errno();
      PALF_LOG(ERROR, "ftruncate second phase failed", K(ret), K(errno), KPC(this), K(log_block_size_));
      ob_usleep(RETRY_INTERVAL);
    } else {
      dio_aligned_buf_.reset_buf();
      ret = OB_SUCCESS;
      PALF_LOG(INFO, "inner_truncate_ success", K(ret), K(offset));
      break;
    }
  } while (OB_FAIL(ret));
  return ret;
}

int LogBlockHandler::inner_load_data_(const offset_t offset)
{
  int ret = OB_SUCCESS;
  char *input = NULL;
  char *output = NULL;
  int64_t output_len = 0;
  offset_t out_offset = 0;
  offset_t aligned_offset = lower_align(offset, LOG_DIO_ALIGN_SIZE);
  // tail minus offset must be greater than LOG_DIO_ALIGN_SIZE
  offset_t read_count = offset - aligned_offset;
  offset_t aligned_read_count = upper_align(offset - aligned_offset, LOG_DIO_ALIGN_SIZE);
  if (OB_ISNULL(input = reinterpret_cast<char *>(
      mtl_malloc_align(LOG_DIO_ALIGN_SIZE, aligned_read_count, "LogDIOAligned")))) {
    PALF_LOG(WARN, "allocate memory failed", K(ret));
  } else if ((aligned_read_count != ob_pread(io_fd_, input, aligned_read_count, aligned_offset))) {
    ret = convert_sys_errno();
    PALF_LOG(WARN, "ob_pread failed", K(ret), K(errno), K(offset), K(read_count), K(aligned_read_count),
        K(aligned_offset), K(dio_aligned_buf_), K(io_fd_), KPC(this));
  } else if (0 != aligned_read_count && OB_FAIL(dio_aligned_buf_.align_buf(
          input, read_count, output, output_len, out_offset))) {
    PALF_LOG(WARN, "align_buf failed", K(ret), K(dio_aligned_buf_));
  } else {
    dio_aligned_buf_.truncate_buf();
    PALF_LOG(INFO, "inner_load_data_ success", K(ret), K(offset), K(read_count), K(dio_aligned_buf_),
        K(aligned_read_count));
  }
  if (NULL != input) {
    mtl_free_align(input);
  }
  return ret;
}

int LogBlockHandler::inner_write_once_(const offset_t offset,
    const char *buf,
    const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  char *aligned_buf = const_cast<char *>(buf);
  int64_t aligned_buf_len = buf_len;

  offset_t aligned_block_offset = offset;
  if (OB_FAIL(dio_aligned_buf_.align_buf(buf, buf_len, aligned_buf,
      aligned_buf_len, aligned_block_offset))) {
    PALF_LOG(ERROR, "align_buf failed", K(ret), K(buf), K(buf_len),
        K(aligned_buf), K(aligned_buf_len), K(aligned_block_offset), K(offset));
  } else if (OB_FAIL(inner_write_impl_(io_fd_, aligned_buf, aligned_buf_len, aligned_block_offset))){
    PALF_LOG(ERROR, "pwrite failed", K(ret), K(io_fd_), K(aligned_buf), K(aligned_block_offset),
        K(offset), K(buf_len));
  } else {
    dio_aligned_buf_.truncate_buf();
    total_write_size_ += buf_len;
    total_write_size_after_dio_ += aligned_buf_len;
    count_++;
    if (palf_reach_time_interval(PALF_IO_STAT_PRINT_INTERVAL_US, trace_time_)) {
      const int64_t each_pwrite_cost = ob_pwrite_used_ts_ / count_;
      PALF_LOG(INFO, "[PALF STAT WRITE LOG INFO TO DISK]", K(ret), K(offset), KPC(this), K(aligned_buf_len),
          K(aligned_buf), K(aligned_block_offset), K(buf_len), K(total_write_size_),
          K(total_write_size_after_dio_), K_(ob_pwrite_used_ts), K_(count), K(each_pwrite_cost));
      total_write_size_ = total_write_size_after_dio_ = count_ = ob_pwrite_used_ts_ = 0;
    }
  }
  return ret;
}

int LogBlockHandler::inner_writev_once_(const offset_t offset,
    const LogWriteBuf &write_buf)
{
  int ret = OB_SUCCESS;
  int64_t write_size = 0;
  const int64_t write_buf_cnt = write_buf.get_buf_count();
  offset_t curr_write_offset = offset;
  for (int64_t i = 0; OB_SUCC(ret) && i < write_buf_cnt; i++) {
    const char *buf = NULL;
    int64_t buf_len = 0;
    if (OB_FAIL(write_buf.get_write_buf(i, buf, buf_len))) {
      PALF_LOG(ERROR, "LogWriteBuf get_write_buf failed", K(ret), K(i));
    } else if (OB_FAIL(inner_write_once_(curr_write_offset, buf, buf_len))) {
      PALF_LOG(ERROR, "inner_write_once_ failed", K(ret), K(offset));
    } else {
      // NB: Advance write offset
      curr_write_offset += buf_len;
    }
  }
  return ret;
}

int LogBlockHandler::inner_write_impl_(const int fd, const char *buf, const int64_t count, const int64_t offset)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::fast_current_time();
  int64_t write_size = 0;
  int64_t time_interval = OB_INVALID_TIMESTAMP;
  do {
    if (count != (write_size = ob_pwrite(fd, buf, count, offset))) {
      if (palf_reach_time_interval(1000 * 1000, time_interval)) {
        ret = convert_sys_errno();
        PALF_LOG(ERROR, "ob_pwrite failed", K(ret), K(fd), K(offset), K(count), K(errno));
      }
      ob_usleep(RETRY_INTERVAL);
    } else {
      ret = OB_SUCCESS;
      break;
    }
  } while (OB_FAIL(ret));
  int64_t cost_ts = ObTimeUtility::fast_current_time() - start_ts;
  EVENT_TENANT_INC(ObStatEventIds::PALF_WRITE_IO_COUNT, MTL_ID());
  EVENT_ADD(ObStatEventIds::PALF_WRITE_SIZE, count);
  EVENT_ADD(ObStatEventIds::PALF_WRITE_TIME, cost_ts);
  ob_pwrite_used_ts_ += cost_ts;
  return ret;
}
} // end of logservice
} // end of oceanbase
