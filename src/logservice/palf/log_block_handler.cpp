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
#include "log_io_adapter.h"                             // LogIOAdapter
namespace oceanbase
{
using namespace common;
using namespace share;
namespace palf
{
constexpr int64_t PRINT_LOG_INTERVAL = 5 * 1000 * 1000;
#define PALF_LOG_FREQUENT(level, info_string, args...) {if (TC_REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) OB_MOD_LOG(PALF, level, info_string, ##args);}

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
      mtl_malloc_align(LOG_DIO_ALIGN_SIZE, aligned_buf_size, "LogDIOAligned")))) {
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
    PALF_LOG(TRACE, "need align", K(ret));
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
  MEMSET(aligned_data_buf_, 0, aligned_buf_size_);
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

SwitchLogIOModeFunctor::SwitchLogIOModeFunctor(LogBlockHandler *block_handler)
  : block_handler_(block_handler)
{}

SwitchLogIOModeFunctor::~SwitchLogIOModeFunctor()
{
  block_handler_ = NULL;
}

int SwitchLogIOModeFunctor::operator()(ObIODevice *prev_io_device, ObIODevice *io_device, const int64_t align_size, const logservice::SwitchLogIOModeState &state)
{
  int ret = OB_SUCCESS;
  const bool with_reopen = false;
  ObSpinLockGuard guard(block_handler_->fd_lock_);
  ObIOFd io_fd = block_handler_->io_fd_;
  bool holder_by_io_adapter = check_fd_holder_by_io_adapter(io_fd.second_id_);
  if (NULL == block_handler_ || NULL == prev_io_device || NULL == io_device) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "unexpected error, block_handler_ or io_device is nullptr", KP(block_handler_), KP(prev_io_device), KP(io_device));
  } else if (logservice::SwitchLogIOModeState::CLOSING == state) {
      if (!io_fd.is_valid()) {
        PALF_LOG(INFO, "io_fd is invalid, do nothing", KPC(block_handler_));
      } else {
        if (OB_FAIL(block_handler_->inner_close_())) {
          PALF_LOG(WARN, "inner_close_ failed", KR(ret));
        } else {
          PALF_LOG(INFO, "close successfully", KPC(block_handler_));
        }
      }
  } else if (logservice::SwitchLogIOModeState::FSYNCING == state) {
    char *ptr = block_handler_->curr_block_path_;
    if (0 == strlen(ptr)) {
      PALF_LOG(INFO, "curr_block_path_ is nullptr do nothing", KPC(block_handler_));
    } else if (OB_FAIL(fsync(ptr, prev_io_device))) {
      PALF_LOG(WARN, "fsync_with_retry failed", KPC(block_handler_));
    } else {
      PALF_LOG(INFO, "fsync_with_retry successfully", KPC(block_handler_));
    }
  } else if (logservice::SwitchLogIOModeState::OPENING == state) {
    ObIOFd tmp_io_fd;
    char *ptr = block_handler_->curr_block_path_;
    block_handler_->dio_aligned_buf_.align_size_ = align_size;
    if (0 == strlen(ptr)) {
      PALF_LOG(INFO, "curr_block_path_ is nullptr do nothing", KPC(block_handler_));
    } else {
      if (OB_FAIL(io_device->open(ptr, LOG_WRITE_FLAG, FILE_OPEN_MODE, tmp_io_fd))) {
        PALF_LOG(WARN, "open failed", KR(ret), KP(io_device));
      } else if (FALSE_IT(tmp_io_fd.device_handle_ = io_device)) {
      } else if (block_handler_->io_fd_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(ERROR, "io_fd of LogBlockHandler is valid", KPC(block_handler_), K(tmp_io_fd));
      } else if (OB_FAIL(block_handler_->inner_load_data_once_(block_handler_->curr_write_offset_, tmp_io_fd))) {
        PALF_LOG(WARN, "inner_load_data_once_ failed", KR(ret), KP(io_device));
      } else if (FALSE_IT(block_handler_->io_fd_ = tmp_io_fd)) {
      } else {
      }
      if (OB_FAIL(ret)) {
        USLEEP(LogBlockHandler::RETRY_INTERVAL);
        io_device->close(tmp_io_fd);
      }
    }
  }
  return ret;
}

LogBlockHandler::LogBlockHandler()
  : dio_aligned_buf_(),
    log_block_size_(0),
    total_write_size_(0),
    total_write_size_after_dio_(0),
    ob_pwrite_used_ts_(0),
    count_(0),
    trace_time_(OB_INVALID_TIMESTAMP),
    curr_write_offset_(0),
    io_fd_(),
    io_adapter_(NULL),
    last_pwrite_start_time_us_(OB_INVALID_TIMESTAMP),
    last_pwrite_size_(-1),
    accum_write_size_(0),
    accum_write_rt_(0),
    accum_write_count_(0),
    sync_io_(false),
    is_inited_(false)
{
  memset(log_dir_, 0, OB_MAX_FILE_NAME_LENGTH);
}

LogBlockHandler::~LogBlockHandler()
{
  destroy();
}

int LogBlockHandler::init(const char *log_dir,
                          const int64_t log_block_size,
                          const int64_t align_size,
                          const int64_t align_buf_size,
                          LogIOAdapter *io_adapter)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
		ret = OB_INIT_TWICE;
	} else if ((0 != align_size && 0 != (log_block_size & (align_size - 1))) || OB_ISNULL(io_adapter)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid argument", K(ret), K(log_block_size), KP(io_adapter), K(align_size));
  } else if (OB_FAIL(dio_aligned_buf_.init(align_size, align_buf_size))) {
    PALF_LOG(ERROR, "init dio_aligned_buf_ failed", K(ret));
  } else if (OB_FAIL(register_to_io_adapter_(log_dir, io_adapter))) {
    PALF_LOG(WARN, "register_to_io_adapter_ failed", K(ret), K(log_dir));
  } else {
    memcpy(log_dir_, log_dir, strlen(log_dir));
    log_block_size_ = log_block_size;
    io_adapter_ = io_adapter;
    memset(curr_block_path_, 0, OB_MAX_FILE_NAME_LENGTH);
    is_inited_ = true;
    PALF_LOG(INFO, "LogBlockHandler init success", K(ret), K(log_block_size_), K(align_size), K(align_buf_size));
  }
  return ret;
}

void LogBlockHandler::destroy()
{
  is_inited_ = false;
  curr_write_offset_ = 0;
  PALF_LOG(INFO, "LogBlockHandler destroy", KPC(this));
  if (io_adapter_ != NULL) {
    (void)unregister_to_io_adapter_(log_dir_, io_adapter_);
  }
  {
    ObSpinLockGuard lock(fd_lock_);
    if (io_fd_.is_valid() && io_adapter_ != NULL) {
      inner_fsync_();
      inner_close_();
      io_fd_.reset();
      memset(curr_block_path_, 0, OB_MAX_FILE_NAME_LENGTH);
    }
  }
  log_block_size_ = 0;
  dio_aligned_buf_.destroy();
  memset(log_dir_, 0, OB_MAX_FILE_NAME_LENGTH);
  io_adapter_ = NULL;
}

int LogBlockHandler::switch_next_block(const char *block_path)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(fd_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(inner_close_())) {
    PALF_LOG(ERROR, "inner_close_", K(ret), K(block_path));
  } else if (OB_FAIL(inner_open_(block_path))) {
    PALF_LOG(WARN, "inner_open_ failed", K(ret));
  } else if (FALSE_IT(dio_aligned_buf_.reset_buf())) {
  } else {
    memset(curr_block_path_, 0, OB_MAX_FILE_NAME_LENGTH);
    STRNCPY(curr_block_path_, block_path, strlen(block_path));
    curr_write_offset_ = 0;
    PALF_LOG(TRACE, "switch_next_block success", K(ret), KPC(this));
  }
  return ret;
}

int LogBlockHandler::open(const char *block_path)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(fd_lock_);
  if (OB_FAIL(inner_open_(block_path))) {
    PALF_LOG_FREQUENT(ERROR, "inner open block failed", K(ret), K(block_path));
  } else if (FALSE_IT(dio_aligned_buf_.reset_buf())) {
  } else {
    memset(curr_block_path_, 0, OB_MAX_FILE_NAME_LENGTH);
    STRNCPY(curr_block_path_, block_path, strlen(block_path));
    curr_write_offset_ = 0;
    PALF_LOG(INFO, "open block success", K(ret), KPC(this));
  }
  return ret;
}

int LogBlockHandler::close_with_fsync()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(fd_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!io_fd_.is_valid()) {
    memset(curr_block_path_, 0, OB_MAX_FILE_NAME_LENGTH);
    PALF_LOG(WARN, "io_fd_ is invalid, do nothing", K(ret), KPC(this));
  } else if (OB_FAIL(inner_fsync_())) {
    PALF_LOG(WARN, "inner_fsync_ failed", K(ret), KPC(this));
  } else if (OB_FAIL(inner_close_())) {
    PALF_LOG(WARN, "close current block failed", K(ret), KPC(this));
  } else {
    memset(curr_block_path_, 0, OB_MAX_FILE_NAME_LENGTH);
    PALF_LOG(INFO, "LogFileHandler close success", K(ret), KPC(this));
  }
  return ret;
}

int LogBlockHandler::truncate(const offset_t offset)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(fd_lock_);
  if (OB_FAIL(inner_truncate_(offset))) {
    PALF_LOG(WARN, "inner_truncate_ failed", K(ret), K(offset), KPC(this));
  } else {
    curr_write_offset_ = offset;
    PALF_LOG(INFO, "LogBlockHandler truncate success", K(ret), K(offset), KPC(this));
  }
  return ret;
}

int LogBlockHandler::load_data(const offset_t offset)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(fd_lock_);
  if (OB_FAIL(inner_load_data_(offset))) {
    PALF_LOG(WARN, "inner_load_data_ failed", K(ret), K(offset), KPC(this));
  } else {
    curr_write_offset_ = offset;
    PALF_LOG(INFO, "LogBlockHandler load_data success", K(ret), K(offset), KPC(this));
  }
  return ret;
}

int LogBlockHandler::pwrite(const offset_t offset,
                            const char *buf,
                            const int64_t buf_len)
{
  int ret = OB_SUCCESS;

  ObSpinLockGuard guard(fd_lock_);
  if (OB_FAIL(inner_write_once_(offset, buf, buf_len))) {
    PALF_LOG(ERROR, "inner_write_once_ failed", K(ret));
  } else {
    curr_write_offset_ = offset+buf_len;
    PALF_LOG(TRACE, "pwrite success", K(ret), K(offset), KPC(this));
  }
  return ret;
}

int LogBlockHandler::writev(const offset_t offset,
                            const LogWriteBuf &write_buf)
{
  int ret = OB_SUCCESS;

  ObSpinLockGuard guard(fd_lock_);
  if (curr_write_offset_ != offset) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "write offset is not continous, unexpected error", KR(ret), K(offset), KPC(this));
  } else if (OB_FAIL(inner_writev_once_(offset, write_buf))) {
    PALF_LOG(ERROR, "inner_write_once_ failed", K(ret), K(offset), K(write_buf));
  } else {
    curr_write_offset_ = offset + write_buf.get_total_size();
    PALF_LOG(TRACE, "writev success", K(ret), K(offset), K(write_buf), KPC(this));
  }
  return ret;
}

int LogBlockHandler::inner_close_()
{
  int ret = OB_SUCCESS;
  do {
    ObIOFd tmp_io_fd = io_fd_;
    if (!io_fd_.is_valid()) {
      PALF_LOG(INFO, "block has been closed or not eixst", K(ret));
    } else if (OB_FAIL(io_adapter_->close(io_fd_))) {
      PALF_LOG_FREQUENT(ERROR, "close block failed", K(ret), K(errno), KPC(this));
      ob_usleep(RETRY_INTERVAL);
    } else {
      PALF_LOG(TRACE, "close block success", K(ret), KPC(this), K(tmp_io_fd));
      io_fd_.reset();
      // NB: inner_close_ must not reset curr_block_path_ because it's used for open block again when LogStore has been restart.
      // memset(curr_block_path_, 0, OB_MAX_FILE_NAME_LENGTH)
    }
  } while (OB_FAIL(ret));
  return ret;
}

int LogBlockHandler::inner_open_(const char *block_path)
{
  int ret = OB_SUCCESS;
  if (io_fd_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "unexpected error", KR(ret), KPC(this), K(block_path));
  } else {
    do {
      ObIOFd io_fd;
      fd_lock_.unlock();
      if (OB_FAIL(io_adapter_->open(block_path, LOG_WRITE_FLAG, FILE_OPEN_MODE, io_fd))) {
        PALF_LOG_FREQUENT(WARN, "open failed", KR(ret), KPC(this), K(block_path));
        ob_usleep(RETRY_INTERVAL);
      }
      fd_lock_.lock();
      if (io_fd.is_valid()) {
        if (!io_fd_.is_valid()) {
          io_fd_ = io_fd;
          io_fd.reset();
        } else if (OB_ISNULL(io_fd_.device_handle_) || check_fd_holder_by_io_adapter(io_fd_.second_id_)) {
          ret = OB_ERR_UNDEFINED;
          PALF_LOG(WARN, "unexpected error, device_handle_ is NULL or hodler is io_adapter", KR(ret), KPC(this), K(block_path));
        } else {
          PALF_LOG(INFO, "others has opened block successfully, need reopen", KPC(this), K(block_path));
          ObIOFd tmp_fd = io_fd_;
          io_fd_.device_handle_->close(tmp_fd);
          io_fd_.reset();
          ret = OB_EAGAIN;
        }
      }
      if (OB_FAIL(ret)) {
      } else {
        PALF_LOG(INFO, "inner_open_ success", KPC(this), K(block_path));
      }
      if (io_fd.is_valid()) {
        (void)io_adapter_->close(io_fd);
      }
    } while (OB_FAIL(ret));
  }
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
    ObIOFd io_fd;
    if (FALSE_IT(get_io_fd_(io_fd))) {
    } else if (OB_FAIL(io_adapter_->truncate(io_fd, offset))) {
      PALF_LOG_FREQUENT(ERROR, "ftruncate first phase failed", K(ret), KPC(this), K(offset));
      ob_usleep(RETRY_INTERVAL);
    } else if (OB_FAIL(io_adapter_->truncate(io_fd, log_block_size_))) {
      PALF_LOG(ERROR, "ftruncate second phase failed", K(ret), KPC(this), K(log_block_size_));
      ob_usleep(RETRY_INTERVAL);
    } else {
      dio_aligned_buf_.reset_buf();
      ret = OB_SUCCESS;
      PALF_LOG(INFO, "inner_truncate_ success", K(ret), K(offset));
      break;
    }
    if (OB_LOG_STORE_EPOCH_CHANGED == ret) {
      int tmp_ret = OB_SUCCESS;
      if(OB_TMP_FAIL(inner_reopen_(curr_block_path_, offset))) {
        PALF_LOG(WARN, "inner_reopen_ failed", KR(tmp_ret), KPC(this), K(offset));
      }
    }
  } while (OB_FAIL(ret));
  return ret;
}

int LogBlockHandler::inner_load_data_once_(const offset_t offset,
                                           const ObIOFd &io_fd)
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
  int64_t aligned_out_read_count = 0;
  if (OB_ISNULL(input = reinterpret_cast<char *>(
      mtl_malloc_align(LOG_DIO_ALIGN_SIZE, aligned_read_count, "LogDIOAligned")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(WARN, "allocate memory failed", K(ret));
  } else if (0 != aligned_read_count
             && OB_FAIL(read_until_success(io_fd, input, aligned_read_count, aligned_offset, aligned_out_read_count))) {
    PALF_LOG(WARN, "ob_pread failed", K(ret), K(offset), K(read_count), K(aligned_read_count),
             K(aligned_offset), K(dio_aligned_buf_), K(io_fd), KPC(this));
  } else if (FALSE_IT(dio_aligned_buf_.reset_buf())) {
  } else if (0 != aligned_out_read_count && OB_FAIL(dio_aligned_buf_.align_buf(
          input, read_count, output, output_len, out_offset))) {
    PALF_LOG(WARN, "align_buf failed", K(ret), K(dio_aligned_buf_));
  } else {
    dio_aligned_buf_.truncate_buf();
    PALF_LOG(INFO, "inner_load_data_once_ success", K(ret), K(offset), K(read_count), K(dio_aligned_buf_),
        K(aligned_out_read_count));
  }
  if (NULL != input) {
    mtl_free_align(input);
  }
  return ret;
}

int LogBlockHandler::inner_load_data_(const offset_t offset)
{
  int ret = OB_SUCCESS;
  do {
    ObIOFd io_fd;
    if (FALSE_IT(get_io_fd_(io_fd))) {
    } else if (OB_FAIL(inner_load_data_once_(offset, io_fd))) {
      PALF_LOG(WARN, "inner_load_data_once_ failed", K(ret), K(offset), KPC(this));
      ob_usleep(RETRY_INTERVAL);
    } else {
      PALF_LOG(INFO, "LogBlockHandler load_data success", K(ret), K(offset), KPC(this));
    }
    if (OB_LOG_STORE_EPOCH_CHANGED == ret) {
      int tmp_ret = OB_SUCCESS;
      if(OB_TMP_FAIL(inner_reopen_(curr_block_path_, offset))) {
        PALF_LOG(WARN, "inner_reopen_ failed", KR(tmp_ret), KPC(this), K(offset));
      }
    }
  } while (OB_FAIL(ret));
  return ret;
}

int LogBlockHandler::inner_write_once_(const offset_t offset,
    const char *buf,
    const int64_t buf_len)
{
  int ret = OB_SUCCESS;

  int64_t start_ts = ObTimeUtility::fast_current_time();
  if (OB_FAIL(inner_write_impl_(buf, buf_len, offset))){
    PALF_LOG(ERROR, "inner_write_impl_ failed", K(ret), K(offset));
  } else {
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
    } else if (OB_FAIL(inner_write_impl_(buf, buf_len, curr_write_offset))) {
      PALF_LOG(ERROR, "inner_write_once_ failed", K(ret), K(offset));
    } else {
      // NB: Advance write offset
      curr_write_offset += buf_len;
    }
  }
  return ret;
}

int LogBlockHandler::inner_write_impl_(const char *buf,
                                       const int64_t buf_len,
                                       const int64_t offset)
{
  int ret = OB_SUCCESS;
  int64_t write_size = 0;
  int64_t time_interval = OB_INVALID_TIMESTAMP;
  int64_t start_ts = ObTimeUtility::fast_current_time();
  ATOMIC_STORE(&last_pwrite_start_time_us_, start_ts);
  ATOMIC_STORE(&last_pwrite_size_, buf_len);
  do {
    ObIOFd io_fd;
    char *aligned_buf = const_cast<char *>(buf);
    int64_t aligned_buf_len = buf_len;
    offset_t aligned_block_offset = offset;
    if (FALSE_IT(get_io_fd_(io_fd))) {
    } else if (OB_FAIL(dio_aligned_buf_.align_buf(buf, buf_len, aligned_buf,
        aligned_buf_len, aligned_block_offset))) {
      PALF_LOG(ERROR, "align_buf failed", K(ret), K(buf), K(buf_len),
          K(aligned_buf), K(aligned_buf_len), K(aligned_block_offset), K(offset));
    } else if (OB_FAIL(io_adapter_->pwrite(io_fd, aligned_buf, aligned_buf_len, aligned_block_offset, write_size))) {
      if (palf_reach_time_interval(1000 * 1000, time_interval)) {
        PALF_LOG(ERROR, "io_adapter pwrite failed", K(ret), K(io_fd), K(aligned_block_offset), K(aligned_buf_len), K(write_size), KP(aligned_buf), KPC(this), KP(buf));
        LOG_DBA_ERROR_V2(OB_LOG_PWRITE_FAIL, ret, "ob_pwrite failed, please check the output of dmesg");
      }
      ob_usleep(RETRY_INTERVAL);
    } else {
      PALF_LOG(TRACE, "pwrite successfully", K(io_fd), K(offset), K(aligned_buf_len), K(write_size));
    }
    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if(OB_TMP_FAIL(inner_reopen_(curr_block_path_, offset))) {
        PALF_LOG(WARN, "inner_reopen_ failed", KR(tmp_ret), KPC(this), K(offset));
      } else {
      }
    }
    if (OB_SUCC(ret)) {
      int64_t cost_ts = ObTimeUtility::fast_current_time() - start_ts;
      EVENT_TENANT_INC(ObStatEventIds::PALF_WRITE_IO_COUNT, MTL_ID());
      EVENT_ADD(ObStatEventIds::PALF_WRITE_SIZE, write_size);
      EVENT_ADD(ObStatEventIds::PALF_WRITE_TIME, cost_ts);
      ob_pwrite_used_ts_ += cost_ts;
      ATOMIC_STORE(&last_pwrite_start_time_us_, OB_INVALID_TIMESTAMP);
      ATOMIC_STORE(&last_pwrite_size_, -1);
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
      // === IO Statistic ===
      accum_write_size_ += aligned_buf_len;
      accum_write_count_++;
      accum_write_rt_ += cost_ts;
    }
  } while (OB_FAIL(ret));
  return ret;
}

int LogBlockHandler::get_io_statistic_info(int64_t &last_working_time,
                                           int64_t &last_write_size,
                                           int64_t &accum_write_size,
                                           int64_t &accum_write_count,
                                           int64_t &accum_write_rt) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    last_working_time = ATOMIC_LOAD(&last_pwrite_start_time_us_);
    last_write_size =  ATOMIC_LOAD(&last_pwrite_size_);
    if (OB_INVALID_TIMESTAMP == last_working_time || -1 == last_write_size) {
      last_working_time = OB_INVALID_TIMESTAMP;
      last_write_size = -1;
    }
    accum_write_size = accum_write_size_;
    accum_write_count = accum_write_count_;
    accum_write_rt = accum_write_rt_;
  }
  return ret;
}

int LogBlockHandler::rename_tmp_block_handler_to_normal(const char *block_path)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(fd_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(inner_close_())) {
    PALF_LOG(WARN, "inner_close_ failed", KR(ret), KPC(this));
  } else if (OB_FAIL(inner_open_(block_path))) {
    PALF_LOG(WARN, "inner_open_ failed", KR(ret), KPC(this));
  } else {
    memset(curr_block_path_, 0, OB_MAX_FILE_NAME_LENGTH);
    strncpy(curr_block_path_, block_path, strlen(block_path));
    PALF_LOG(INFO, "rename_tmp_block_handler_to_normal success", KPC(this), K(block_path));
  }
  return ret;
}

int LogBlockHandler::fsync()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(fd_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(inner_fsync_())) {
    PALF_LOG(WARN, "inner_fsync_ failed", KR(ret), KPC(this));
  } else {
  }
  return ret;
}

int LogBlockHandler::inner_reopen_(const char *block_path, const int64_t offset)
{
  int ret = OB_SUCCESS;
  do {
    ObIOFd io_fd;
    if (OB_FAIL(inner_close_())) {
      PALF_LOG_FREQUENT(ERROR, "inner_close_ failed", KR(ret));
      ob_usleep(RETRY_INTERVAL);
    } else if (OB_FAIL(inner_open_(block_path))) {
      PALF_LOG(ERROR, "inner_open_ failed", KR(ret), KPC(this), K(block_path));
      ob_usleep(RETRY_INTERVAL);
    } else if (FALSE_IT(get_io_fd_(io_fd))) {
    } else if (FALSE_IT(dio_aligned_buf_.reset_buf())) {
    } else if (OB_FAIL(inner_load_data_once_(offset, io_fd))) {
      PALF_LOG(ERROR, "inner_load_data_once_ failed", KR(ret), KPC(this), K(block_path));
      ob_usleep(RETRY_INTERVAL);
    } else {
      PALF_LOG(INFO, "inner_reopen_ success", KPC(this), K(block_path), K(offset));
    }
  } while (OB_FAIL(ret));
  return ret;
}

int LogBlockHandler::inner_fsync_(const bool with_reopen)
{
  int ret = OB_SUCCESS;
  do {
    ObIOFd io_fd;
    if (FALSE_IT(get_io_fd_(io_fd))) {
    } else if (OB_FAIL(io_adapter_->fsync(io_fd))) {
      PALF_LOG_FREQUENT(WARN, "fsync failed", KR(ret), KPC(this));
    } else {
      PALF_LOG(INFO, "fsync success", KR(ret), KPC(this));
    }
    if (OB_LOG_STORE_EPOCH_CHANGED == ret && !with_reopen) {
      ret = OB_SUCCESS;
    }
    if (OB_LOG_STORE_EPOCH_CHANGED == ret) {
      int tmp_ret = OB_SUCCESS;
      if(OB_TMP_FAIL(inner_reopen_(curr_block_path_, 0))) {
        PALF_LOG(WARN, "inner_reopen_ failed", KR(tmp_ret), KPC(this));
      }
    }
  } while (OB_LOG_STORE_EPOCH_CHANGED == ret);
  return ret;
}

int LogBlockHandler::set_log_store_sync_mode(const LogSyncMode &mode)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogBlockHandler not inited", KR(ret), K(mode), KPC(this));
  } else if (!is_valid_log_sync_mode(mode)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), K(mode), KPC(this));
  } else {
    ATOMIC_STORE(&sync_io_, (LogSyncMode::SYNC == mode ? true : false));
    PALF_LOG(INFO, "set_log_store_sync_mode", KR(ret), K(mode), KPC(this));
  }
  return ret;
}

int64_t LogBlockHandler::get_curr_write_offset() const
{
  return curr_write_offset_;
}

void LogBlockHandler::get_io_fd_(ObIOFd &io_fd)
{
  do {
    io_fd = io_fd_;
    if (!io_fd.is_valid()) {
      fd_lock_.unlock();
      PALF_LOG_RET(WARN, OB_ERR_UNEXPECTED, "block has been closed by others, need wait", KPC(this), K(lbt()));
      ob_usleep(RETRY_INTERVAL);
      fd_lock_.lock();
    }
  } while (!io_fd.is_valid());
  if (ATOMIC_LOAD(&sync_io_)) {
    set_fd_in_sync_mode(io_fd.second_id_);
  }
  PALF_LOG(TRACE, "get_io_fd_ success", K(io_fd), KPC(this));
}

int LogBlockHandler::register_to_io_adapter_(const char *log_dir,
                                             LogIOAdapter *adapter)
{
  int ret = OB_SUCCESS;
  SwitchLogIOModeFunctor functor(this);
  ObFunction<int(ObIODevice *,ObIODevice *, const int64_t, const logservice::SwitchLogIOModeState&)> cb(functor);
  if (!cb.is_valid()) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(WARN, "ObFunction alloc memory failed", KR(ret), K(log_dir));
  } else if (OB_FAIL(adapter->register_cb(log_dir, cb))) {
    PALF_LOG(WARN, "register_cb failed", KR(ret), K(log_dir));
  } else {
    PALF_LOG(INFO, "register_cb success", K(log_dir));
  }
  return ret;
}

int LogBlockHandler::unregister_to_io_adapter_(const char *log_dir,
                                               LogIOAdapter *adapter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(adapter->unregister_cb(log_dir))) {
    PALF_LOG(WARN, "unregister_cb failed", KR(ret), K(log_dir));
  } else {
    PALF_LOG(INFO, "unregister_cb success", K(log_dir), KPC(this));
  }
  return ret;
}
} // end of logservice
} // end of oceanbase
