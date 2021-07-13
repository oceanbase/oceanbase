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

#include "ob_log_direct_reader.h"
#include "lib/file/ob_file.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "ob_log_file_trailer.h"
#include "lib/thread_local/ob_tsi_factory.h"
#include "share/redolog/ob_log_store_factory.h"
#include "share/redolog/ob_log_file_reader.h"
#include "ob_log_file_trailer.h"
#include "ob_log_compress.h"
#include "ob_file_id_cache.h"
#include "storage/blocksstable/ob_store_file_system.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
using namespace common;
using namespace share;
namespace clog {
ObAlignedBuffer::ObAlignedBuffer() : align_buf_(NULL), size_(0), align_size_(0), is_inited_(false)
{}

ObAlignedBuffer::~ObAlignedBuffer()
{
  destroy();
}

// alloc a specified size of space and align it
// to align_size.
int ObAlignedBuffer::init(const int64_t size, const int64_t align_size, const char* label)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(0 != (align_size & (align_size - 1)))) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(align_size));
  } else if (OB_UNLIKELY(NULL == (align_buf_ = static_cast<char*>(ob_malloc_align(align_size, size, label))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(ERROR, "ob_malloc fail", K(ret), K(align_size), K(size));
  } else {
    size_ = size;
    align_size_ = align_size;
    is_inited_ = true;
  }
  return ret;
}

void ObAlignedBuffer::destroy()
{
  if (NULL != align_buf_) {
    ob_free_align(align_buf_);
    align_buf_ = NULL;
  }
  size_ = 0;
  align_size_ = 0;
  is_inited_ = false;
}

void ObAlignedBuffer::reuse()
{
  if (NULL != align_buf_) {
    memset(align_buf_, 0, size_);
  }
}

ObLogDirectReader::ObLogDirectReader()
    : is_inited_(false),
      use_cache_(false),
      file_store_(NULL),
      log_cache_(NULL),
      tail_(NULL),
      log_ctrl_(NULL),
      log_buffer_(NULL),
      cache_miss_count_(0),
      cache_hit_count_(0)
{}

ObLogDirectReader::~ObLogDirectReader()
{
  destroy();
}

int ObLogDirectReader::init(const char* log_dir, const char* shm_buf, const bool use_cache, ObLogCache* log_cache,
    ObTailCursor* tail, ObLogWritePoolType type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(log_dir) || OB_UNLIKELY(use_cache && NULL == log_cache) || OB_UNLIKELY(NULL == tail)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "init fail", K(ret), KP(log_dir), K(use_cache), KP(log_cache), KP(tail));
  } else if (nullptr != shm_buf && OB_FAIL(ObBaseLogBufferMgr::get_instance().get_buffer(shm_buf, log_ctrl_))) {
    CLOG_LOG(WARN, "Fail to get log buffer, ", K(ret));
  } else if (NULL == (file_store_ = ObLogStoreFactory::create(log_dir, CLOG_FILE_SIZE, type))) {
    ret = OB_INIT_FAIL;
    CLOG_LOG(WARN, "create file store failed.", K(ret));
  } else {
    log_buffer_ = nullptr != log_ctrl_ ? log_ctrl_->base_buf_ : nullptr;
    log_cache_ = log_cache;
    tail_ = tail;
    cache_miss_count_ = 0;
    cache_hit_count_ = 0;
    use_cache_ = use_cache;
    is_inited_ = true;
  }
  return ret;
}

int ObLogDirectReader::get_file_id_range(uint32_t& min_file_id, uint32_t& max_file_id) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogDirectReader is not inited", K(ret));
  } else if (OB_FAIL(file_store_->get_file_id_range(min_file_id, max_file_id))) {
    CLOG_LOG(WARN, "Fail to get file id range", K(ret));
  }
  return ret;
}

void ObLogDirectReader::destroy()
{
  is_inited_ = false;
  use_cache_ = false;
  ObLogStoreFactory::destroy(file_store_);
  log_cache_ = NULL;
  log_ctrl_ = NULL;
  log_buffer_ = NULL;
  cache_miss_count_ = 0;
  cache_hit_count_ = 0;
}

offset_t ObLogDirectReader::limit_by_file_size(const int64_t offset) const
{
  int ret = OB_SUCCESS;
  offset_t offset_ret = OB_INVALID_OFFSET;
  ObLogWritePoolType file_type = INVALID_WRITE_POOL;
  if (OB_FAIL(get_log_file_type(file_type))) {
    CLOG_LOG(ERROR, "get_log_file_type failed", K(ret), K(offset));
  } else {
    if (CLOG_WRITE_POOL == file_type) {
      offset_ret = static_cast<offset_t>(CLOG_FILE_SIZE < offset ? CLOG_FILE_SIZE : offset);
    } else if (ILOG_WRITE_POOL == file_type) {
      offset_ret = static_cast<offset_t>(offset);
    } else {
      CLOG_LOG(ERROR, "not support file type", K(file_type));
    }
  }
  return offset_ret;
}

// return OB_SUCCESS / OB_READ_NOTHING
int ObLogDirectReader::limit_param_by_tail(const ObReadParam& want_param, ObReadParam& real_param)
{
  int ret = OB_SUCCESS;
  real_param.reset();
  if (OB_ISNULL(tail_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(!tail_->is_valid())) {
    // when just started, tail_ is invalid
    real_param.shallow_copy(want_param);
  } else {
    ObTailCursor tail_cursor = tail_->get();
    const file_id_t f_want = want_param.file_id_;
    const offset_t o_want = want_param.offset_;
    const int64_t len = want_param.read_len_;
    const file_id_t f_tail = tail_cursor.file_id_;
    const offset_t o_tail = tail_cursor.offset_;
    if (cursor_cmp(f_tail, o_tail, f_want, limit_by_file_size(o_want + len)) >= 0) {
      // can read all data wanted (except limited by file_size)
      real_param.file_id_ = f_want;
      real_param.offset_ = o_want;
      real_param.read_len_ = limit_by_file_size(o_want + len) - o_want;
      real_param.timeout_ = want_param.timeout_;
      CLOG_LOG(DEBUG, "read is not limited by tail", K(ret), K(f_want), K(o_want), K(f_tail), K(o_tail));
    } else if (cursor_cmp(f_tail, o_tail, f_want, o_want) > 0) {
      // can read part of wanted data
      real_param.file_id_ = f_want;
      real_param.offset_ = o_want;
      real_param.read_len_ = o_tail - o_want;
      real_param.timeout_ = want_param.timeout_;
      CLOG_LOG(DEBUG, "read is limited by tail", K(ret), K(f_want), K(o_want), K(f_tail), K(o_tail));
    } else if (cursor_cmp(f_tail, o_tail, f_want, o_want) == 0) {
      // trying to read at tail
      ret = OB_READ_NOTHING;
      CLOG_LOG(DEBUG, "trying to read at tail", K(ret), K(f_want), K(o_want), K(f_tail), K(o_tail));
    } else {
      // read beyond tail
      ret = OB_READ_NOTHING;
      CLOG_LOG(DEBUG, "try read beyond tail", K(ret), K(want_param), K(f_tail), K(o_tail));
    }
  }
  return ret;
}

// align param for direct io
void ObLogDirectReader::align_param_for_dio_without_cache(
    const ObReadParam& param, ObReadParam& align_param, int64_t& backoff)
{
  align_param.timeout_ = param.timeout_;
  align_param.file_id_ = param.file_id_;
  // align offset to CLOG_DIO_ALIGN_SIZE
  align_param.offset_ = static_cast<offset_t>(lower_align((int64_t)param.offset_, CLOG_DIO_ALIGN_SIZE));
  backoff = param.offset_ - align_param.offset_;
  align_param.read_len_ = upper_align((param.read_len_ + backoff), CLOG_DIO_ALIGN_SIZE);
  // check align read_len
  if (align_param.read_len_ > (OB_MAX_LOG_BUFFER_SIZE + CLOG_DIO_ALIGN_SIZE)) {
    CLOG_LOG(ERROR, "align_param.read_len_ is too large, unexpected", K(param), K(align_param), K(backoff));
    // limit align_param.read_len_ to (OB_MAX_LOG_BUFFER_SIZE + CLOG_DIO_ALIGN_SIZE)
    align_param.read_len_ = OB_MAX_LOG_BUFFER_SIZE + CLOG_DIO_ALIGN_SIZE;
  }
}

int ObLogDirectReader::read_data_from_file(
    const ObReadParam& aligned_param, ObReadBuf& rbuf, ObReadRes& read_file_res, ObReadCost& read_cost)
{
  UNUSED(read_cost);
  int ret = OB_SUCCESS;
  int64_t read_count = -1;
  ObLogReadFdHandle fd_handle;
  if (OB_UNLIKELY(!rbuf.is_valid()) || OB_UNLIKELY(!rbuf.is_aligned())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid rbuf for direct io", K(ret), K(rbuf), K(aligned_param));
  }
  // check rbuf is enough to store data
  else if (OB_UNLIKELY(rbuf.buf_len_ < aligned_param.read_len_)) {
    ret = OB_BUF_NOT_ENOUGH;
    CLOG_LOG(WARN, "buffer not enough", K(ret), K(rbuf), K(aligned_param));
  } else {
    CriticalGuard(get_log_file_qs());
    if (OB_FAIL(OB_LOG_FILE_READER.get_fd(file_store_->get_dir_name(), aligned_param.file_id_, fd_handle))) {
      ret = handle_no_file(aligned_param.file_id_);
    } else if (OB_FAIL(read_from_io(aligned_param.file_id_,
                   fd_handle,
                   rbuf.buf_,
                   aligned_param.read_len_,
                   aligned_param.offset_,
                   read_count))) {
      CLOG_LOG(WARN, "Fail to read from io, ", K(ret), K(aligned_param), K(rbuf), K(read_file_res));
    } else if (OB_UNLIKELY(read_count < 0)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "unexpected read count", K(ret), K(aligned_param), K(rbuf), K(read_count));
    } else {
      read_file_res.buf_ = rbuf.buf_;
      read_file_res.data_len_ = read_count;
    }
  }
  return ret;
}

void ObLogDirectReader::stat_cache_hit_rate()
{
  // Print cache hit rate every 10 seconds
  if (REACH_TIME_INTERVAL(STAT_TIME_INTERVAL)) {
    const int64_t hit_count = ATOMIC_LOAD(&cache_hit_count_);
    const int64_t miss_count = ATOMIC_LOAD(&cache_miss_count_);
    const int64_t total_count = hit_count + miss_count;
    double hit_rate = 0.0;
    if (total_count > 0) {
      hit_rate = static_cast<double>(hit_count) / static_cast<double>(total_count);
    }
    const char* dir_name = NULL;
    if (NULL != file_store_) {
      dir_name = file_store_->get_dir_name();
    }
    CLOG_LOG(INFO, "clog cache hit rate", K(dir_name), K(hit_count), K(miss_count), K(total_count), K(hit_rate));
    ATOMIC_STORE(&cache_hit_count_, 0);
    ATOMIC_STORE(&cache_miss_count_, 0);
  }
}

int ObLogDirectReader::read_data_from_hot_cache(const common::ObAddr& addr, const int64_t seq,
    const file_id_t want_file_id, const offset_t want_offset, const int64_t want_size, char* user_buf)
{
  int ret = OB_SUCCESS;
  if (!use_cache_) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "reader do not use cache", K(use_cache_), KP(log_cache_), KR(ret));
  } else if (OB_ISNULL(log_cache_)) {
    CLOG_LOG(ERROR, "invalid log cache", K(log_cache_));
    ret = OB_INVALID_ARGUMENT;
  } else {
    UNUSED(seq);
    // only local logs will enter the hot cache
    ret = log_cache_->hot_read(addr, want_file_id, want_offset, want_size, user_buf);
  }
  if (OB_NOT_INIT == ret) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObLogDirectReader::read_uncompressed_data_from_hot_cache(const common::ObAddr& addr, const int64_t seq,
    const file_id_t want_file_id, const offset_t want_offset, const int64_t want_size, char* user_buf,
    const int64_t buf_size, int64_t& origin_size)
{
  int ret = OB_SUCCESS;
  ObCompressedLogEntryHeader header;
  const int64_t PROBE_BUF_SIZE = 16;
  char probe_buf[PROBE_BUF_SIZE] = {0};
  // probe_size include three parts: size of magic, size before compress, size after compress
  const int64_t probe_size = sizeof(int16_t) + sizeof(int32_t) + sizeof(int32_t);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogDirectReader not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid_file_id(want_file_id)) || OB_UNLIKELY(!is_valid_offset(want_offset)) ||
             OB_UNLIKELY(want_size <= 0) || OB_UNLIKELY(want_size > OB_MAX_LOG_BUFFER_SIZE) ||
             OB_UNLIKELY(NULL == user_buf) || OB_UNLIKELY(buf_size < want_size) ||
             OB_UNLIKELY(want_size < probe_size)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "failed to read data from hot cache",
        K(want_file_id),
        K(want_offset),
        K(want_size),
        KP(user_buf),
        K(buf_size),
        K(probe_size),
        K(ret));
  } else if (OB_FAIL(read_data_from_hot_cache(addr, seq, want_file_id, want_offset, probe_size, probe_buf))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      CLOG_LOG(WARN, "failed to read data from hot cache", K(want_file_id), K(want_offset), K(probe_size), K(ret));
    }
  } else if (is_compressed_clog(*probe_buf, *(probe_buf + 1))) {
    int32_t orig_size = 0;
    int64_t pos = sizeof(int16_t);
    int64_t consume_len = 0;
    ObReadBufGuard guard(ObModIds::OB_LOG_DIRECT_READER_COMPRESS_ID);
    ObReadBuf& compress_rbuf = guard.get_read_buf();
    if (OB_UNLIKELY(!compress_rbuf.is_valid())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      CLOG_LOG(WARN, "failed to alloc read buf", K(compress_rbuf), K(want_file_id), K(want_offset), K(ret));
    } else if (OB_FAIL(serialization::decode_i32(probe_buf, probe_size, pos, &orig_size))) {
      CLOG_LOG(WARN, "failed to decode orig_size", K(ret));
    } else if (orig_size > buf_size) {
      ret = OB_BUF_NOT_ENOUGH;
      CLOG_LOG(WARN,
          "user buf is not enough",
          K(orig_size),
          K(buf_size),
          K(want_file_id),
          K(want_offset),
          K(want_size),
          K(ret));
    } else if (OB_FAIL(read_data_from_hot_cache(addr, seq, want_file_id, want_offset, want_size, compress_rbuf.buf_))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        CLOG_LOG(WARN, "failed to read data from hot cache", K(want_file_id), K(want_offset), K(want_size), K(ret));
      }
    } else if (OB_FAIL(uncompress(compress_rbuf.buf_, want_size, user_buf, buf_size, origin_size, consume_len))) {
      CLOG_LOG(WARN, "failed to uncompress", K(ret));
    } else { /*do nothing*/
    }
  } else {
    if (OB_FAIL(read_data_from_hot_cache(addr, seq, want_file_id, want_offset, want_size, user_buf))) {
      CLOG_LOG(WARN, "failed to read data from hot cache", K(want_file_id), K(want_offset), K(want_size), K(ret));
    } else {
      origin_size = want_size;
    }
  }
  return ret;
}
// if read a file which has beed reclaimed, return OB_FILE_RECYCLED
// if read a file which will be written, return OB_READ_NOTHING
int ObLogDirectReader::handle_no_file(const file_id_t file_id)
{
  int ret = OB_SUCCESS;
  file_id_t min_file_id = OB_INVALID_FILE_ID;
  file_id_t max_file_id = OB_INVALID_FILE_ID;
  if (OB_ISNULL(file_store_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(file_store_->get_file_id_range(min_file_id, max_file_id))) {
    // OB_IO_SYS or OB_ENTRY_NOT_EXIST
    CLOG_LOG(WARN, "get file id range error", K(ret), K(file_id));
  } else {
    const char* path = file_store_->get_dir_name();
    if (file_id < min_file_id) {
      ret = OB_FILE_RECYCLED;
      CLOG_LOG(
          WARN, "try to read from a recycled log file", K(ret), K(path), K(file_id), K(min_file_id), K(max_file_id));
    } else {
      ret = OB_READ_NOTHING;
      CLOG_LOG(INFO,
          "try to read from a new log file which is creating",
          K(ret),
          K(path),
          K(file_id),
          K(min_file_id),
          K(max_file_id));
    }
  }
  return ret;
}

// Line is a processing unit
int64_t ObLogDirectReader::calc_read_size(const int64_t rbuf_len, const offset_t line_key_offset) const
{
  const int64_t try_read_size = READ_DISK_SIZE;
  // read size cannot exceed the end of the file
  const int64_t size_limitted_by_file_size = limit_by_file_size(line_key_offset + try_read_size) - line_key_offset;
  // read size cannot exceed the length of the buff
  const int64_t unaligned_read_size = std::min(rbuf_len, size_limitted_by_file_size);
  // avoid incomplate data in line cache
  const int64_t read_size = do_align_offset((offset_t)unaligned_read_size, CLOG_CACHE_SIZE);
  return read_size;
}

// File trailer is not in cold cache, excep for file trailer
// the others are processed according to line granularity
// line_key_offset has aligned by cache_line.
int ObLogDirectReader::read_lines_from_file(const file_id_t want_file_id, const share::ObLogReadFdHandle& fd_handle,
    const offset_t line_key_offset, ObReadBuf& read_buf, int64_t& total_read_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!read_buf.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "read buf is invalid", KR(ret), K(read_buf), K(want_file_id), K(line_key_offset));
  } else if (OB_UNLIKELY(!is_offset_align(line_key_offset, CLOG_CACHE_SIZE))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "line_key_offset is not aligned", K(want_file_id), K(line_key_offset));
  } else {
    const int64_t read_size = calc_read_size(read_buf.buf_len_, line_key_offset);
    int64_t actual_read_count = -1;
    if (OB_UNLIKELY(read_size <= 0) || OB_UNLIKELY(!is_offset_align((offset_t)read_size, CLOG_CACHE_SIZE))) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR,
          "invalid read_size",
          K(ret),
          K(want_file_id),
          K(line_key_offset),
          K(fd_handle),
          K(total_read_size),
          K(read_size));
    } else if (OB_FAIL(read_from_io(
                   want_file_id, fd_handle, read_buf.buf_, read_size, line_key_offset, actual_read_count))) {
      CLOG_LOG(WARN, "can not read enough bytes", K(ret), K(want_file_id), K(line_key_offset), K(fd_handle));
    } else if (OB_UNLIKELY(actual_read_count < 0)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR,
          "invalid actual read count",
          K(ret),
          K(want_file_id),
          K(line_key_offset),
          K(fd_handle),
          K(total_read_size),
          K(read_size),
          K(actual_read_count));
    } else {
      total_read_size = actual_read_count;
    }
  }
  return ret;
}

int ObLogDirectReader::put_into_kvcache(const common::ObAddr& addr, const int64_t seq, const file_id_t want_file_id,
    const offset_t line_key_offset, char* buf)
{
  int ret = OB_SUCCESS;
  if (!use_cache_) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "reader do not use cache", K(use_cache_), KP(log_cache_), KR(ret));
  } else {
    ret = log_cache_->put_line(addr, seq, want_file_id, line_key_offset, buf);
  }
  return ret;
}

// The buffser used to read is thread local, line_buf points to this buffer.
int ObLogDirectReader::read_disk_and_update_kvcache(const common::ObAddr& addr, const int64_t seq,
    const file_id_t want_file_id, const offset_t line_key_offset, ObReadBuf& read_buf, const char*& line_buf,
    ObReadCost& cost)
{
  int ret = OB_SUCCESS;
  int put_ret = OB_SUCCESS;
  ObLogReadFdHandle fd_handle;
  CriticalGuard(get_log_file_qs());
  if (OB_FAIL(OB_LOG_FILE_READER.get_fd(file_store_->get_dir_name(), want_file_id, fd_handle))) {
    CLOG_LOG(WARN, "get fd failed.", K(ret), K(fd_handle), K(want_file_id));
    ret = handle_no_file(want_file_id);
  }
  if (OB_SUCC(ret) && fd_handle.is_valid()) {  // check fd to make coverity happy
    // If need read disk, make a thread local buffer to store data, line_buf points
    // to this buffer.
    // Clod cache guarantes that the data to be read is limit by file tailer.
    int64_t total_read_size = 0;
    int64_t start_ts = ObTimeUtility::current_time();

    offset_t align_offset = do_align_offset(line_key_offset, READ_DISK_SIZE);
    offset_t backoff = line_key_offset - align_offset;

    if (OB_FAIL(read_lines_from_file(want_file_id, fd_handle, align_offset, read_buf, total_read_size))) {
      CLOG_LOG(WARN, "read line from file error", K(ret), K(want_file_id), K(fd_handle), K(align_offset), K(read_buf));
    } else {
      int64_t after_read_disk_ts = ObTimeUtility::current_time();

      cost.read_disk_count_++;
      cost.read_clog_disk_time_ += after_read_disk_ts - start_ts;
      cost.read_disk_size_ += total_read_size;
      line_buf = read_buf.buf_ + backoff;

      int64_t finish_size = 0;
      int64_t cache_item_count = 0;
      int64_t exist_cache_item_count = 0;

      while (use_cache_ && finish_size < total_read_size && OB_SUCCESS == put_ret) {
        put_ret = put_into_kvcache(
            addr, seq, want_file_id, (offset_t)(align_offset + finish_size), read_buf.buf_ + finish_size);
        if (OB_SUCCESS == put_ret || OB_ENTRY_EXIST == put_ret) {
          CLOG_LOG(TRACE,
              "step put into kvcache success or entry exist",
              K(put_ret),
              K(want_file_id),
              K(align_offset),
              K(total_read_size),
              K(finish_size));
          if (OB_SUCCESS == put_ret) {
            cache_item_count++;
          } else if (OB_ENTRY_EXIST == put_ret) {
            exist_cache_item_count++;
          }

          put_ret = OB_SUCCESS;
          finish_size += CLOG_CACHE_SIZE;
        } else {
          CLOG_LOG(WARN, "put into kvcache error", K(put_ret), K(want_file_id), K(line_key_offset));
        }
      }

      int64_t after_put_cache_ts = ObTimeUtility::current_time();
      cost.fill_clog_cold_cache_time_ += after_put_cache_ts - after_read_disk_ts;

      CLOG_LOG(TRACE,
          "read_disk_and_put_into_kvcache",
          "read_disk_time",
          after_read_disk_ts - start_ts,
          "put_cache_time",
          after_put_cache_ts - after_read_disk_ts,
          K(want_file_id),
          K(line_key_offset),
          K(align_offset),
          K(cache_item_count),
          K(exist_cache_item_count),
          K(total_read_size),
          K(cost));
    }
  }
  return ret;
}

int ObLogDirectReader::get_line_buf(common::ObKVCacheHandle& handle, const common::ObAddr& addr, const int64_t seq,
    const file_id_t want_file_id, const offset_t line_key_offset, ObReadBuf& rbuf, const char*& line_buf,
    ObReadCost& cost)
{
  int ret = OB_SUCCESS;
  if (!use_cache_) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "reader do not use cache", K(use_cache_), KP(log_cache_), KR(ret));
  } else {
    ret = log_cache_->get_line(addr, seq, want_file_id, line_key_offset, handle, line_buf);
    if (OB_SUCC(ret)) {
      // success
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      // If need read disk, line_buf points to the position on a thread local buffer(2MB), the thread local buffer
      // is always valid.
      if (OB_FAIL(read_disk_and_update_kvcache(addr, seq, want_file_id, line_key_offset, rbuf, line_buf, cost))) {
        CLOG_LOG(WARN, "read disk and update kvcache error", K(ret), K(want_file_id), K(line_key_offset), KP(line_buf));
      }
    } else {
      CLOG_LOG(WARN, "log cache get handle and line buffer error", K(ret), K(want_file_id), K(line_key_offset));
    }
  }
  return ret;
}

int ObLogDirectReader::read_from_cold_cache(common::ObKVCacheHandle& handle, const common::ObAddr& addr,
    const int64_t seq, const file_id_t want_file_id, const offset_t want_offset, const int64_t want_size,
    char* user_buf, ObReadCost& cost)
{
  int ret = OB_SUCCESS;
  const char* line_buf = NULL;
  const int64_t clog_cache_line_size = CLOG_CACHE_SIZE;
  offset_t step_line_key_offset = do_align_offset(want_offset, clog_cache_line_size);
  offset_t backoff = want_offset - step_line_key_offset;
  int64_t step_size = std::min(clog_cache_line_size, want_size + backoff);
  int64_t finish_size = 0;
  const int64_t old_cost_read_disk_count = cost.read_disk_count_;
  // We have guaranteed that hot cache contains the valid data for last file
  //
  // A read request must not cross file.
  //
  // To ensure cache_line alignment, split read request into multiple rounds.
  //
  // Each round, the ling_buf is protected by handle, meanwhile, the data of
  // line buf will copy to user buffer.

  ObReadBufGuard guard(ObModIds::OB_LOG_DIRECT_READER_CACHE_ID);
  ObReadBuf& rbuf = guard.get_read_buf();
  if (OB_UNLIKELY(!rbuf.is_valid())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN,
        "failed to alloc read_buf",
        K(rbuf),
        K(ret),
        K(want_file_id),
        K(want_offset),
        K(want_size),
        K(step_line_key_offset),
        K(step_size),
        K(finish_size),
        K(backoff));
  }
  while (OB_SUCC(ret) && finish_size < want_size) {
    if (OB_FAIL(get_line_buf(handle, addr, seq, want_file_id, step_line_key_offset, rbuf, line_buf, cost))) {
      if (OB_SIZE_OVERFLOW != ret) {
        CLOG_LOG(WARN,
            "get line buf error",
            K(ret),
            K(want_file_id),
            K(want_offset),
            K(want_size),
            K(step_line_key_offset),
            K(step_size),
            K(finish_size),
            K(backoff));
      } else {
        CLOG_LOG(TRACE,
            "get line buf error",
            K(ret),
            K(want_file_id),
            K(want_offset),
            K(want_size),
            K(step_line_key_offset),
            K(step_size),
            K(finish_size),
            K(backoff));
      }
    } else if (OB_ISNULL(line_buf)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN,
          "get null line buf",
          K(ret),
          K(want_file_id),
          K(want_offset),
          K(want_size),
          K(step_line_key_offset),
          K(step_size),
          K(finish_size),
          K(backoff));
    } else {
      CLOG_LOG(TRACE,
          "copy from line",
          K(ret),
          K(want_file_id),
          K(want_offset),
          K(want_size),
          K(step_line_key_offset),
          K(step_size),
          K(finish_size),
          K(backoff));

      int64_t valid_data_size = step_size - backoff;

      int64_t begin_ts = ObTimeUtility::current_time();
      // copy line_buf every round
      MEMCPY(user_buf + finish_size, line_buf + backoff, valid_data_size);
      int64_t end_ts = ObTimeUtility::current_time();

      // update statistics
      cost.memcpy_time_ += end_ts - begin_ts;
      cost.valid_data_size_ += valid_data_size;

      step_line_key_offset += static_cast<offset_t>(clog_cache_line_size);
      finish_size += step_size - backoff;
      step_size = std::min(clog_cache_line_size, (want_size - finish_size));
      backoff = 0;
    }
  }

  if (OB_SUCC(ret)) {
    const int64_t new_cost_read_disk_count = cost.read_disk_count_;
    if (new_cost_read_disk_count > old_cost_read_disk_count) {
      ATOMIC_INC(&cache_miss_count_);
    } else {
      ATOMIC_INC(&cache_hit_count_);
    }
  }
  return ret;
}

// caller guarantee the size of user_buf is enough, meanwhile,
// handle is used in single thread
int ObLogDirectReader::read_data_impl(common::ObKVCacheHandle& handle, const common::ObAddr& addr, const int64_t seq,
    const file_id_t want_file_id, const offset_t want_offset, const int64_t want_size, char* user_buf, ObReadCost& cost)
{
  int ret = OB_SUCCESS;
  ObLogWritePoolType file_type = INVALID_WRITE_POOL;
  if (OB_UNLIKELY(OB_FAIL(get_log_file_type(file_type))) || OB_UNLIKELY(!addr.is_valid()) ||
      OB_UNLIKELY(!is_valid_file_id(want_file_id)) ||
      OB_UNLIKELY(CLOG_WRITE_POOL == file_type && !is_valid_offset(want_offset)) ||
      OB_UNLIKELY(ILOG_WRITE_POOL == file_type && want_offset < 0) || OB_UNLIKELY(want_size <= 0) ||
      OB_UNLIKELY(NULL == user_buf)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(want_file_id), K(want_offset), K(want_size), KP(user_buf));
  } else if (OB_UNLIKELY(!use_cache_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN,
        "should not enter here where not using_cache",
        KR(ret),
        K(use_cache_),
        K(want_file_id),
        K(want_offset),
        K(want_size));
  } else {
    ret = read_data_from_hot_cache(addr, seq, want_file_id, want_offset, want_size, user_buf);
    if (OB_SUCC(ret)) {
      ATOMIC_INC(&cache_hit_count_);
      CLOG_LOG(TRACE, "read from hot cache success", K(want_file_id), K(want_offset), K(want_size), KP(user_buf));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      CLOG_LOG(TRACE, "hot cache miss", K(want_file_id), K(want_offset), K(want_size), KP(user_buf));
      // hot cache miss, read from cold cache(or disk if miss)
      if (OB_SUCC(read_from_cold_cache(handle, addr, seq, want_file_id, want_offset, want_size, user_buf, cost))) {
        // If part of this read hits the cache, part of it reads the disk,
        // count as read disk
        CLOG_LOG(TRACE, "read from cold cache success", K(want_file_id), K(want_offset), K(want_size), KP(user_buf));
      } else if (OB_SIZE_OVERFLOW == ret) {
        CLOG_LOG(TRACE,
            "read from cold cache size overflow",
            K(ret),
            K(want_file_id),
            K(want_offset),
            K(want_offset),
            KP(user_buf));
      } else {
        CLOG_LOG(
            WARN, "read from cold cache error", K(ret), K(want_file_id), K(want_offset), K(want_offset), KP(user_buf));
      }
    } else {
      CLOG_LOG(WARN, "read from hot cache error", K(ret), K(want_file_id), K(want_offset), K(want_size), KP(user_buf));
    }
    EVENT_ADD(CLOG_READ_SIZE, want_size);
    EVENT_INC(CLOG_READ_COUNT);
  }
  return ret;
}

int ObLogDirectReader::read_from_io(const file_id_t want_file_id, const share::ObLogReadFdHandle& fd_handle, char* buf,
    const int64_t count, const int64_t offset, int64_t& read_count)
{
  int ret = OB_SUCCESS;
  bool has_read = false;
  ObAtomicFilePos read_start_pos;
  ObAtomicFilePos read_end_pos;
  ObAtomicFilePos flush_pos;
  ObAtomicFilePos write_pos;

  read_start_pos.file_id_ = want_file_id;
  read_start_pos.file_offset_ = (uint32_t)offset;
  read_end_pos.file_id_ = want_file_id;
  read_end_pos.file_offset_ = (uint32_t)(offset + count);
  if (nullptr != log_buffer_) {
    flush_pos.atomic_ = ATOMIC_LOAD(&(log_buffer_->file_flush_pos_.atomic_));
    write_pos.atomic_ = ATOMIC_LOAD(&(log_buffer_->file_write_pos_.atomic_));
  }

  if (0 == flush_pos.atomic_ || 0 == write_pos.atomic_) {
    // first start or server has been restarted or no shared memory buffer(OFS)
  } else {
    if (flush_pos >= read_end_pos) {
      // log has been flushed to file, read log from disk
    } else {
      lib::ObMutexGuard guard(log_ctrl_->buf_mutex_);
      flush_pos = log_buffer_->file_flush_pos_;
      write_pos = log_buffer_->file_write_pos_;

      if (flush_pos >= read_end_pos) {
        // double check, log has been flushed to file, read log from disk
      } else if (OB_FAIL(read_disk(fd_handle, buf, count, offset, read_count))) {
        CLOG_LOG(ERROR, "Fail to read log from disk, ", K(ret), K(want_file_id), K(count), K(offset), K(read_count));
      } else {
        CLOG_LOG(INFO, "Read log from shm buf, ", K(want_file_id), K(count), K(offset), K(write_pos), K(flush_pos));
        has_read = true;
        if (flush_pos == write_pos) {
          // log has been flushed
        } else if (flush_pos.file_id_ != write_pos.file_id_) {
          // may switch file
        } else {
          // log has not been flushed to file, read log from share memory, flush_pos < read_pos <= write_pos
          write_pos.file_offset_ = write_pos.file_offset_ + ObPaddingEntry::get_padding_size(write_pos.file_offset_);
          if (write_pos > read_start_pos) {
            if (write_pos.file_id_ == read_start_pos.file_id_) {
              int64_t shm_data_len = write_pos.file_offset_ - lower_align(flush_pos.file_offset_, CLOG_DIO_ALIGN_SIZE);
              if (offset < lower_align(flush_pos.file_offset_, CLOG_DIO_ALIGN_SIZE)) {
                int64_t read_offset = lower_align(flush_pos.file_offset_, CLOG_DIO_ALIGN_SIZE) - offset;
                if (count - read_offset > 0) {
                  MEMCPY(buf + read_offset, log_ctrl_->data_buf_, std::min(shm_data_len, count - read_offset));
                }
                CLOG_LOG(INFO, "Copy memory from shm to buf, ", K(read_offset), K(shm_data_len));
              } else {
                int64_t buf_offset = offset - lower_align(flush_pos.file_offset_, CLOG_DIO_ALIGN_SIZE);
                if (shm_data_len - buf_offset > 0) {
                  MEMCPY(buf, log_ctrl_->data_buf_ + buf_offset, std::min(count, shm_data_len - buf_offset));
                }
                CLOG_LOG(INFO, "Copy memory from shm to buf, ", K(buf_offset), K(shm_data_len));
              }
            }
          } else {
            // check_last_block function may read data beyond write_pos
          }
        }
      }
    }
  }

  if (OB_SUCC(ret) && !has_read) {
    if (OB_FAIL(read_disk(fd_handle, buf, count, offset, read_count))) {
      CLOG_LOG(ERROR, "Fail to read log from disk, ", K(ret), K(want_file_id), K(count), K(offset), K(read_count));
    }
  }
  return ret;
}

int ObLogDirectReader::read_disk(const share::ObLogReadFdHandle& fd_handle, char* buf, const int64_t count,
    const int64_t offset, int64_t& read_count)
{
  int ret = OB_SUCCESS;
  read_count = -1;
  const int64_t before_read_disk = ObTimeUtility::current_time();
  if (OB_FAIL(OB_LOG_FILE_READER.pread(fd_handle, buf, count, offset, read_count))) {
    CLOG_LOG(WARN, "log file reader fail", K(ret), K(fd_handle), K(offset), K(read_count), K(count));
  } else if (count != read_count) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "read not enough", K(ret), K(fd_handle), K(offset), K(read_count), K(count));
  }

  if (OB_SUCC(ret)) {
    const int64_t after_read_disk = ObTimeUtility::current_time();
    EVENT_ADD(CLOG_DISK_READ_SIZE, read_count);
    EVENT_INC(CLOG_DISK_READ_COUNT);
    EVENT_ADD(CLOG_DISK_READ_TIME, after_read_disk - before_read_disk);
  }
  return ret;
}

int ObLogDirectReader::is_valid_read_param(const ObReadParam& param, bool& is_valid) const
{
  int ret = OB_SUCCESS;
  file_id_t file_id = param.file_id_;
  offset_t offset = param.offset_;
  int64_t read_len = param.read_len_;
  int64_t timeout = param.timeout_;

  ObLogWritePoolType type = INVALID_WRITE_POOL;
  is_valid = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "is_valid_read_param not init", K(ret), K(param));
  } else if (OB_FAIL(get_log_file_type(type))) {
    CLOG_LOG(ERROR, "get_log_file_type", K(ret), K(type));
  } else if (OB_UNLIKELY(!is_valid_file_id(file_id)) || OB_UNLIKELY(ILOG_WRITE_POOL == type && offset < 0) ||
             OB_UNLIKELY(CLOG_WRITE_POOL == type && !is_valid_offset(offset)) ||
             OB_UNLIKELY(read_len <= 0 || read_len > OB_MAX_LOG_BUFFER_SIZE) || OB_UNLIKELY(timeout <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid argument", K(ret), K(param));
  } else {
    is_valid = true;
  }
  return ret;
}

int ObLogDirectReader::get_log_file_type(ObLogWritePoolType& file_type) const
{
  int ret = OB_SUCCESS;
  if (NULL == file_store_) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "get_log_file_type failed", K(ret));
  } else {
    ObRedoLogType log_type = file_store_->get_redo_log_type();
    switch (log_type) {
      case OB_REDO_TYPE_CLOG:
        file_type = CLOG_WRITE_POOL;
        break;
      case OB_REDO_TYPE_ILOG:
        file_type = ILOG_WRITE_POOL;
        break;
      case OB_REDO_TYPE_SLOG:
        file_type = SLOG_WRITE_POOL;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "invalid write pool type", K(ret), K(log_type));
        break;
    }
  }
  return ret;
}

// read data from disk directlly
int ObLogDirectReader::read_data_direct_impl(
    const ObReadParam& param, ObReadBuf& rbuf, ObReadRes& res, ObReadCost& cost)
{
  int ret = OB_SUCCESS;
  bool is_valid_param = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "direct read not init", K(ret), K(param));
  } else if (OB_FAIL(is_valid_read_param(param, is_valid_param))) {
    CLOG_LOG(WARN, "is_valid_read_param falied", K(ret), K(param), K(is_valid_param));
  } else if (OB_UNLIKELY(!is_valid_param) || OB_UNLIKELY(!rbuf.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid read argument", K(ret), K(param), K(rbuf));
  } else {
    ObReadParam limited_param;
    ObReadParam aligned_param;
    ObReadRes read_file_res;
    int64_t backoff = 0;
    if (OB_SUCC(limit_param_by_tail(param, limited_param))) {
      // for direct io
      align_param_for_dio_without_cache(limited_param, aligned_param, backoff);
      // do not need split since bypass kvcache
      if (OB_FAIL(read_data_from_file(aligned_param, rbuf, read_file_res, cost))) {
        CLOG_LOG(WARN, "read data direct fail", K(ret), K(param), K(limited_param), K(aligned_param), K(rbuf));
      } else {
        res.buf_ = read_file_res.buf_ + backoff;
        // the visible length is still the value limited by tail, so it may read more byte(aligned by 512)
        res.data_len_ = std::min(read_file_res.data_len_ - backoff, limited_param.read_len_);
        EVENT_ADD(CLOG_READ_SIZE, res.data_len_);
        EVENT_INC(CLOG_READ_COUNT);
        cost.read_disk_count_++;
        ATOMIC_INC(&cache_miss_count_);
      }
    } else {
      // OB_READ_NOTHING
      CLOG_LOG(TRACE, "limit param by tail", K(ret), K(limited_param));
    }
    CLOG_LOG(TRACE,
        "read_data_direct_impl",
        K(ret),
        K(param),
        K(limited_param),
        K(aligned_param),
        K(read_file_res),
        K(res),
        K(backoff));
  }
  return ret;
}

// --- Public Interface ---
int ObLogDirectReader::read_data_direct(const ObReadParam& param, ObReadBuf& rbuf, ObReadRes& res, ObReadCost& cost)
{
  stat_cache_hit_rate();
  return read_data_direct_impl(param, rbuf, res, cost);
}

int ObLogDirectReader::read_data(const ObReadParam& param, const common::ObAddr& addr, const int64_t seq,
    ObReadBuf& rbuf, ObReadRes& res, ObReadCost& cost)
{
  int ret = OB_SUCCESS;
  ObReadParam limited_param;
  bool is_valid_param = false;
  if (OB_FAIL(is_valid_read_param(param, is_valid_param))) {
    CLOG_LOG(WARN, "is_valid_read_param failed", K(ret), K(param));
  } else if (OB_UNLIKELY(!is_valid_param) || OB_UNLIKELY(!rbuf.is_valid()) || OB_UNLIKELY(!addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(param), K(addr), K(rbuf), K(res));
  } else if (OB_SUCC(limit_param_by_tail(param, limited_param))) {
    common::ObKVCacheHandle handle;
    if (OB_FAIL(read_data_impl(handle,
            addr,
            seq,
            limited_param.file_id_,
            limited_param.offset_,
            limited_param.read_len_,
            rbuf.buf_,
            cost))) {
      CLOG_LOG(WARN, "read_data_impl error", K(ret), K(param), K(addr), K(seq), K(rbuf), K(cost));
    } else {
      res.buf_ = rbuf.buf_;
      res.data_len_ = limited_param.read_len_;
    }
  } else {
    // OB_READ_NOTHING
  }
  stat_cache_hit_rate();
  CLOG_LOG(TRACE, "read data", K(ret), K(param), K(rbuf), K(res));
  return ret;
}

int ObLogDirectReader::read_log(const ObReadParam& param, const common::ObAddr& addr, const int64_t seq,
    ObReadBuf& rbuf, ObLogEntry& entry, ObReadCost& cost)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObReadRes res;
  ObReadParam probe_param;
  probe_param.shallow_copy(param);
  probe_param.read_len_ = sizeof(int16_t);  // read magic first
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogDirectReader not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid_file_id(param.file_id_)) || OB_UNLIKELY(!is_valid_offset(param.offset_)) ||
             OB_UNLIKELY(param.read_len_ <= 0) || OB_UNLIKELY(param.read_len_ > OB_MAX_LOG_BUFFER_SIZE) ||
             OB_UNLIKELY(!addr.is_valid()) || OB_UNLIKELY(!rbuf.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(param), K(addr), K(seq), K(rbuf));
    // read two bytes, used to determine whether is a compressed log
  } else if (OB_FAIL(read_data(probe_param, addr, seq, rbuf, res, cost))) {
    CLOG_LOG(WARN, "read data error", K(ret), K(param), K(rbuf), K(res), K(cost));
  } else if (OB_UNLIKELY(res.data_len_ < 2)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "invalid read data len", K(ret), K(param), K(rbuf), K(res), K(cost));
  } else {
    if (is_compressed_clog(*res.buf_, *(res.buf_ + 1))) {
      int64_t uncompress_len = 0;
      int64_t comsume_buf_len = 0;
      ObReadBufGuard guard(ObModIds::OB_LOG_DIRECT_READER_COMPRESS_ID);
      ObReadBuf& compress_rbuf = guard.get_read_buf();
      if (OB_UNLIKELY(!compress_rbuf.is_valid())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        CLOG_LOG(WARN, "failed to alloc read_buf", K(compress_rbuf), K(param), K(ret));
      } else if (OB_FAIL(read_data(param, addr, seq, compress_rbuf, res, cost))) {
        CLOG_LOG(WARN, "read data error", K(ret), K(param), K(rbuf), K(res), K(cost));
      } else if (OB_FAIL(
                     uncompress(res.buf_, res.data_len_, rbuf.buf_, rbuf.buf_len_, uncompress_len, comsume_buf_len))) {
        CLOG_LOG(WARN, "failed to uncompress", K(res), K(param), K(ret));
      } else {
        res.buf_ = rbuf.buf_;
        res.data_len_ = uncompress_len;
      }
    } else {
      if (OB_FAIL(read_data(param, addr, seq, rbuf, res, cost))) {
        CLOG_LOG(WARN, "read data error", K(ret), K(param), K(rbuf), K(res), K(cost));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(entry.deserialize(res.buf_, res.data_len_, pos))) {
        CLOG_LOG(WARN, "clog entry deserialize error", K(ret), K(res), K(pos), K(entry), K(param));
      } else if (OB_UNLIKELY(OB_UNLIKELY(entry.get_header().get_total_len() > res.data_len_) ||
                             OB_UNLIKELY(!entry.check_integrity()))) {
        ret = OB_INVALID_DATA;
        CLOG_LOG(WARN, "clog entry check data error", K(ret), K(entry), K(res), K(param), K(rbuf));
      } else {
        // success, do nothing
      }
    }
  }
  return ret;
}

int ObLogDirectReader::read_trailer(
    const ObReadParam& param, ObReadBuf& rbuf, int64_t& start_pos, file_id_t& next_file_id, ObReadCost& cost)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_file_id(param.file_id_)) || OB_UNLIKELY(param.timeout_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "read trailer data error", K(ret), K(param));
  } else {
    ObLogFileTrailer trailer;
    int64_t pos = 0;
    ObReadRes res;
    ObReadParam trailer_param;
    trailer_param.file_id_ = param.file_id_;
    trailer_param.offset_ = CLOG_TRAILER_ALIGN_WRITE_OFFSET;  // 4k aligned write, but data is in last 512bytes
    trailer_param.read_len_ = CLOG_DIO_ALIGN_SIZE;
    trailer_param.timeout_ = param.timeout_;

    const char* trailer_buf = NULL;
    int64_t trailer_len = 0;

    // always read trailed from disk, handling error code  specially
    if (OB_SUCCESS != (ret = read_data_direct_impl(trailer_param, rbuf, res, cost))) {
      if (OB_READ_NOTHING == ret) {
        CLOG_LOG(INFO, "read trailer data error", K(ret), K(trailer_param));
      } else {
        CLOG_LOG(WARN, "read trailer data error", K(ret), K(trailer_param));
      }
    } else {
      trailer_buf = res.buf_ + (CLOG_DIO_ALIGN_SIZE - CLOG_TRAILER_SIZE);
      trailer_len = CLOG_TRAILER_SIZE;
    }

    if (OB_FAIL(ret)) {
      CLOG_LOG(WARN, "fail to read trailer data", K(ret));
    } else if (OB_FAIL(trailer.deserialize(trailer_buf, trailer_len, pos))) {
      CLOG_LOG(WARN, "trailer deserialize fail", K(ret), KP(trailer_buf), K(trailer_len), K(res), K(pos));
    } else if (OB_UNLIKELY(trailer.get_file_id() != trailer_param.file_id_ + 1)) {
      ret = OB_INVALID_DATA;
      CLOG_LOG(WARN,
          "trailer file id wrong",
          K(ret),
          "cur_file_id",
          trailer_param.file_id_,
          "next_file_id",
          trailer.get_file_id());
    } else {
      start_pos = trailer.get_start_pos();
      next_file_id = trailer.get_file_id();
    }
  }
  return ret;
}

int ObLogDirectReader::get_clog_real_length(
    const common::ObAddr& addr, const int64_t seq, const ObReadParam& param, int64_t& real_length)
{
  int ret = OB_SUCCESS;
  // probe_size include three parts: size of magic, size before compress, size after compress
  const int64_t PROBE_SIZE = sizeof(int16_t) + sizeof(int32_t) + sizeof(int32_t);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogDirectReader not init", KR(ret), K(param));
  } else if (OB_UNLIKELY(!is_valid_file_id(param.file_id_)) || OB_UNLIKELY(!is_valid_offset(param.offset_)) ||
             OB_UNLIKELY(param.read_len_ <= PROBE_SIZE) || OB_UNLIKELY(param.read_len_ > OB_MAX_LOG_BUFFER_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(param), KR(ret));
  } else {
    char probe_buf[PROBE_SIZE] = {0};
    ObReadBuf rbuf;
    rbuf.buf_ = probe_buf;
    rbuf.buf_len_ = PROBE_SIZE;

    ObReadRes res;
    ObReadCost dummy_cost;
    ObReadParam probe_param;
    probe_param.shallow_copy(param);
    probe_param.read_len_ = PROBE_SIZE;  // read magic first
    // read two bytes, used to determine whether is a compressed log
    if (OB_FAIL(read_data(probe_param, addr, seq, rbuf, res, dummy_cost))) {
      CLOG_LOG(WARN, "read data error", K(ret), K(param), K(rbuf), K(res), K(dummy_cost));
    } else if (OB_UNLIKELY(res.data_len_ < PROBE_SIZE)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "invalid read data len", K(ret), K(param), K(rbuf), K(res), K(dummy_cost));
    } else if (is_compressed_clog(*probe_buf, *(probe_buf + 1))) {
      // skip magic_num
      int64_t pos = sizeof(int16_t);
      int32_t orig_size = 0;
      if (OB_FAIL(serialization::decode_i32(res.buf_, PROBE_SIZE, pos, &orig_size))) {
        CLOG_LOG(WARN, "failed to decode orig_size", K(param), K(ret));
      } else {
        real_length = orig_size;
      }
    } else {
      real_length = param.read_len_;
    }
  }
  return ret;
}

int ObLogDirectReader::get_batch_log_cursor(const ObPartitionKey& partition_key, const common::ObAddr& addr,
    const int64_t seq, const uint64_t log_id, const Log2File& log2file_item, ObGetCursorResult& result)
{
  int ret = OB_SUCCESS;
  const file_id_t file_id = log2file_item.get_file_id();
  const uint64_t min_log_id = log2file_item.get_min_log_id();
  const uint64_t max_log_id = log2file_item.get_max_log_id();
  const offset_t start_offset = log2file_item.get_start_offset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "direct reader is not inited", K(ret), K(partition_key), K(log_id), K(log2file_item));
  } else if (!partition_key.is_valid() || !addr.is_valid() || !is_valid_log_id(log_id) || !is_valid_file_id(file_id) ||
             !is_valid_log_id(min_log_id) || !is_valid_log_id(max_log_id) || min_log_id > max_log_id ||
             log_id < min_log_id || log_id > max_log_id || start_offset < 0 || !result.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(
        ERROR, "invalid arguments", K(ret), K(partition_key), K(addr), K(seq), K(log_id), K(log2file_item), K(result));
  } else {
    const int64_t MAX_RET_LEN = 1024;
    int64_t ret_len = std::min(static_cast<int64_t>(max_log_id - log_id + 1), result.arr_len_);
    ret_len = std::min(ret_len, MAX_RET_LEN);
    ObLogCursorExt tmp_log_cursor;
    ObReadParam param;
    param.file_id_ = file_id;
    param.offset_ = start_offset + static_cast<offset_t>((log_id - min_log_id) * tmp_log_cursor.get_serialize_size());
    param.read_len_ = ret_len * tmp_log_cursor.get_serialize_size();
    param.timeout_ = DEFAULT_READ_TIMEOUT;

    ObReadRes res;
    ObReadCost dummy_cost;
    ObReadBufGuard guard(ObModIds::OB_LOG_DIRECT_READER_ILOG_ID);
    ObReadBuf& rbuf = guard.get_read_buf();
    if (OB_UNLIKELY(!rbuf.is_valid())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      CSR_LOG(ERROR, "direct reader alloc_buf failed", K(partition_key), K(param), K(ret));
    } else if (OB_FAIL(read_data(param, addr, seq, rbuf, res, dummy_cost))) {
      CSR_LOG(WARN, "read_data failed", K(ret), K(param), K(addr), K(seq));
    } else if (res.data_len_ != param.read_len_) {
      ret = OB_ERR_UNEXPECTED;
      CSR_LOG(ERROR, "data_len not match", K(ret), K(partition_key), K(res.data_len_), K(param.read_len_));
    } else {
      int64_t pos = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < ret_len; i++) {
        if (OB_FAIL(tmp_log_cursor.deserialize(res.buf_, res.data_len_, pos))) {
          CSR_LOG(ERROR, "tmp_log_cursor deserialize failed", K(ret));
        } else {
          result.csr_arr_[i] = tmp_log_cursor;
        }
      }
    }

    if (OB_SUCC(ret)) {
      result.ret_len_ = ret_len;
    }
  }
  return ret;
}

}  // end namespace clog
}  // end namespace oceanbase
