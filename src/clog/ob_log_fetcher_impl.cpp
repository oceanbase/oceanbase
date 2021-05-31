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

#define USING_LOG_PREFIX EXTLOG

#include "ob_log_fetcher_impl.h"
#include <string.h>
#include "lib/allocator/ob_qsync.h"

#include "ob_log_engine.h"
#include "ob_log_line_cache.h"
#include "ob_log_compress.h"
#include "storage/transaction/ob_trans_log.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace clog;
using namespace storage;
using namespace obrpc;
using namespace transaction;
namespace logservice {
int ObLogFetcherImpl::init(ObLogLineCache& line_cache, ObILogEngine* log_engine)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == log_engine)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init error", K(ret), KP(log_engine));
  } else {
    const char* skip_hotcache_str = getenv("OB_LIBOBLOG_SKIP_HOTCACHE");
    if (NULL != skip_hotcache_str) {
      skip_hotcache_ = (0 == strcasecmp(skip_hotcache_str, "true")) ? true : false;
    }
    line_cache_ = &line_cache;
    log_engine_ = log_engine;
    is_inited_ = true;
  }
  return ret;
}

int ObLogFetcherImpl::read_line_data_(
    const char* line, const offset_t offset_in_line, char* buf, int64_t& pos, const int64_t read_size)
{
  int ret = OB_SUCCESS;
  int64_t line_size = ObLogLineCache::LINE_SIZE;

  if (OB_ISNULL(line) || OB_ISNULL(buf) || OB_UNLIKELY(pos < 0) || OB_UNLIKELY(offset_in_line < 0) ||
      OB_UNLIKELY(offset_in_line >= line_size) || OB_UNLIKELY(read_size > (line_size - offset_in_line))) {
    LOG_WARN("invalid argument", KP(line), KP(buf), K(pos), K(offset_in_line), K(read_size));
    ret = OB_INVALID_ARGUMENT;
  } else {
    MEMCPY(buf + pos, line + offset_in_line, read_size);
    pos += read_size;
  }
  return ret;
}

int ObLogFetcherImpl::fetch_decrypted_log_entry_(const ObPartitionKey& pkey, const clog::ObLogCursorExt& cursor_ext,
    char* log_buf, const int64_t buf_size, const int64_t end_tstamp, ObReadCost& read_cost,
    bool& fetch_log_from_hot_cache, int64_t& log_entry_size)
{
  int ret = OB_SUCCESS;
  ObReadParam param;
  param.offset_ = cursor_ext.get_offset();
  param.read_len_ = cursor_ext.get_size();
  param.file_id_ = cursor_ext.get_file_id();
  ret = fetch_decrypted_log_entry_(
      pkey, param, log_buf, buf_size, end_tstamp, read_cost, fetch_log_from_hot_cache, log_entry_size);
  return ret;
}

int ObLogFetcherImpl::fetch_decrypted_log_entry_(const ObPartitionKey& pkey, const ObReadParam& param, char* buf,
    const int64_t buf_size, const int64_t end_tstamp, ObReadCost& read_cost, bool& fetch_log_from_hot_cache,
    int64_t& log_entry_size)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(fetch_log_entry_(
          pkey, param, buf, buf_size, end_tstamp, read_cost, fetch_log_from_hot_cache, log_entry_size))) {
    LOG_WARN("failed to fetch log entry", K(ret), K(param), K(pkey));
  }
  return ret;
}

int ObLogFetcherImpl::fetch_log_entry_(const ObPartitionKey& pkey, const ObReadParam& param, char* buf,
    const int64_t buf_size, const int64_t end_tstamp, ObReadCost& read_cost, bool& fetch_log_from_hot_cache,
    int64_t& log_entry_size)
{
  int ret = OB_SUCCESS;
  offset_t offset = param.offset_;
  int64_t data_size = param.read_len_;
  file_id_t file_id = param.file_id_;

  const common::ObAddr addr = GCTX.self_addr_;
  const int64_t seq = 0;

  if (OB_ISNULL(log_engine_)) {
    LOG_WARN("invalid log engine", K(log_engine_));
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(data_size > buf_size)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_ERROR("buffer not enough", K(ret), K(buf_size), K(param), K(pkey));
  } else if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), KP(buf), K(param), K(pkey));
  } else if (skip_hotcache_) {
    fetch_log_from_hot_cache = false;
    if (OB_FAIL(read_uncompressed_data_from_line_cache_(
            file_id, offset, data_size, buf, buf_size, log_entry_size, end_tstamp, read_cost))) {
      LOG_WARN("failed read_uncompressed_data_from_line_cache", K(ret), K(param), K(pkey));
    }
  } else if (OB_SUCC(log_engine_->read_uncompressed_data_from_hot_cache(
                 addr, seq, file_id, offset, data_size, buf, buf_size, log_entry_size))) {
    fetch_log_from_hot_cache = true;
  } else if (OB_ENTRY_NOT_EXIST != ret) {
    LOG_WARN("read_data_from_hot_cache error", K(ret), K(param), K(pkey));
  } else {
    // rewrite ret
    ret = OB_SUCCESS;
    fetch_log_from_hot_cache = false;
    if (OB_FAIL(read_uncompressed_data_from_line_cache_(
            file_id, offset, data_size, buf, buf_size, log_entry_size, end_tstamp, read_cost))) {
      LOG_WARN("failed read_uncompressed_data_from_line_cache", K(ret), K(param), K(pkey));
    }
  }
  return ret;
}

int ObLogFetcherImpl::read_uncompressed_data_from_line_cache_(file_id_t file_id, offset_t offset, int64_t request_size,
    char* buf, int64_t buf_size, int64_t& log_entry_size, const int64_t end_tstamp, ObReadCost& read_cost)
{
  int ret = OB_SUCCESS;
  ObCompressedLogEntryHeader header;
  const int64_t PROBE_BUF_SIZE = header.get_serialize_size();
  char probe_buf[PROBE_BUF_SIZE];
  // size of magic, length before compress, length after compress
  const int64_t probe_size = sizeof(int16_t) + sizeof(int32_t) + sizeof(int32_t);
  if (OB_UNLIKELY(request_size > buf_size || offset < 0) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(file_id), K(offset), KP(buf), K(buf_size), K(ret));
  } else if (request_size < probe_size) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "request_size is too small", K(file_id), K(offset), K(request_size), K(ret));
  } else if (OB_FAIL(read_data_from_line_cache(file_id, offset, probe_size, probe_buf, end_tstamp, read_cost))) {
    CLOG_LOG(WARN, "failed to read data from line cache", K(file_id), K(offset), K(PROBE_BUF_SIZE), K(ret));
  } else if (is_compressed_clog(*probe_buf, *(probe_buf + 1))) {
    int32_t origin_size = 0;
    int64_t consume_len = 0;
    int64_t pos = sizeof(int16_t);  // skip magic_num
    int64_t uncompress_size = 0;
    ObReadBufGuard guard(ObModIds::OB_LOG_DIRECT_READER_COMPRESS_ID);
    ObReadBuf& compress_rbuf = guard.get_read_buf();
    if (OB_UNLIKELY(!compress_rbuf.is_valid())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      CLOG_LOG(WARN, "failed to prepare tsi read buf", K(ret), K(request_size));
    } else if (OB_FAIL(serialization::decode_i32(probe_buf, probe_size, pos, &origin_size))) {
      CLOG_LOG(WARN, "failed to decode orig_size", K(ret));
    } else if (origin_size > buf_size) {
      ret = OB_BUF_NOT_ENOUGH;
      CLOG_LOG(
          WARN, "user buf is not enough", K(origin_size), K(buf_size), K(file_id), K(offset), K(request_size), K(ret));
    } else if (OB_UNLIKELY(request_size > compress_rbuf.buf_len_)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "request data size is bigger than compress_buf_len", K(request_size), K(compress_rbuf), K(ret));
    } else if (OB_FAIL(read_data_from_line_cache(
                   file_id, offset, request_size, compress_rbuf.buf_, end_tstamp, read_cost))) {
      CLOG_LOG(WARN, "failed to read data from line cache", K(file_id), K(offset), K(request_size), K(ret));
    } else if (OB_FAIL(uncompress(compress_rbuf.buf_, request_size, buf, buf_size, uncompress_size, consume_len))) {
      CLOG_LOG(WARN, "failed to uncompress", K(ret));
    } else if (OB_UNLIKELY(uncompress_size != origin_size)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN,
          "data may be corruptted",
          K(uncompress_size),
          K(origin_size),
          K(file_id),
          K(offset),
          K(request_size),
          K(ret));
    } else {
      log_entry_size = uncompress_size;
    }
  } else {
    if (OB_FAIL(read_data_from_line_cache(file_id, offset, request_size, buf, end_tstamp, read_cost))) {
      CLOG_LOG(
          WARN, "failed to read data from line cache", K(file_id), K(offset), K(request_size), K(buf_size), K(ret));
    } else {
      log_entry_size = request_size;
    }
  }
  return ret;
}

int ObLogFetcherImpl::read_data_from_line_cache(file_id_t file_id, offset_t offset, int64_t request_size, char* buf,
    const int64_t end_tstamp, ObReadCost& read_cost)
{
  int ret = OB_SUCCESS;
  // Divide the request into multiple lines of read operations
  int64_t line_size = ObLogLineCache::LINE_SIZE;
  int64_t left_size = request_size;
  int64_t pos = 0;
  while (OB_SUCC(ret) && left_size > 0) {
    // According to the offset, calculate the length of the data to be read this time to ensure that it will not cross
    // the line
    offset_t offset_in_line = ObLogLineCache::offset_in_line(offset);
    int64_t read_size_in_line = std::min(line_size - offset_in_line, left_size);

    // raad data from line cache
    if (OB_SUCC(read_data_from_line_cache_(file_id, offset, read_size_in_line, buf, pos, end_tstamp, read_cost))) {}

    if (OB_SUCCESS == ret) {
      offset += static_cast<offset_t>(read_size_in_line);
      left_size -= read_size_in_line;
    }
  }
  return ret;
}

int ObLogFetcherImpl::read_data_from_line_cache_(const file_id_t file_id, const offset_t offset,
    const int64_t read_size, char* buf, int64_t& pos, const int64_t end_tstamp, ObReadCost& read_cost)
{
  int ret = OB_SUCCESS;
  char* line = NULL;
  bool need_load_data = false;
  offset_t offset_in_line = ObLogLineCache::offset_in_line(offset);
  static const int64_t GET_TIMEOUT_US = 100 * 1000L;

  if (OB_ISNULL(line_cache_)) {
    LOG_WARN("invalid line cache", K(line_cache_));
    ret = OB_NOT_INIT;
  } else {
    do {
      if (OB_FAIL(line_cache_->get_line(file_id, offset, GET_TIMEOUT_US, line, need_load_data))) {
        if (OB_ITEM_NOT_MATCH == ret || OB_EXCEED_MEM_LIMIT == ret || OB_TIMEOUT == ret) {
          // FIXME: Expected error code, retry directly under normal circumstances
          LOG_WARN("[LINE_CACHE] get_line fail", K(ret), K(file_id), K(offset), K(read_size));
          if (OB_TIMEOUT == ret) {
            int64_t cur_tstamp = ObTimeUtility::current_time();
            if (cur_tstamp > end_tstamp) {
              // Determine whether it really timed out
              ret = OB_TIMEOUT;
            } else {
              ret = OB_SUCCESS;
            }
          } else {
            ret = OB_SUCCESS;
          }
        } else {
          LOG_WARN("get_line from line cache fail", K(ret), K(file_id), K(offset));
        }
      } else if (OB_ISNULL(line)) {
        LOG_WARN("LINE_CACHE get_line NULL value", K(line), K(file_id), K(offset), K(need_load_data));
        ret = OB_ERR_UNEXPECTED;
      }
    } while (OB_SUCCESS == ret && NULL == line);

    if (OB_SUCCESS == ret && need_load_data) {
      // If you need to load data, read the line data from the disk
      if (OB_FAIL(load_line_data_(line, file_id, offset, read_cost))) {
        LOG_WARN("load_line_data_ fail", K(ret), K(file_id), K(offset));
      }

      // Identifies line data loading is complete
      bool load_data_ready = (OB_SUCCESS == ret);
      int tmp_ret = line_cache_->mark_line_status(file_id, offset, load_data_ready);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("mark_line_status fail", K(tmp_ret), K(file_id), K(offset), K(ret), K(load_data_ready));
        ret = (OB_SUCCESS == ret ? tmp_ret : ret);
      }
    }

    if (OB_SUCCESS == ret) {
      // read data from line
      if (OB_FAIL(read_line_data_(line, offset_in_line, buf, pos, read_size))) {
        LOG_WARN("read_line_data_ fail", K(ret), KP(line), K(file_id), K(offset), K(offset_in_line), K(read_size));
      } else {
        // success
      }
    }

    if (NULL != line) {
      // gc line
      int tmp_ret = line_cache_->revert_line(file_id, offset, read_size);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("line cache revert_line fail", K(tmp_ret), K(file_id), K(offset));
        ret = (OB_SUCCESS == ret ? tmp_ret : ret);
      }
    }
  }

  return ret;
}

int ObLogFetcherImpl::load_line_data_(char* line, const file_id_t file_id, const offset_t offset, ObReadCost& cost)
{
  int ret = OB_SUCCESS;
  static const int64_t DEFAULT_DISK_TIMEOUT = 1L * 1000L * 1000L;
  if (OB_ISNULL(line)) {
    LOG_WARN("invalid argument", K(line));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(log_engine_)) {
    LOG_WARN("invalid log engine", K(log_engine_));
    ret = OB_NOT_INIT;
  } else {
    ObReadParam param;
    ObReadBuf rbuf;
    ObReadRes res;
    int64_t line_size = ObLogLineCache::LINE_SIZE;  // Read the entire line at a time

    param.file_id_ = file_id;
    param.offset_ = static_cast<offset_t>(lower_align(offset, line_size));  // aligned by line_size
    param.read_len_ = line_size;
    // FIXME: At present, the bottom layer is pread, and the timeout parameter is no longer needed.
    // After timeout is supported in the future, a reasonable value should be set here.
    param.timeout_ = DEFAULT_DISK_TIMEOUT;

    rbuf.buf_ = line;
    rbuf.buf_len_ = line_size;

    res.reset();

    if (OB_FAIL(log_engine_->read_data_direct(param, rbuf, res, cost))) {
      LOG_WARN("read_data_direct fail", K(ret), K(param), K(rbuf), K(res));

    } else if (OB_UNLIKELY(rbuf.buf_ != res.buf_)) {
      LOG_WARN("read_data_direct return invalid data, unexpected", K(res), K(rbuf), K(param), K(line_size));
      ret = OB_INVALID_DATA;
    } else if (res.data_len_ < line_size) {
      ret = OB_INVALID_DATA;
      LOG_WARN("read_data_direct return invalid data, unexpected", K(ret), K(res), K(rbuf), K(param), K(line_size));
    } else {
      // read success
    }
  }
  return ret;
}

}  // namespace logservice
}  // namespace oceanbase
