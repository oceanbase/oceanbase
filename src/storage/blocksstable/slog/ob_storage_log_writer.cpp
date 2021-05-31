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

#define USING_LOG_PREFIX STORAGE_REDO

#include "ob_storage_log_writer.h"
#include <errno.h>
#include "lib/file/ob_file.h"
#include "share/config/ob_server_config.h"
#include "share/redolog/ob_log_store_factory.h"
#include "storage/blocksstable/ob_store_file_system.h"
#include "share/ob_debug_sync_point.h"
#include "share/ob_debug_sync.h"

namespace oceanbase {
using namespace lib;
using namespace common;

namespace blocksstable {
ObStorageLogItem::ObStorageLogItem()
    : start_cursor_(),
      end_cursor_(),
      is_inited_(false),
      is_local_(false),
      buf_size_(0),
      buf_(NULL),
      len_(0),
      flush_cond_(),
      flush_finish_(false),
      flush_ret_(OB_SUCCESS)
{
  flush_cond_.init(ObWaitEventIds::SLOG_FLUSH_COND_WAIT);
}

ObStorageLogItem::~ObStorageLogItem()
{
  reset();
}

int ObStorageLogItem::init(char* buf, const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(buf), K(buf_size));
  } else {
    if (nullptr == buf) {
      const ObMemAttr attr(OB_SERVER_TENANT_ID, ObModIds::OB_SLOG_WRITER);
      if (nullptr ==
          (buf_ = reinterpret_cast<char*>(ob_malloc_align(ObStorageLogWriter::LOG_FILE_ALIGN_SIZE, buf_size, attr)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc local buf fail", K(buf_size), K(attr));
      } else {
        is_local_ = true;
      }
    } else {
      buf_ = buf;
      is_local_ = false;
    }

    if (OB_SUCC(ret)) {
      buf_size_ = buf_size;
      is_inited_ = true;
    }
  }

  if (IS_NOT_INIT) {
    reset();
  }
  return ret;
}

bool ObStorageLogItem::is_valid() const
{
  return is_inited_ && start_cursor_.is_valid() && end_cursor_.is_valid() && NULL != buf_ && len_ >= 0 &&
         buf_size_ >= 0;
}

void ObStorageLogItem::reset()
{
  if (is_local_ && nullptr != buf_) {
    ob_free_align(buf_);
    buf_ = nullptr;
  }
  is_local_ = false;
  start_cursor_.reset();
  end_cursor_.reset();
  buf_ = NULL;
  len_ = 0;
  buf_size_ = 0;
  flush_finish_ = false;
  flush_ret_ = OB_SUCCESS;
  is_inited_ = false;
}

int ObStorageLogItem::wait_flush_log(const uint64_t max_wait_time)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObThreadCondGuard guard(flush_cond_);
    while (!flush_finish_) {
      if (OB_FAIL(flush_cond_.wait(max_wait_time))) {
        if (ret == OB_TIMEOUT) {
          LOG_WARN("flush_cond wait timeout");
        } else if (REACH_TIME_INTERVAL(30 * 1000 * 1000)) {
          LOG_ERROR("flush log wait failed", K(ret), K(*this));
        } else {
          LOG_WARN("flush log wait failed", K(ret), K(*this));
        }
      }
    }
#ifdef ERRSIM
    ret = E(EventTable::EN_SLOG_WAIT_FLUSH_LOG) OB_SUCCESS;
#endif
    if (OB_LIKELY(OB_SUCC(ret))) {
      ret = flush_ret_;
    } else {  // should never happen, just record error
      LOG_ERROR("wait finished but error occurs", K(ret), K_(flush_finish), K_(flush_ret));
    }
  }
  return ret;
}

void ObStorageLogItem::finish_flush(const int flush_ret)
{
  ObThreadCondGuard guard(flush_cond_);
  flush_ret_ = flush_ret;
  flush_finish_ = true;
  flush_cond_.signal();
}

int ObStorageLogItem::set_data_len(const int64_t len)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (len < 0 || len > buf_size_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(len), K(buf_size_));
  } else {
    len_ = len;
  }
  return ret;
}

int ObStorageLogWriter::NopLog::advance()
{
  int ret = OB_SUCCESS;
  last_ctime_ = ctime_;
  ctime_ = ObTimeUtility::fast_current_time();
  return ret;
}

int ObStorageLogWriter::NopLog::serialize(char* buf, int64_t limit, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_ISNULL(buf) || limit < 0 || pos > limit) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::encode_i64(buf, limit, new_pos, MAGIC)) ||
             OB_FAIL(server_.serialize(buf, limit, new_pos)) ||
             OB_FAIL(serialization::encode_i64(buf, limit, new_pos, ctime_)) ||
             OB_FAIL(serialization::encode_i64(buf, limit, new_pos, last_ctime_))) {
    LOG_WARN("serialize error", K(ret), KP(buf), K(limit), K(pos));
    ret = OB_SERIALIZE_ERROR;
  } else {
    pos = new_pos;
  }
  return ret;
}

int ObStorageLogWriter::NopLog::deserialize(const char* buf, int64_t limit, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  int64_t magic = 0;
  if (OB_ISNULL(buf) || limit < 0 || pos > limit) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::decode_i64(buf, limit, new_pos, static_cast<int64_t*>(&magic)))) {
    ret = OB_DESERIALIZE_ERROR;
  } else if (magic != MAGIC) {
    ret = OB_DESERIALIZE_ERROR;
  } else if (OB_FAIL(server_.deserialize(buf, limit, new_pos)) ||
             OB_FAIL(serialization::decode_i64(buf, limit, new_pos, static_cast<int64_t*>(&ctime_))) ||
             OB_FAIL(serialization::decode_i64(buf, limit, new_pos, static_cast<int64_t*>(&last_ctime_)))) {
    LOG_WARN("deserialization error", K(buf), K(limit), K(ctime_), K(last_ctime_));
    ret = OB_DESERIALIZE_ERROR;
  } else {
    pos = new_pos;
  }
  return ret;
}

int ObStorageLogWriter::SwitchLog::serialize(char* buf, int64_t limit, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_ISNULL(buf) || limit < 0 || pos > limit) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(limit), K(pos));
  } else if (OB_FAIL(serialization::encode_i64(buf, limit, new_pos, next_file_id_))) {
    LOG_WARN("serialize failed", K(ret));
  } else {
    pos = new_pos;
  }
  return ret;
}

int ObStorageLogWriter::ObSerializableBuffer::serialize(char* buf, int64_t limit, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (pos + len_ > limit) {
    ret = OB_SERIALIZE_ERROR;
    LOG_WARN("serialize failed", K(ret), K(pos), K_(len), K(limit));
  } else {
    MEMCPY(buf + pos, buf_, len_);
    pos += len_;
  }
  return ret;
}

ObStorageLogWriter::ObStorageLogWriter()
    : is_inited_(false),
      log_buffers_(),
      log_buffer_size_(0),
      log_item_allocator_(),
      slog_items_(),
      file_size_(0),
      build_log_mutex_(),
      build_cursor_(),
      nop_log_(),
      nop_log_buf_(),
      switch_log_(),
      switch_log_buf_(),
      log_dir_(NULL),
      cur_file_id_(-1),
      write_cursor_(),
      flush_cursor_(),
      is_ok_(false),
      write_failed_times_(0),
      file_store_(nullptr),
      batch_write_buf_(nullptr),
      batch_write_len_(0),
      batch_limit_size_(0)
{}

ObStorageLogWriter::~ObStorageLogWriter()
{
  destroy();
}

int ObStorageLogWriter::init(
    const char* log_dir, const int64_t log_file_size, const int64_t max_log_size, const int64_t max_trans_cnt)
{
  int ret = OB_SUCCESS;
  ObBaseLogWriterCfg log_cfg;
  log_cfg.max_buffer_item_cnt_ = ObBaseLogWriterCfg::DEFAULT_MAX_BUFFER_ITEM_CNT;
  log_cfg.group_commit_max_wait_us_ = FLUSH_THREAD_IDLE_INTERVAL_US;
  log_cfg.group_commit_min_item_cnt_ = 1;
  log_cfg.group_commit_max_item_cnt_ = ObBaseLogWriterCfg::DEFAULT_MAX_BUFFER_ITEM_CNT;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (NULL == log_dir || log_file_size < 2 * LOG_FILE_ALIGN_SIZE || max_log_size <= 0 || max_trans_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(log_dir), K(log_file_size), K(max_log_size), K(max_trans_cnt));
  } else if (OB_FAIL(ObBaseLogWriter::init(log_cfg))) {
    LOG_WARN("init ObBaseLogWriter failed", K(ret));
  } else if (OB_FAIL(prepare_log_buffers(max_trans_cnt, max_log_size))) {
    LOG_WARN("prepare_log_buffers failed", K(ret), K(max_trans_cnt), K(max_log_size));
  } else if (OB_FAIL(prepare_log_items(max_trans_cnt))) {
    LOG_WARN("prepare_log_items failed", K(ret), K(max_trans_cnt));
  } else {
    batch_write_len_ = 0;
    batch_limit_size_ = (4 << 10);
    batch_write_buf_ = static_cast<char*>(ob_malloc_align(DIO_ALIGN_SIZE, batch_limit_size_, "SlogBatchBuffer"));
    if (OB_ISNULL(batch_write_buf_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc batch write buffer", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (NULL ==
        (file_store_ = ObLogStoreFactory::create(log_dir, log_file_size, clog::ObLogWritePoolType::SLOG_WRITE_POOL))) {
      ret = OB_INIT_FAIL;
      LOG_WARN("create file store failed.", K(ret));
    } else {
      // build log context params
      file_size_ = log_file_size;
      build_cursor_.reset();
      nop_log_.reset();
      nop_log_.server_ = GCONF.self_addr_;
      memset(nop_log_buf_, 0, sizeof(nop_log_buf_));
      switch_log_.reset();
      memset(switch_log_buf_, 0, sizeof(switch_log_buf_));

      // flush log context params
      log_dir_ = strdup(log_dir);
      cur_file_id_ = -1;
      write_cursor_.reset();
      flush_cursor_.reset();

      set_ok(false);
      write_failed_times_ = 0;
      is_inited_ = true;
    }
  }
  return ret;
}

void ObStorageLogWriter::destroy()
{
  destroy_log_buffers();
  slog_items_.destroy();
  file_size_ = 0;
  build_cursor_.reset();
  nop_log_.reset();
  memset(nop_log_buf_, 0, sizeof(nop_log_buf_));
  switch_log_.reset();
  memset(switch_log_buf_, 0, sizeof(switch_log_buf_));
  log_dir_ = NULL;
  cur_file_id_ = -1;
  write_cursor_.reset();
  flush_cursor_.reset();
  set_ok(false);
  write_failed_times_ = 0;
  ObLogStoreFactory::destroy(file_store_);
  log_item_allocator_.reset();
  ObBaseLogWriter::destroy();
  if (nullptr != batch_write_buf_) {
    ob_free_align(batch_write_buf_);
    batch_write_buf_ = nullptr;
  }
  batch_write_len_ = 0;
  is_inited_ = false;
}

int ObStorageLogWriter::start_log(const ObLogCursor& start_cursor)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!start_cursor.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(start_cursor));
  }

  if (OB_SUCC(ret)) {
    build_cursor_ = start_cursor;
    write_cursor_ = start_cursor;
    flush_cursor_ = start_cursor;
    set_ok(true);
    write_failed_times_ = 0;
    LOG_INFO("start log", K(start_cursor));
  }
  return ret;
}

int ObStorageLogWriter::flush_log(
    const LogCommand cmd, const ObBaseStorageLogBuffer& log_buffer, ObLogCursor& start_cursor)
{
  int ret = OB_SUCCESS;
  ObStorageLogItem* log_item = NULL;
  start_cursor.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (log_buffer.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(log_buffer));
  } else {
    {
      ObMutexGuard guard(build_log_mutex_);
      if (OB_FAIL(get_log_item(cmd, log_buffer, log_item))) {
        LOG_WARN("get_log_item failed", K(ret), K(cmd), K(log_buffer));
      } else if (OB_FAIL(append_log(*log_item))) {
        LOG_WARN("append_log failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(log_item->wait_flush_log(MAX_FLUSH_WAIT_TIME_MS))) {
        LOG_WARN("wait_flush_log failed", K(ret));
      } else {
        start_cursor = log_item->start_cursor_;
      }
    }

    // if wait_flush_log finished, we assume that logs are already flushed to disk,
    // otherwise flush log thread will retry until success,
    // so release log item is safe here, we won't check return value.
    release_log_item(log_item);
  }
  return ret;
}

int ObStorageLogWriter::delete_log_file(int64_t file_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (file_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(file_id));
  } else if (OB_FAIL(file_store_->delete_file(file_id))) {
    LOG_WARN("delete log file failed", K(ret), K(file_id));
  }
  return ret;
}

int ObStorageLogWriter::get_using_disk_space(int64_t& using_space) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ret = file_store_->get_total_used_size(using_space);
  }
  return ret;
}

int ObStorageLogWriter::prepare_log_buffers(const int64_t count, const int64_t log_buf_size)
{
  int ret = OB_SUCCESS;
  // invoked by init, don't check inited_
  if (count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(count));
  } else if (OB_FAIL(log_buffers_.init(count))) {
    LOG_WARN("log_buffers init failed", K(ret), K(count));
  } else {
    void* buf = NULL;
    const ObMemAttr attr(OB_SERVER_TENANT_ID, ObModIds::OB_SLOG_WRITER);
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (nullptr == (buf = ob_malloc_align(LOG_FILE_ALIGN_SIZE, log_buf_size, attr))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("allocate buffer failed", K(ret), K(log_buf_size), LITERAL_K(LOG_FILE_ALIGN_SIZE));
      } else if (OB_FAIL(log_buffers_.push(buf))) {
        LOG_ERROR("log_buffers push failed", K(ret), KP(buf));
      }
    }

    if (OB_SUCC(ret)) {
      log_buffer_size_ = log_buf_size + LOG_FILE_ALIGN_SIZE;
    }
  }
  return ret;
}

void ObStorageLogWriter::destroy_log_buffers()
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  if (IS_INIT) {
    while (OB_SUCC(log_buffers_.pop(buf))) {
      if (NULL == buf) {
        // don't set ret, should continue even error occur
        LOG_ERROR("buf in log_buffers is null", KP(buf));
      } else {
        ob_free_align(buf);
        buf = NULL;
      }
    }
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("log_buffers pop failed", K(ret));
    }
    log_buffers_.destroy();
  }
}

int ObStorageLogWriter::prepare_log_items(const int64_t count)
{
  int ret = OB_SUCCESS;
  // invoked by init, don't check inited_
  if (count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(count));
  } else if (OB_FAIL(slog_items_.init(count))) {
    LOG_WARN("log_items init failed", K(ret), K(count));
  } else {
    void* ptr = NULL;
    ObStorageLogItem* log_item = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (NULL == (ptr = log_item_allocator_.alloc(sizeof(ObStorageLogItem)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc failed", K(ret), "size", sizeof(ObStorageLogItem));
      } else {
        log_item = new (ptr) ObStorageLogItem();
        if (OB_FAIL(slog_items_.push(log_item))) {
          LOG_WARN("push failed", K(ret), KP(log_item));
        }
      }
    }
  }
  return ret;
}

int ObStorageLogWriter::get_log_item(
    const LogCommand cmd, const ObBaseStorageLogBuffer& data, ObStorageLogItem*& log_item)
{
  int ret = OB_SUCCESS;
  void* log_buffer = nullptr;
  log_item = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (data.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data));
  }

  if (OB_SUCC(ret)) {
    common::ObLogEntry log_entry;
    int64_t total_item_size = log_entry.get_serialize_size() + data.get_serialize_size() + LOG_BUF_RESERVED_SIZE;
    const bool alloc_locally = total_item_size > log_buffer_size_;
    if (alloc_locally) {
      total_item_size = upper_align(total_item_size, LOG_FILE_ALIGN_SIZE);
      LOG_INFO("large log item", K(log_buffer_size_), K(total_item_size));
    } else {
      total_item_size = log_buffer_size_;
    }

    if (total_item_size > LOG_ITEM_MAX_LENGTH) {
      ret = OB_SIZE_OVERFLOW;
      LOG_ERROR("log item too long", K(ret), K(total_item_size), LITERAL_K(LOG_ITEM_MAX_LENGTH));
    } else if (!alloc_locally && OB_FAIL(log_buffers_.pop(log_buffer))) {
      LOG_WARN("pop failed", K(ret));
    } else if (OB_FAIL(slog_items_.pop(log_item))) {
      LOG_WARN("pop log_item failed", K(ret));
    } else if (OB_FAIL(log_item->init(reinterpret_cast<char*>(log_buffer), total_item_size))) {
      LOG_WARN("init log_item failed", K(ret));
    } else {
      log_item->start_cursor_ = build_cursor_;
      if (OB_FAIL(build_log_item(cmd, data, log_item))) {
        LOG_WARN("build_log_item failed", K(ret), K(cmd), K(data));
      } else {
        log_item->end_cursor_ = build_cursor_;
      }
    }
  }

  if (OB_FAIL(ret)) {
    int temp_ret = OB_SUCCESS;
    if (NULL != log_buffer) {
      if (OB_SUCCESS != (temp_ret = log_buffers_.push(log_buffer))) {
        LOG_ERROR("push log_buffer failed", K(temp_ret), KP(log_buffer));
      }
    }
    if (NULL != log_item) {
      log_item->reset();
      if (OB_SUCCESS != (temp_ret = slog_items_.push(log_item))) {
        LOG_ERROR("push log_item failed", K(temp_ret), KP(log_item));
      }
    }
  }
  return ret;
}

int ObStorageLogWriter::build_log_item(
    const LogCommand cmd, const ObBaseStorageLogBuffer& data, ObStorageLogItem* log_item)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (data.is_empty() || NULL == log_item) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data), KP(log_item));
  } else if (OB_FAIL(fill_log(log_item, LOG_BUF_RESERVED_SIZE, cmd, data))) {
    LOG_WARN("fill_log failed", K(ret), KP(log_item), LITERAL_K(LOG_BUF_RESERVED_SIZE), K(cmd), K(data));
  } else if (OB_FAIL(fill_nop_log(log_item))) {
    LOG_WARN("fill_nop_log failed", K(ret), KP(log_item));
  } else if (OB_FAIL(switch_log_on_demand(log_item))) {
    LOG_WARN("switch_log_on_demand failed", K(ret), KP(log_item));
  }
  return ret;
}

int ObStorageLogWriter::fill_nop_log(ObStorageLogItem* log_item)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == log_item) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(log_item));
  } else if (OB_FAIL(nop_log_.advance())) {
    LOG_ERROR("nop_log advance failed", K(ret));
  } else if (OB_FAIL(nop_log_.serialize(nop_log_buf_, sizeof(nop_log_buf_), pos))) {
    LOG_ERROR("nop_log serialize failed", K(ret));
  } else {
    ObSerializableBuffer sb(nop_log_buf_, calc_nop_log_len(log_item->get_data_len(), pos));
    const int64_t reserved_len = 0;
    if (OB_FAIL(fill_log(log_item, reserved_len, OB_LOG_NOP, sb))) {
      LOG_WARN("fill_log failed", K(ret), KP(log_item), K(reserved_len), K(sb));
    }
  }
  return ret;
}

int64_t ObStorageLogWriter::calc_nop_log_len(int64_t pos, int64_t min_log_size)
{
  ObLogEntry entry;
  int64_t header_size = entry.get_serialize_size();
  return get_align_padding_size(pos + header_size + min_log_size, LOG_FILE_ALIGN_SIZE - 1) + min_log_size;
}

int ObStorageLogWriter::switch_log_on_demand(ObStorageLogItem* log_item)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == log_item) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(log_item));
  } else if (build_cursor_.offset_ + log_item->get_buf_size() <= file_size_) {
    // do nothing
  } else {
    int64_t pos = 0;
    switch_log_.set_next_file_id(build_cursor_.file_id_ + 1);
    if (OB_FAIL(switch_log_.serialize(switch_log_buf_, sizeof(switch_log_buf_), pos))) {
      LOG_WARN("switch_log serialize failed", K(ret));
    } else {
      ObLogEntry entry;
      int64_t header_size = entry.get_serialize_size();
      ObSerializableBuffer sb(switch_log_buf_, sizeof(switch_log_buf_) - header_size);
      const int64_t reserved_len = 0;
      if (OB_FAIL(fill_log(log_item, reserved_len, OB_LOG_SWITCH_LOG, sb))) {
        LOG_WARN("fill_log failed", K(ret), KP(log_item), K(reserved_len), K(sb));
      }
    }
  }
  return ret;
}

void ObStorageLogWriter::release_log_item(ObStorageLogItem* log_item)
{
  int ret = OB_SUCCESS;
  if (NULL != log_item) {
    if (!log_item->is_local() && NULL != log_item->get_buf()) {
      if (OB_FAIL(log_buffers_.push(log_item->get_buf()))) {
        LOG_ERROR("push failed", K(ret), "log_item", *log_item);
      }
    }

    // continue even error occurs
    log_item->reset();
    if (OB_FAIL(slog_items_.push(log_item))) {
      LOG_ERROR("push failed", K(ret), KP(log_item));
    }
  }
}

void ObStorageLogWriter::process_log_items(ObIBaseLogItem** items, const int64_t item_cnt, int64_t& finish_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == items || item_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(items), K(item_cnt));
  } else if (OB_FAIL(process_logs_in_batch(items, item_cnt, finish_cnt))) {
    LOG_WARN("ofs process failed", K(ret), KP(items), K(item_cnt), K(finish_cnt));
  }
}

int ObStorageLogWriter::process_logs_in_batch(
    common::ObIBaseLogItem** items, const int64_t item_cnt, int64_t& finish_cnt)
{
  int ret = OB_SUCCESS;
  int64_t file_id = -1;
  int64_t sync_index = -1;
  int64_t cur_aggregate_len = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < item_cnt; ++i) {
    ObStorageLogItem* log_item = reinterpret_cast<ObStorageLogItem*>(items[i]);
    if (OB_ISNULL(log_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("log_item in items is null", K(ret), KP(log_item));
    } else if (!log_item->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("log_item is invalid", K(ret), "log_item", *log_item);
    } else if (file_id != log_item->start_cursor_.file_id_) {
      // group sync log already written to OS buffer cache, then wake up
      if (0 != i) {
        if (OB_FAIL(write_and_sync_logs(items, item_cnt, sync_index, i - 1))) {
          LOG_ERROR("write_and_sync_logs failed", K(ret), K(sync_index), K(i));
        } else {
          cur_aggregate_len = 0;
        }
      }

      if (OB_SUCC(ret)) {  // prepare fd for next log file
        if (OB_FAIL(prepare_fd(log_item->start_cursor_.file_id_))) {
          LOG_WARN("prepare_fd failed", K(ret));
        } else {
          file_id = log_item->start_cursor_.file_id_;
        }
      }
    } else if (cur_aggregate_len + log_item->get_data_len() > batch_limit_size_ && i >= 1) {
      if (OB_FAIL(write_and_sync_logs(items, item_cnt, sync_index, i - 1))) {
        LOG_ERROR("write_and_sync_logs failed", K(ret), K(sync_index), K(i));
      } else {
        cur_aggregate_len = 0;
      }
    }

    if (OB_SUCC(ret)) {
      cur_aggregate_len += log_item->get_data_len();
    }
  }  // end-for-loop

  if (OB_SUCC(ret)) {  // sync left logs written
    if (OB_FAIL(write_and_sync_logs(items, item_cnt, sync_index, item_cnt - 1))) {
      LOG_ERROR("write_and_sync_logs failed", K(ret), K(sync_index), K(item_cnt));
    }
  }

  if (OB_FAIL(ret)) {  // wake up logs not sync
    for (int64_t i = sync_index + 1; i < item_cnt; ++i) {
      if (NULL == items[i]) {
        LOG_ERROR("log_item in items is null", K(ret), KP(items[i]));
      } else {
        ObStorageLogItem* log_item = reinterpret_cast<ObStorageLogItem*>(items[i]);
        // aggregate_logs_to_buffer may fail for invalid cursor arguments,
        // we just finish flush and let upper layer retry
        log_item->finish_flush(ret);
      }
    }

    ObMutexGuard guard(build_log_mutex_);
    ObStorageLogItem* log_item = reinterpret_cast<ObStorageLogItem*>(items[sync_index + 1]);
    build_cursor_ = log_item->start_cursor_;
    write_cursor_ = log_item->start_cursor_;
    LOG_ERROR("slog write failed, record cursor", K(ret), K_(build_cursor), K_(write_cursor));
  }
  finish_cnt = item_cnt;
  return ret;
}

int ObStorageLogWriter::prepare_fd(const int64_t file_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (file_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(file_id));
  } else if (file_id == cur_file_id_) {
    // do nothing
  } else {
    // close last log file opened
    if (-1 != cur_file_id_ && OB_FAIL(file_store_->close())) {
      ret = OB_IO_ERROR;
      LOG_ERROR("close error", K_(cur_file_id), KERRMSG);
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(file_store_->open(file_id))) {
        LOG_ERROR("open log file failed", K(ret), K(file_id), KERRMSG);
      } else {
        cur_file_id_ = file_id;
        LOG_INFO("open slog file", K_(cur_file_id));
      }
    }
  }
  return ret;
}

int ObStorageLogWriter::aggregate_logs_to_buffer(common::ObIBaseLogItem** items, const int64_t item_cnt,
    const int64_t sync_idx, const int64_t cur_idx, char*& write_buf, int64_t& write_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cur_idx >= item_cnt || cur_idx < 0 || sync_idx >= item_cnt || sync_idx >= cur_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(item_cnt), K(cur_idx), K(sync_idx));
  } else if (1 == cur_idx - sync_idx) {
    ObStorageLogItem& single_item = *(reinterpret_cast<ObStorageLogItem*>(items[cur_idx]));
    write_buf = single_item.get_buf();
    write_len = single_item.get_data_len();
    write_cursor_ = single_item.end_cursor_;
  } else {
    for (int64_t i = sync_idx + 1; OB_SUCC(ret) && i <= cur_idx; ++i) {
      if (OB_ISNULL(items[i])) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid arguments", K(ret), K(item_cnt), K(i), KP(items[i]));
      } else {
        ObStorageLogItem& log_item = *(reinterpret_cast<ObStorageLogItem*>(items[i]));
        if (!log_item.is_valid() || cur_file_id_ != log_item.start_cursor_.file_id_ ||
            (cur_file_id_ != log_item.end_cursor_.file_id_ && cur_file_id_ != log_item.end_cursor_.file_id_ - 1)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid arguments", K(ret), K(log_item), K_(cur_file_id));
        } else if (!write_cursor_.equal(log_item.start_cursor_)) {
          ret = OB_DISCONTINUOUS_LOG;
          LOG_ERROR("log not continuous", K(ret), K(log_item), K_(write_cursor));
        } else {
          MEMCPY((batch_write_buf_) + batch_write_len_, log_item.get_buf(), log_item.get_data_len());
          batch_write_len_ += log_item.get_data_len();
          write_cursor_ = log_item.end_cursor_;
        }
      }
    }

    if (OB_SUCC(ret)) {
      write_buf = batch_write_buf_;
      write_len = batch_write_len_;
    }
  }
  return ret;
}

int ObStorageLogWriter::write_logs_local(common::ObIBaseLogItem** items, const int64_t item_cnt, int64_t& sync_idx,
    const int64_t cur_idx, char* write_buf, const int64_t write_len)
{
  int ret = OB_SUCCESS;
  const int64_t offset = flush_cursor_.offset_;
  const int64_t start_ts = ObTimeUtility::fast_current_time();

  // retry until success
  while (OB_SUCC(ret)) {
    ret = file_store_->write(write_buf, write_len, offset);
    if (OB_SUCC(ret)) {
      if (--write_failed_times_ <= 0) {
        write_failed_times_ = 0;
        set_ok(true);
      }
      break;
    } else {
      if (++write_failed_times_ >= 10) {
        write_failed_times_ = 10;
        set_ok(false);
      }
      if (REACH_TIME_INTERVAL(30 * 1000 * 1000)) {
        LOG_ERROR("log write failed",
            K(ret),
            K(write_len),
            K(flush_cursor_),
            K(cur_file_id_),
            K(item_cnt),
            K(sync_idx),
            K(cur_idx));
      } else {
        LOG_WARN("log write failed",
            K(ret),
            K(write_len),
            K(flush_cursor_),
            K(cur_file_id_),
            K(item_cnt),
            K(sync_idx),
            K(cur_idx));
      }
      ret = OB_SUCCESS;
    }
  }

  if (OB_SUCC(ret)) {
    batch_write_len_ = 0;
    const int64_t duration = ObTimeUtility::fast_current_time() - start_ts;
    if (duration > 10000) {
      LOG_INFO("slow write logs local",
          K(duration),
          K(write_len),
          K(cur_idx),
          K(sync_idx),
          K(item_cnt),
          "sync_cnt",
          cur_idx - sync_idx);
    }

    if (OB_FAIL(sync_log(items, sync_idx, cur_idx))) {
      LOG_ERROR("sync_log failed", K(ret), K(sync_idx), K(cur_idx));
    }
  }
  return ret;
}

int ObStorageLogWriter::advance_single_item(const int64_t cur_file_id, ObStorageLogItem& log_item)
{
  int ret = OB_SUCCESS;
  const int64_t next_file_id = cur_file_id + 1;
  const int64_t item_len = log_item.end_cursor_.offset_ - log_item.start_cursor_.offset_;
  const int64_t file_id_step = log_item.start_cursor_.file_id_ - cur_file_id;

  if (OB_UNLIKELY(cur_file_id < 0 || !log_item.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(cur_file_id), K(log_item));
  } else {
    if (log_item.start_cursor_.file_id_ < cur_file_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected log item", K(log_item), K(cur_file_id));
    } else if (log_item.start_cursor_.file_id_ == cur_file_id) {
      // next_item is in the same file, update file_id_ and offset_
      log_item.start_cursor_.file_id_ = build_cursor_.file_id_;
      log_item.start_cursor_.offset_ = build_cursor_.offset_;
      log_item.end_cursor_.file_id_ = build_cursor_.file_id_;
      log_item.end_cursor_.offset_ = build_cursor_.offset_ + item_len;
    } else {
      // next_item has switch to new file, only push forward file_id_
      log_item.start_cursor_.file_id_ = next_file_id + file_id_step;
      log_item.end_cursor_.file_id_ = next_file_id + file_id_step;
    }
    build_cursor_ = log_item.end_cursor_;
  }
  return ret;
}

int ObStorageLogWriter::advance_log_items(common::ObIBaseLogItem** items, const int64_t item_cnt, const int64_t cur_idx)
{
  int ret = OB_SUCCESS;

  // process_items_
  for (int64_t i = cur_idx + 1; OB_SUCC(ret) && i < item_cnt; ++i) {
    ObStorageLogItem& next_item = *(reinterpret_cast<ObStorageLogItem*>(items[i]));
    if (OB_FAIL(advance_single_item(cur_file_id_, next_item))) {
      LOG_WARN("advance next item fail", K_(cur_file_id), K(next_item));
    } else {
      FLOG_INFO("advance process_items_ success", K_(build_cursor), K(next_item));
    }
  }
  // log_items_
  if (OB_SUCC(ret) && log_item_push_idx_ - log_item_pop_idx_ > 0) {
    const int64_t start_idx = log_item_pop_idx_ % max_buffer_item_cnt_;
    const int64_t end_idx = log_item_push_idx_ % max_buffer_item_cnt_;
    if (end_idx > start_idx) {
      for (int64_t i = start_idx; OB_SUCC(ret) && i < end_idx; ++i) {
        ObStorageLogItem& next_item = *(reinterpret_cast<ObStorageLogItem*>(log_items_[i]));
        if (OB_FAIL(advance_single_item(cur_file_id_, next_item))) {
          LOG_WARN("advance next item fail", K_(cur_file_id), K(next_item));
        } else {
          FLOG_INFO("advance log_items_ success", K(build_cursor_), K(next_item));
        }
      }
    } else {
      for (int64_t i = start_idx; OB_SUCC(ret) && i < max_buffer_item_cnt_; ++i) {
        ObStorageLogItem& next_item = *(reinterpret_cast<ObStorageLogItem*>(log_items_[i]));
        if (OB_FAIL(advance_single_item(cur_file_id_, next_item))) {
          LOG_WARN("advance next item fail", K_(cur_file_id), K(next_item));
        } else {
          FLOG_INFO("advance log_items_ success", K(build_cursor_), K(next_item));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < end_idx; ++i) {
        ObStorageLogItem& next_item = *(reinterpret_cast<ObStorageLogItem*>(log_items_[i]));
        if (OB_FAIL(advance_single_item(cur_file_id_, next_item))) {
          LOG_WARN("advance next item fail", K(cur_file_id_), K(next_item));
        } else {
          FLOG_INFO("advance log_items_ success", K_(build_cursor), K(next_item));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    cur_file_id_++;
  }
  return ret;
}

int ObStorageLogWriter::write_and_sync_logs(
    common::ObIBaseLogItem** items, const int64_t item_cnt, int64_t& sync_idx, const int64_t cur_idx)
{
  int ret = OB_SUCCESS;
  char* write_buf = nullptr;
  int64_t write_len = 0;
  if (OB_FAIL(aggregate_logs_to_buffer(items, item_cnt, sync_idx, cur_idx, write_buf, write_len))) {
    LOG_WARN("aggregate logs fail", K(ret), K_(flush_cursor), K_(cur_file_id), K(item_cnt), K(sync_idx), K(cur_idx));
  } else if (OB_ISNULL(write_buf) || OB_UNLIKELY(cur_idx - sync_idx > 1 && write_len > batch_limit_size_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected write", K(ret), KP(write_buf), K(write_len), K(item_cnt), K(sync_idx), K(cur_idx));
  } else {
    if (OB_FAIL(write_logs_local(items, item_cnt, sync_idx, cur_idx, write_buf, write_len))) {
      LOG_WARN("write local fail", K(ret), K(item_cnt), K(sync_idx), K(cur_idx), K_(flush_cursor));
    }
  }
  return ret;
}

int ObStorageLogWriter::sync_log(common::ObIBaseLogItem** items, int64_t& sync_index, const int64_t write_index)
{
  int ret = OB_SUCCESS;
  if (NULL == items || write_index < 0 || sync_index < -1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(items), K(write_index), K(sync_index));
  } else {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    if (OB_FAIL(file_store_->fsync())) {
      ret = OB_IO_ERROR;
      LOG_ERROR("fsync failed", K(ret), K_(cur_file_id));
    } else {
      for (int64_t i = sync_index + 1; i <= write_index; ++i) {
        ObStorageLogItem* log_item = reinterpret_cast<ObStorageLogItem*>(items[i]);
        if (i == write_index) {
          DEBUG_SYNC(BEFORE_SLOG_UPDATE_FLUSH_CURSOR);
          flush_cursor_ = log_item->end_cursor_;
        }
        log_item->finish_flush(OB_SUCCESS);
      }
      const int64_t duration = ObTimeUtility::fast_current_time() - start_ts;
      if (duration > 10000) {
        LOG_INFO("slow fsync end ", K(duration), K_(flush_cursor), "sync_cnt", write_index - sync_index);
      }
      sync_index = write_index;
    }
  }
  return ret;
}
}  // end namespace blocksstable
}  // end namespace oceanbase
