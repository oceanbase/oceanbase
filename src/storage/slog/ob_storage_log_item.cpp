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

#include "ob_storage_log_item.h"
#include "lib/oblog/ob_log.h"
#include "lib/wait_event/ob_wait_event.h"
#include "lib/alloc/alloc_struct.h"
#include "lib/utility/ob_tracepoint.h"
#include "ob_storage_log_batch_header.h"
#include "ob_storage_log_entry.h"
#include "ob_storage_log_replayer.h"

namespace oceanbase
{
using namespace common;
using namespace lib;

namespace storage
{
ObStorageLogItem::ObStorageLogItem()
  : start_cursor_(), end_cursor_(),
    is_inited_(false), is_local_(false),
    buf_size_(0), buf_(nullptr),
    len_(0), log_data_len_(0), seq_(0),
    flush_cond_(), flush_finish_(false),
    flush_ret_(OB_SUCCESS), local_offset_arr_(nullptr), log_cnt_(0)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(flush_cond_.init(ObWaitEventIds::SLOG_FLUSH_COND_WAIT))) {
    ob_abort();
  }
}

ObStorageLogItem::~ObStorageLogItem()
{
  destroy();
}

int ObStorageLogItem::init(
    char *buf,
    const int64_t buf_size,
    const int64_t align_size,
    const int64_t num)
{
  int ret = OB_SUCCESS;

  if(IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_REDO_LOG(WARN, "Init twice.", K(ret));
  } else if (OB_UNLIKELY(buf_size <= 0 || align_size <= 0 || num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid argument.", K(ret), K(buf_size), K(align_size), K(num));
  } else if (num <= OFFSET_ARR_SIZE) {
    local_offset_arr_ = offset_arr_;
  } else if (OB_ISNULL(local_offset_arr_ =
      (int64_t *)ob_malloc(num * sizeof(int64_t), "OffsetArray"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_REDO_LOG(ERROR, "Fail to allocate memory for offset array", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (nullptr == buf) {
      const ObMemAttr attr(OB_SERVER_TENANT_ID, ObModIds::OB_SLOG_WRITER);
      if (nullptr == (buf_ = reinterpret_cast<char *>(ob_malloc_align(align_size, buf_size, attr)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_REDO_LOG(WARN, "Fail to alloc local buf", K(ret), K(buf_size), K(attr));
      } else {
        is_local_ = true;
      }
    } else {
      buf_ = buf;
      is_local_ = false;
    }

    if (OB_SUCC(ret)) {
      buf_size_ = buf_size;
      log_cnt_ = num;
      is_inited_ = true;
    }
  }

  if (IS_NOT_INIT) {
    destroy();
  }

  return ret;
}

bool ObStorageLogItem::is_valid() const
{
  return is_inited_ && start_cursor_.is_valid() && end_cursor_.is_valid()
      && NULL != buf_ && len_ >= 0 && log_data_len_ >= 0 && seq_ > 0 && buf_size_ >= 0;
}

void ObStorageLogItem::destroy()
{
  if (is_local_ && nullptr != buf_) {
    ob_free_align(buf_);
    buf_ = nullptr;
  }
  is_local_ = false;
  start_cursor_.reset();
  end_cursor_.reset();
  buf_ = nullptr;
  len_ = 0;
  log_data_len_ = 0;
  seq_ = 0;
  buf_size_ = 0;
  flush_finish_ = false;
  flush_ret_ = OB_SUCCESS;
  is_inited_ = false;
  log_cnt_ = 0;
  if (nullptr != local_offset_arr_ && local_offset_arr_ != offset_arr_) {
    ob_free(local_offset_arr_);
  }
}

int ObStorageLogItem::wait_flush_log(const uint64_t max_wait_time)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "Not init", K(ret));
  } else {
    ObThreadCondGuard guard(flush_cond_);
    while(!flush_finish_) {
      if (OB_FAIL(flush_cond_.wait(max_wait_time))) {
        if (ret == OB_TIMEOUT) {
          STORAGE_REDO_LOG(WARN, "flush_cond wait timeout", K(ret), K(max_wait_time), K(*this));
        } else if (REACH_TIME_INTERVAL(30 * 1000 * 1000)) {
          STORAGE_REDO_LOG(ERROR, "Fail to wait log flush (reach time-interval 30s)",
              K(ret), K(max_wait_time), K(*this));
        } else {
          STORAGE_REDO_LOG(WARN, "Fail to wait log flush", K(ret), K(max_wait_time), K(*this));
        }
      }
    }
#ifdef ERRSIM
    ret = OB_E(EventTable::EN_SLOG_WAIT_FLUSH_LOG) OB_SUCCESS;
#endif
    if (OB_LIKELY(OB_SUCC(ret))) {
      ret = flush_ret_;
    } else {
      STORAGE_REDO_LOG(ERROR, "Error occurs after wait finishes",
          K(ret), K_(flush_finish), K_(flush_ret));
    }
  }

  return ret;
}

void ObStorageLogItem::finish_flush(const int flush_ret)
{
  if (IS_NOT_INIT) {
    flush_ret_ = OB_NOT_INIT;
    STORAGE_REDO_LOG_RET(WARN, flush_ret_, "Not init", K_(flush_ret));
  } else {
    ObThreadCondGuard guard(flush_cond_);
    flush_ret_ = flush_ret;
    flush_finish_ = true;
    flush_cond_.signal();
  }
}

int ObStorageLogItem::set_log_data_len(const int64_t log_data_len)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "Not init", K(ret));
  } else if (OB_UNLIKELY(log_data_len < 0 || log_data_len > len_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid argument.", K(ret), K(log_data_len));
  } else {
    log_data_len_ = log_data_len;
  }
  return ret;
}

int ObStorageLogItem::set_data_len(const int64_t len)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "Not init", K(ret));
  } else if (OB_UNLIKELY(len < 0 || len > buf_size_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid argument.", K(ret), K(len));
  } else {
    len_ = len;
  }
  return ret;
}

int ObStorageLogItem::fill_log(
    const int64_t seq,
    const ObStorageLogParam &log_param,
    const int index)
{
  int ret = OB_SUCCESS;
  enum ObRedoLogMainType main_type = ObRedoLogMainType::OB_REDO_LOG_MAX;
  enum ObRedoLogSubType sub_type;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "Not init", K(ret));
  } else if (OB_UNLIKELY(seq < 0) || OB_UNLIKELY(!log_param.is_valid()) || OB_UNLIKELY(index >= log_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "invalid args", K(ret), K(seq), K(log_param), K(index));
  } else if (FALSE_IT(ObIRedoModule::parse_cmd(log_param.cmd_, main_type, sub_type))) {
    // do nothing
  } else if (ObRedoLogMainType::OB_REDO_LOG_SYS != main_type) {
    // log item's seq is equal to the first log's seq
    if (0 == index) {
      seq_ = seq;
    }
    local_offset_arr_[index] = len_;
  }

  if (OB_SUCC(ret)) {
    ObStorageLogEntry entry;
    const int64_t entry_size = entry.get_serialize_size();
    const ObIBaseStorageLogEntry *data = log_param.data_;
    const int64_t data_len = data->get_serialize_size();

    int64_t entry_pos = len_;
    const int64_t entry_start_pos = entry_pos;
    const int64_t data_start_pos = entry_pos + entry_size;
    len_ = data_start_pos;
    int64_t left_space = buf_size_ - entry_pos;

    if (entry_size + data_len > left_space) {
      ret = common::OB_LOG_TOO_LARGE;
      STORAGE_REDO_LOG(WARN, "Log is too large", K(ret),
          "log_size", (entry_size + data_len), K(left_space));
    } else if (OB_FAIL(data->serialize(buf_, len_ + data_len, len_))) {
      STORAGE_REDO_LOG(WARN, "Fail to serialize data", K(ret), K(len_));
    } else if (OB_UNLIKELY(len_ - data_start_pos != data_len)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_REDO_LOG(ERROR, "data actual serialize size is not equal to expected size",
          K(ret), K(data_start_pos), K_(len), K(data_len));
    } else if (OB_FAIL(entry.fill_entry(buf_ + data_start_pos, data_len,
        log_param.cmd_, seq))) {
      STORAGE_REDO_LOG(WARN, "Fail to fill header", K(ret), K(data_start_pos));
    } else if (OB_FAIL(entry.serialize(buf_, entry_pos + entry_size, entry_pos))) {
      STORAGE_REDO_LOG(WARN, "Fail to serialize entry", K(ret), K(entry_pos));
    } else if (OB_UNLIKELY(entry_pos - entry_start_pos != entry_size)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_REDO_LOG(ERROR, "entry actual serialize size is not equal to expected size",
          K(ret), K(entry_start_pos), K(entry_start_pos), K(entry_size));
    } else if (ObRedoLogMainType::OB_REDO_LOG_SYS != main_type) {
      log_data_len_ = len_;
    }
  }

  return ret;
}

int ObStorageLogItem::fill_batch_header(
    const int32 data_len,
    const int16_t cnt,
    int64_t pos)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "Not init", K(ret));
  } else if (OB_UNLIKELY(data_len <= 0 || cnt <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid arguments", K(ret), K(data_len), K(cnt), K(pos));
  } else {
    ObStorageLogBatchHeader batch_header;
    batch_header.cnt_ = cnt;
    batch_header.total_len_ = data_len;
    batch_header.checksum_ =
        batch_header.cal_checksum(buf_ + pos + batch_header.get_serialize_size(), data_len);
    if (OB_FAIL(batch_header.serialize(buf_, pos + batch_header.get_serialize_size(), pos))) {
      STORAGE_REDO_LOG(WARN, "Fail to serialize batch header", K(ret), K(pos), K(ret));
    }
  }

  return ret;
}

}
}
