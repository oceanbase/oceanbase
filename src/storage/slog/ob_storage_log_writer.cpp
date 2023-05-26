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

#include "common/log/ob_log_constants.h"
#include "lib/file/ob_file.h"
#include "share/config/ob_server_config.h"
#include "share/ob_debug_sync_point.h"
#include "share/ob_debug_sync.h"
#include "share/rc/ob_tenant_base.h"
#include "share/redolog/ob_log_policy.h"
#include "storage/blocksstable/ob_log_file_spec.h"
#include "storage/slog/ob_storage_log_batch_header.h"
#include "storage/slog/ob_storage_log_entry.h"
#include "storage/slog/ob_storage_log_item.h"
#include "storage/slog/ob_storage_log_replayer.h"
#include "storage/slog/ob_storage_log_writer.h"
#include "share/ob_thread_mgr.h"
#include <errno.h>
#include <limits.h>
#include <string.h>
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
using namespace lib;
using namespace common;

namespace storage
{

ObStorageLogWriter::ObStorageLogWriter()
  : is_inited_(false), flush_seq_(0), write_align_size_(0),
    file_size_(0),  write_offset_(0), cursor_(),
    retry_write_policy_(ObLogRetryWritePolicy::INVALID_RETRY_WRITE),
    log_write_policy_(ObLogWritePolicy::INVALID_WRITE), nop_log_(),
    nop_data_param_(), file_handler_(), batch_write_buf_(), slog_write_runner_()
{
}

ObStorageLogWriter::~ObStorageLogWriter()
{
  destroy();
}

int ObStorageLogWriter::init(
    const char *log_dir,
    const int64_t log_file_size,
    const int64_t max_log_size,
    const blocksstable::ObLogFileSpec &log_file_spec,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  const char *thread_name = "OB_SLOG";
  ObBaseLogWriterCfg log_cfg;
  log_cfg.max_buffer_item_cnt_ = ObBaseLogWriterCfg::DEFAULT_MAX_BUFFER_ITEM_CNT;
  log_cfg.group_commit_max_wait_us_ = FLUSH_THREAD_IDLE_INTERVAL_US;
  log_cfg.group_commit_min_item_cnt_ = 1;
  log_cfg.group_commit_max_item_cnt_ = ObBaseLogWriterCfg::DEFAULT_MAX_BUFFER_ITEM_CNT;
  int64_t buf_size = max_log_size + ObLogConstants::LOG_FILE_ALIGN_SIZE;
  write_align_size_ = ObLogConstants::LOG_FILE_ALIGN_SIZE;
  file_size_ = log_file_size;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_REDO_LOG(WARN, "Init twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == log_dir || max_log_size <= 0 || log_file_size < max_log_size)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid arguments", K(ret), KP(log_dir),
        K(log_file_size), K(max_log_size));
  } else if (OB_FAIL(ObBaseLogWriter::init(log_cfg, thread_name, tenant_id))) {
    STORAGE_REDO_LOG(WARN, "Fail to init ObBaseLogWriter", K(ret));
  } else if (OB_FAIL(ObLogPolicyParser::parse_retry_write_policy(log_file_spec.retry_write_policy_,
      retry_write_policy_))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Fail to parse retry write policy", K(ret), K(log_file_spec));
  } else if (OB_FAIL(ObLogPolicyParser::parse_log_write_policy(log_file_spec.log_write_policy_,
      log_write_policy_))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Fail to parse log write policy", K(ret), K(log_file_spec));
  } else if (OB_FAIL(nop_log_.init(tenant_id, ObLogConstants::LOG_FILE_ALIGN_SIZE))) {
    STORAGE_REDO_LOG(WARN, "Fail to init nop log", K(ret));
  } else if (OB_FAIL(batch_write_buf_.init(ObLogConstants::LOG_FILE_ALIGN_SIZE, buf_size, tenant_id))) {
    STORAGE_REDO_LOG(WARN, "Fail to init batch write buf", K(ret), K(buf_size));
  } else if (OB_FAIL(file_handler_.init(log_dir, log_file_size, tenant_id))) {
    STORAGE_REDO_LOG(WARN, "Fail to create file handler", K(ret), KP(log_dir));
  } else if (OB_SERVER_TENANT_ID != tenant_id && OB_FAIL(slog_write_runner_.init(this))) {
    STORAGE_REDO_LOG(WARN, "Fail to init slog write runner.", K(ret));
  } else {
    is_inited_ = true;
    STORAGE_REDO_LOG(INFO, "Successfully init slog writer", K(ret), KP(log_dir),
          K(log_file_size), K(max_log_size), K(log_file_spec));
  }

  if (IS_NOT_INIT) {
    destroy();
  }

  return ret;
}

int ObStorageLogWriter::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "ObStorageLogWriter hasn't been inited.", K(ret), K(is_inited_));
  } else if (!has_stopped_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_REDO_LOG(WARN, "ObStorageLogWriter has started.", K(ret), K(has_stopped_));
  } else if (FALSE_IT(has_stopped_ = false)) {
    // has_stopped_ must be set before flush thread run. Otherwise it might caused thread exit
    // No worry about councurrent slog write, because the ObStorageLogger hasn't started yet.
  } else if (OB_FAIL(slog_write_runner_.start())) {
    STORAGE_REDO_LOG(WARN, "fail to start ObSlogWriteRunner's thread", K(ret));
  }
  return ret;
}

void ObStorageLogWriter::wait()
{
  if (has_stopped_ && is_inited_) {
    slog_write_runner_.stop();
    slog_write_runner_.wait();
  }
}

void ObStorageLogWriter::destroy()
{
  flush_seq_ = 0;
  write_align_size_ = 0;
  file_size_ = 0;
  write_offset_ = 0;
  cursor_.reset();
  file_handler_.destroy();
  ObBaseLogWriter::destroy();
  nop_log_.destroy();
  batch_write_buf_.destroy();
  slog_write_runner_.destroy();
  is_inited_ = false;
}

ObLogCursor ObStorageLogWriter::get_cur_cursor()
{
  ObSpinLockGuard guard(cursor_lock_);
  return cursor_;
}

int ObStorageLogWriter::start_log(const ObLogCursor &start_cursor)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "Not init.", K(ret));
  } else if (OB_UNLIKELY(!start_cursor.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid arguments.", K(ret));
  } else if (OB_FAIL(file_handler_.open(start_cursor.file_id_))) {
    STORAGE_REDO_LOG(WARN, "Fail to open file", K(ret), K(cursor_.file_id_));
  } else {
    write_offset_ = start_cursor.offset_;
    cursor_ = start_cursor;
    flush_seq_ = start_cursor.log_id_;
    STORAGE_REDO_LOG(INFO, "slog writer start log", K(ret), K(start_cursor));
  }
  return ret;
}

int ObStorageLogWriter::delete_log_file(int64_t file_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "Slog writer has not been inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(file_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid argument", K(ret), K(file_id));
  } else if (OB_FAIL(file_handler_.delete_file(file_id))) {
    STORAGE_REDO_LOG(WARN, "Fail to delete log file", K(ret), K(file_id));
  }

  return ret;
}

int ObStorageLogWriter::get_using_disk_space(int64_t &using_space) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "Slog writer has not been inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(file_handler_.get_total_used_size(using_space))) {
    STORAGE_REDO_LOG(WARN, "Fail to get the used size", K(ret), K(using_space));
  }
  return ret;
}

int ObStorageLogWriter::fill_nop_log(
    ObStorageLogItem *log_item,
    const int64_t occupied_len) {
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(nullptr == log_item || occupied_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(ERROR, "Invalid argument", K(ret), K(occupied_len));
  } else if (0 == occupied_len % write_align_size_) {
    // log item buffer is aligned, no need to fill nop log
  } else {
    int64_t padding_buffer_size = nop_log_.get_fixed_serialize_len(occupied_len);
    if (0 == padding_buffer_size) {
      padding_buffer_size = write_align_size_;
    }
    if (OB_FAIL(nop_log_.set_needed_size(padding_buffer_size))) {
      STORAGE_REDO_LOG(ERROR, "Fail to set nop_log's size", K(ret), K(padding_buffer_size));
    } else {
      nop_data_param_.reset();
      nop_data_param_.data_ = &nop_log_;
      nop_data_param_.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_SYS,
          ObRedoLogSubType::OB_REDO_LOG_NOP);
      ObStorageLogEntry entry;
      ObStorageLogBatchHeader batch_header;
      int64_t header_pos = log_item->get_data_len();

      if (OB_FAIL(log_item->set_data_len(header_pos + batch_header.get_serialize_size()))) {
        STORAGE_REDO_LOG(WARN, "Fail to set log item's data length", K(ret));
      } else if (OB_FAIL(log_item->fill_log(ObLogConstants::NOP_SWITCH_LOG_SEQ, nop_data_param_, 0))) {
        STORAGE_REDO_LOG(WARN, "Fail to fill log", K(ret));
      } else if (OB_FAIL(log_item->fill_batch_header(
          padding_buffer_size + entry.get_serialize_size(), 1, header_pos))) {
        STORAGE_REDO_LOG(WARN, "Fail to fill batch header", K(ret), K(padding_buffer_size));
      }
    }
  }

  return ret;
}

void ObStorageLogWriter::process_log_items(ObIBaseLogItem **items,
    const int64_t item_cnt, int64_t &finish_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "Not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == items || item_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid arguments", K(ret), KP(items), K(item_cnt));
  } else if (OB_FAIL(batch_process_log_items(items, item_cnt, finish_cnt))) {
    STORAGE_REDO_LOG(WARN, "Fail to batch process log items", K(ret), KP(items), K(item_cnt));
  }
}

int ObStorageLogWriter::batch_process_log_items(ObIBaseLogItem **items,
    const int64_t item_cnt, int64_t &finish_cnt)
{
  int ret = OB_SUCCESS;

  STORAGE_REDO_LOG(DEBUG, "batch process start", K(item_cnt), K(cursor_));

  finish_cnt = 0;
  int64_t batch_begin_index = 0;
  int64_t batch_end_index = -1;
  while(OB_SUCC(ret) && batch_begin_index < item_cnt) {
    int64_t cur_aggregate_len = 0;
    int64_t occupied_len = write_offset_ + batch_write_buf_.get_log_data_len();
    bool file_end = false;

    for (int64_t i = batch_begin_index; OB_SUCC(ret) && i < item_cnt; i++) {
      ObStorageLogItem *log_item = static_cast<ObStorageLogItem *>(items[i]);
      if (OB_UNLIKELY(nullptr == log_item)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_REDO_LOG(WARN, "Log item is null", K(ret), KP(log_item), K(i));
      } else if (log_item->is_local() && 0 == cur_aggregate_len) {
        // single large item, also first item in this batch
        if (0 != batch_write_buf_.get_log_data_len()) {
          // if batch write buffer has reserved log data
          // skip the reserved data and nop log, write at next 4K offset
          occupied_len = write_offset_ + batch_write_buf_.get_write_len();
          write_offset_ = occupied_len;
          STORAGE_REDO_LOG(INFO, "Modify write_offset due to single large item", K_(write_offset));
        }
        file_end = is_log_file_reach_end(*log_item, occupied_len);
        if (!file_end) {
          occupied_len += log_item->get_data_len();
          batch_end_index = i;
        }
        break;
      } else if (log_item->is_local() && 0 != cur_aggregate_len) {
        // single large item, but not first item in this batch
        // just flush aggregated items and flush this item at next roud
        break;
      } else {
        file_end = is_log_file_reach_end(*log_item, occupied_len);
        if (file_end) {
          // reach file size limit, aggregation finishes
          break;
        } else if (!is_buffer_enough(*log_item, cur_aggregate_len)) {
          // reach buffer size limit, aggregation finishes
          break;
        } else {
          occupied_len += log_item->get_data_len();
          cur_aggregate_len += log_item->get_data_len();
          batch_end_index = i;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (batch_end_index < batch_begin_index) {
        // end index is -1
        if (file_end) {
          if (OB_FAIL(advance_file_id())) {
            STORAGE_REDO_LOG(ERROR, "Fail to switch file", K(ret), K(file_end));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_REDO_LOG(WARN, "No log is aggregated and filedoesn't reach end",
              K(ret), K(file_end), K(batch_end_index), K(batch_begin_index));
        }
      } else if (OB_FAIL(batch_write_logs(items,
          batch_begin_index, batch_end_index, finish_cnt))) {
        STORAGE_REDO_LOG(WARN, "Fail to write batch logs", K(ret), K(file_end),
            K(batch_end_index), K(batch_begin_index));
      } else if (file_end && OB_FAIL(advance_file_id())) {
        STORAGE_REDO_LOG(ERROR, "Fail to switch file", K(ret), K(file_end));
      } else if (OB_FAIL(notify_flush(items, batch_begin_index, batch_end_index))) {
        STORAGE_REDO_LOG(ERROR, "Fail to notify flush", K(ret),
            K(batch_begin_index), K(batch_end_index));
      } else {
        batch_begin_index = batch_end_index + 1;
        batch_end_index = -1;
      }
    }

    if (OB_FAIL(ret)) {
      STORAGE_REDO_LOG(ERROR, "Unexpected error, aggregation should never fail", K(ret));
      ob_abort();
    }
  }

  return ret;
}

int ObStorageLogWriter::batch_write_logs(
    ObIBaseLogItem **items,
    const int64_t begin,
    const int64_t end,
    int64_t &finish_cnt)
{
  int ret = OB_SUCCESS;
  char *write_buf = nullptr;
  int64_t write_len = 0;

  if (OB_UNLIKELY(nullptr == items || begin < 0 || end < 0 || begin > end)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid arguments", K(ret), K(begin), KP(end), KP(items));
  } else if (OB_FAIL(aggregate_logs(items, begin, end, write_buf, write_len))) {
    STORAGE_REDO_LOG(WARN, "Fail to aggregate logs", K(ret), K(begin), KP(end), KP(items));
  } else if (OB_FAIL(write_logs(items, begin, end, write_buf, write_len))) {
    STORAGE_REDO_LOG(WARN, "Fail to write logs", K(ret), K(begin), KP(end), KP(items));
  } else {
    finish_cnt += (end - begin + 1);
  }

  return ret;
}

int ObStorageLogWriter::aggregate_logs(
    ObIBaseLogItem **items,
    const int64_t begin,
    const int64_t end,
    char *&write_buf,
    int64_t &write_len)
{
  int ret = OB_SUCCESS;

  if (begin == end) {
    ObStorageLogItem *log_item = static_cast<ObStorageLogItem *>(items[begin]);
    if (OB_UNLIKELY(nullptr == log_item)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_REDO_LOG(WARN, "Log item is null", K(ret), KP(log_item));
    } else if (log_item->is_local() &&
        OB_FAIL(aggregate_single_large_log_item(log_item, write_buf, write_len))) {
      STORAGE_REDO_LOG(WARN, "Fail to aggregate single large log item", K(ret), KP(log_item));
    } else if (!log_item->is_local() &&
        OB_FAIL(aggregate_multi_log_items(items, begin, end, write_buf, write_len))) {
      STORAGE_REDO_LOG(WARN, "Fail to aggregate multi log items", K(ret), KP(log_item));
    }
  } else if (OB_FAIL(aggregate_multi_log_items(items, begin, end, write_buf, write_len))) {
    STORAGE_REDO_LOG(WARN, "Fail to aggregate multi log items", K(ret), K(begin), K(end));
  }

  return ret;
}

int ObStorageLogWriter::aggregate_single_large_log_item(
    ObStorageLogItem *log_item,
    char *&write_buf,
    int64_t &write_len)
{
  int ret = OB_SUCCESS;

  int64_t begin_offset = write_offset_;
  int64_t occupied_len = write_offset_ + log_item->get_data_len();
  if (OB_FAIL(fill_nop_log(log_item, occupied_len))) {
    STORAGE_REDO_LOG(WARN, "Fail to fill nop log", K(ret), KP(log_item), K(occupied_len));
  } else if (OB_FAIL(update_log_item_cursor(log_item, begin_offset))) {
    STORAGE_REDO_LOG(WARN, "Fail to update log item cursor", K(ret), KP(log_item), K(begin_offset));
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_UNLIKELY((ObLogWritePolicy::LOG_TRUNCATE_WRITE == log_write_policy_)
      && (0 != log_item->get_data_len() % write_align_size_))) {
    // defense code
    ret = OB_ERR_UNEXPECTED;
    STORAGE_REDO_LOG(WARN, "Unexpected error", K(ret), K(log_item->get_data_len()));
  } else {
    write_buf = log_item->get_buf();
    write_len = log_item->get_data_len();
  }
  return ret;
}

int ObStorageLogWriter::aggregate_multi_log_items(
    ObIBaseLogItem **items,
    const int64_t begin,
    const int64_t end,
    char *&write_buf,
    int64_t &write_len)
{
  int ret = OB_SUCCESS;
  int64_t begin_offset = write_offset_ + batch_write_buf_.get_log_data_len();

  for (int64_t i = begin; OB_SUCC(ret) && i <= end; i++) {
    if (OB_UNLIKELY(nullptr == items[i])) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_REDO_LOG(WARN, "Invalid arguments", K(ret), KP(items[i]), K(i));
      break;
    }
    ObStorageLogItem *log_item = static_cast<ObStorageLogItem *>(items[i]);
    if (i == end) {
      int64_t occupied_len = begin_offset + log_item->get_data_len();
      if (OB_FAIL(fill_nop_log(log_item, occupied_len))) {
        STORAGE_REDO_LOG(WARN, "Fail to fill nop log", K(ret), KP(log_item), K(occupied_len));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(update_log_item_cursor(log_item, begin_offset))) {
      STORAGE_REDO_LOG(WARN, "Fail to update log item's cursor",
          K(ret), KP(log_item), K(begin_offset));
    } else if (OB_FAIL(batch_write_buf_.copy_log_item(log_item))) {
      STORAGE_REDO_LOG(WARN, "Fail to copy log item to buffer", K(ret), KP(log_item));
    } else {
      begin_offset += log_item->get_data_len();
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_UNLIKELY((ObLogWritePolicy::LOG_TRUNCATE_WRITE == log_write_policy_)
      && (0 != batch_write_buf_.get_write_len() % write_align_size_))) {
    // defense code
    ret = OB_ERR_UNEXPECTED;
    STORAGE_REDO_LOG(WARN, "unexpected error",
      K(ret), K_(batch_write_buf), K_(log_write_policy));
  } else {
    write_buf = const_cast<char *>(batch_write_buf_.get_buf());
    write_len = batch_write_buf_.get_write_len();
  }
  return ret;
}

int ObStorageLogWriter::write_logs(
    ObIBaseLogItem **items,
    const int64_t begin,
    const int64_t end,
    char *write_buf,
    const int64_t write_len)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::fast_current_time();
  // TODO: delete it
  bool is_single_local_item = false;
  const int64_t TIME_THRESHOLD = 10000;

  if (OB_ISNULL(items) || OB_ISNULL(write_buf) ||
      OB_UNLIKELY(begin < 0 || end < 0 || begin > end || write_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid arguments", K(ret), KP(items),
        K(begin), K(end), KP(write_buf), K(write_len));
  } else if (OB_FAIL(file_handler_.write(write_buf, write_len, write_offset_))) {
    STORAGE_REDO_LOG(WARN, "Fail to write logs to disk",
        K(ret), KP(write_buf), K(write_len), K_(write_offset));
  }

  if (OB_SUCC(ret)) {
    const int64_t duration = ObTimeUtility::fast_current_time() - start_ts;
    if (duration > TIME_THRESHOLD) {
      STORAGE_REDO_LOG(INFO, "Slow write", K(duration), K(write_len));
    }
  }

  if (OB_SUCC(ret)) {
    ObStorageLogItem *log_item = static_cast<ObStorageLogItem *>(items[end]);
    is_single_local_item = (begin == end && log_item->is_local());
    int64_t backward_size = 0;
    if (is_single_local_item) {
      batch_write_buf_.reuse();
      write_offset_ += write_len;
    } else if (OB_FAIL(batch_write_buf_.move_buffer(backward_size))) {
      STORAGE_REDO_LOG(WARN, "Fail to move buffer", K(ret), K(is_single_local_item),
          K(backward_size));
    } else {
      write_offset_ += backward_size;
    }
  }

  return ret;
}

int ObStorageLogWriter::notify_flush(
    ObIBaseLogItem **items,
    const int64_t begin,
    const int64_t end)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == items || begin < 0 || begin > end)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid arguments", K(ret), KP(items), K(begin), K(end));
  } else {
    for (int64_t i = begin; i <= end; i++) {
      ObStorageLogItem *log_item = static_cast<ObStorageLogItem *>(items[i]);
      if (OB_UNLIKELY(flush_seq_ != log_item->get_seq())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_REDO_LOG(ERROR, "The flush_seq_ doesn't match", K(flush_seq_),
            K(log_item->start_cursor_), K(log_item->end_cursor_));
      } else {
        flush_seq_ += log_item->get_log_cnt();
        STORAGE_REDO_LOG(INFO, "Successfully flush", "log_item", *log_item);
      }
      log_item->finish_flush(OB_SUCCESS);
    }
  }
  return ret;
}

int ObStorageLogWriter::advance_file_id()
{
  int ret = OB_SUCCESS;

  ObSpinLockGuard guard(cursor_lock_);
  if (file_handler_.is_opened() && OB_FAIL(file_handler_.close())) {
    STORAGE_REDO_LOG(WARN, "Fail to close file", K(ret), K(cursor_.file_id_));
  } else if (OB_FAIL(file_handler_.open(cursor_.file_id_ + 1))) {
    STORAGE_REDO_LOG(WARN, "Fail to open file", K(ret), K(cursor_.file_id_));
  } else {
    cursor_.file_id_++;
    write_offset_ = 0;
    cursor_.offset_ = write_offset_;
    batch_write_buf_.reuse();
    STORAGE_REDO_LOG(INFO, "Successfully open slog file", K(cursor_.file_id_));
  }

  return ret;
}

bool ObStorageLogWriter::is_log_file_reach_end(
    const ObStorageLogItem &log_item,
    const int64_t used_len)
{
  bool b_ret = false;
  // max nop log occupied size: nop log entry + max nop log(4K)
  ObStorageLogEntry dummy_entry;
  const int64_t max_nop_log_size = dummy_entry.get_serialize_size() +
                                   ObLogConstants::LOG_FILE_ALIGN_SIZE;
  if (log_item.get_data_len() + used_len + max_nop_log_size > file_size_) {
    b_ret = true;
  }
  return b_ret;
}

bool ObStorageLogWriter::is_buffer_enough(const ObStorageLogItem &log_item,
    const int64_t aggregate_len)
{
  bool b_ret = true;
  // max nop log occupied size: nop log entry + max nop log(4K)
  ObStorageLogEntry dummy_entry;
  const int64_t max_nop_log_size = dummy_entry.get_serialize_size() +
                                   ObLogConstants::LOG_FILE_ALIGN_SIZE;
  if (log_item.get_data_len() + aggregate_len + max_nop_log_size >
      batch_write_buf_.get_left_space()) {
    b_ret = false;
  }
  return b_ret;
}

int ObStorageLogWriter::update_log_item_cursor(
    ObStorageLogItem *log_item,
    const int64_t begin_offset)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(log_item)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("Invalid argument", K(ret), KP(log_item));
  } else {
    ObSpinLockGuard guard(cursor_lock_);
    log_item->start_cursor_.file_id_ = cursor_.file_id_;
    log_item->start_cursor_.log_id_ = log_item->get_seq();
    log_item->start_cursor_.offset_ = begin_offset;

    log_item->end_cursor_.file_id_ = cursor_.file_id_;
    log_item->end_cursor_.log_id_ = log_item->get_seq() + log_item->get_log_cnt();
    log_item->end_cursor_.offset_ = begin_offset + log_item->get_log_data_len();

    cursor_.log_id_ = log_item->end_cursor_.log_id_;
    cursor_.offset_ = log_item->end_cursor_.offset_;
    LOG_DEBUG("successfully update log item cursor",
        K(cursor_), K(log_item->start_cursor_), K(log_item->end_cursor_));
  }
  return ret;
}

ObStorageLogWriter::ObSLogWriteRunner::ObSLogWriteRunner()
  : log_writer_(nullptr), tg_id_(-1), is_inited_(false)
{
}

ObStorageLogWriter::ObSLogWriteRunner::~ObSLogWriteRunner()
{
  destroy();
}

int ObStorageLogWriter::ObSLogWriteRunner::init(ObStorageLogWriter *log_writer)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_REDO_LOG(WARN, "ObSLogWriteRunner has been inited.", K(ret));
  } else if (OB_ISNULL(log_writer)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Log_writer is nullptr.", K(ret), KP(log_writer));
  } else {
    log_writer_ = log_writer;
    if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::StorageLogWriter, tg_id_))) {
      STORAGE_REDO_LOG(WARN, "Fail to create thread for log writer.", K(ret), K(tg_id_));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObStorageLogWriter::ObSLogWriteRunner::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "ObSLogWriteRunner hasn't been inited.", K(ret), K(is_inited_));
  } else if (OB_FAIL(TG_SET_RUNNABLE_AND_START(tg_id_, *this))) {
    STORAGE_REDO_LOG(WARN, "Fail to start log writer thread.", K(ret), K(tg_id_));
  }
  return ret;
}

void ObStorageLogWriter::ObSLogWriteRunner::destroy()
{
  if (is_inited_) {
    TG_DESTROY(tg_id_);
    is_inited_ = false;
  }
}

void ObStorageLogWriter::ObSLogWriteRunner::stop()
{
  TG_STOP(tg_id_);
}

void ObStorageLogWriter::ObSLogWriteRunner::wait()
{
  TG_WAIT(tg_id_);
}

void ObStorageLogWriter::ObSLogWriteRunner::run1()
{
  STORAGE_REDO_LOG(INFO, "ObSLogWriteRunner run", K(tg_id_), K(is_inited_));
  lib::set_thread_name(log_writer_->get_thread_name());
  log_writer_->flush_log();
}

}
}
