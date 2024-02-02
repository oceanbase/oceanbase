/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */


#define USING_LOG_PREFIX LOGMNR

#include "ob_log_miner_record_converter.h"
#include "ob_log_miner_batch_record.h"

namespace oceanbase
{
namespace oblogminer
{

void ObLogMinerBatchRecord::reset()
{
  range_.reset();
  begin_record_.reset();
  buf_.reset();
  trans_data_record_count_ = 0;
  total_data_record_count_ = 0;
  written_record_count_ = 0;
  first_access_ts_ = OB_INVALID_TIMESTAMP;
  last_trans_end_ts_ = OB_INVALID_TIMESTAMP;

  seq_no_ = -1;
  file_id_ = -1;
  file_offset_ = -1;
  is_file_end_ = false;

  freezed_ = false;
}

int ObLogMinerBatchRecord::init_last_trans_end_ts(const int64_t commit_ts)
{
  int ret = OB_SUCCESS;

  if (OB_INVALID_TIMESTAMP == last_trans_end_ts_) {
    last_trans_end_ts_ = commit_ts;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("last trans end ts rollbacked, unexpected", K(last_trans_end_ts_), K(commit_ts));
  }

  return ret;
}

int ObLogMinerBatchRecord::append_record(const ObLogMinerRecord &record)
{
  int ret = OB_SUCCESS;
  if (freezed_) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("batch record has been freezed, need retry", K(freezed_), "buf_len", buf_.length(),
        "buf_cap", buf_.capacity());
  } else {
    const RecordType record_type = record.get_record_type();
    if (OB_INVALID_TIMESTAMP == first_access_ts_) {
      first_access_ts_ = ObTimeUtility::current_time();
    }
    switch (record_type) {
      case EBEGIN: {
        ret = handle_begin_record_(record);
        break;
      }

      case ECOMMIT: {
        ret = handle_commit_record_(record);
        break;
      }

      case EDDL: {
        ret = handle_ddl_record_(record);
        break;
      }

      case EINSERT:
      case EUPDATE:
      case EDELETE: {
        ret = handle_dml_record_(record);
        break;
      }

      case HEARTBEAT: {
        ret = handle_heartbeat_record_(record);
        break;
      }

      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("get unexpected record type", K(record));
      }
    }

    if (OB_FAIL(ret)) {
      LOG_ERROR("append record to buffer failed", K(record));
    } else {
      const int64_t record_commit_ts = record.get_commit_scn().convert_to_ts();
      update_progress_range_(record_commit_ts);
      total_data_record_count_++;
      if (is_trans_end_record_type(record_type)) {
        last_trans_end_ts_ = record_commit_ts;
      }
      if (buf_.length() >= BATCH_RECORD_THRESHOLD) {
        freezed_ = true;
      }
    }
  }


  return ret;
}

int ObLogMinerBatchRecord::handle_begin_record_(const ObLogMinerRecord &record)
{
  int ret = OB_SUCCESS;
  if (begin_record_.is_inited()) {
    ret = OB_ERR_DEFENSIVE_CHECK;
    LOG_ERROR("get duplicate begin record, unexpected", K(begin_record_), K(record), KPC(this));
  } else {
    begin_record_.copy_base_info(record);
  }
  return ret;
}

int ObLogMinerBatchRecord::handle_commit_record_(const ObLogMinerRecord &record)
{
  int ret = OB_SUCCESS;
  if (begin_record_.is_inited() && 0 == trans_data_record_count_) {
    // exit, no need to write begin & commit record
    LOG_TRACE("all data in trans is filtered", K(record), K(begin_record_), KPC(this));
    begin_record_.reset();
  } else if (OB_FAIL(write_record_(record))) {
    LOG_ERROR("failed to write commit record into buffer", K(record), KPC(this));
  } else {
    trans_data_record_count_ = 0;

    if (begin_record_.is_inited()) {
      begin_record_.reset();
    }
  }

  return ret;
}

int ObLogMinerBatchRecord::handle_dml_record_(const ObLogMinerRecord &record)
{
  int ret = OB_SUCCESS;
  if (begin_record_.is_inited()) {
    if (OB_FAIL(write_record_(begin_record_))) {
      LOG_ERROR("write begin record into buffer failed", K(begin_record_), KPC(this));
    } else {
      begin_record_.reset();
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(write_record_(record))) {
    LOG_ERROR("failed to write dml record", K(record), KPC(this));
  } else {
    trans_data_record_count_++;
  }
  return ret;
}

int ObLogMinerBatchRecord::handle_ddl_record_(const ObLogMinerRecord &record)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(write_record_(record))) {
    LOG_ERROR("write ddl record failed", K(record), KPC(this));
  }
  return ret;
}

int ObLogMinerBatchRecord::handle_heartbeat_record_(const ObLogMinerRecord &record)
{
  int ret = OB_SUCCESS;

  return ret;
}

void ObLogMinerBatchRecord::update_progress_range_(const int64_t progress)
{
  if (OB_INVALID_TIMESTAMP == range_.min_commit_ts_) {
    range_.min_commit_ts_ = progress;
  }

  if (OB_INVALID_TIMESTAMP == range_.max_commit_ts_ || progress >= range_.max_commit_ts_){
    range_.max_commit_ts_ = progress;
  }
}

int ObLogMinerBatchRecord::write_record_(const ObLogMinerRecord &record)
{
  int ret = OB_SUCCESS;
  bool is_written = false;
  if (OB_ISNULL(converter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get an invalid converter when write_record", K(record));
  } else if (OB_FAIL(converter_->write_record(record, buf_, is_written))) {
    LOG_ERROR("converter failed to convert record", K(record), "buf_len", buf_.length(),
        "buf_cap", buf_.capacity());
  } else {
    if (is_written) {
      written_record_count_++;
    }
  }
  return ret;
}

}
}