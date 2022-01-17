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

#define USING_LOG_PREFIX OBLOG

#include "ob_log_row_data_index.h"
#include "lib/atomic/ob_atomic.h"     // ATOMIC_*
#include "ob_log_instance.h"
#include "ob_log_binlog_record_pool.h"
#include "ob_log_meta_manager.h"

namespace oceanbase
{
namespace liboblog
{
ObLogRowDataIndex::ObLogRowDataIndex() :
    br_(NULL),
    host_(NULL),
    tenant_id_(OB_INVALID_TENANT_ID),
    participant_key_str_(NULL),
    log_id_(OB_INVALID_ID),
    log_offset_(0),
    row_no_(OB_INVALID_ID),
    is_rollback_(false),
    row_sql_no_(0),
    br_commit_seq_(0),
    trans_ctx_host_(NULL),
    next_(NULL)
{
}

ObLogRowDataIndex::~ObLogRowDataIndex()
{
  reset();
}

void ObLogRowDataIndex::reset()
{
  br_ = NULL;
  host_ = NULL;
  tenant_id_= OB_INVALID_TENANT_ID;
  participant_key_str_ = NULL;
  log_id_ = OB_INVALID_ID;
  log_offset_ = 0;
  row_no_ = OB_INVALID_ID;
  is_rollback_ = false;
  row_sql_no_ = 0;
  br_commit_seq_ = 0;
  trans_ctx_host_ = NULL;
  next_ = NULL;
}

int ObLogRowDataIndex::init(const uint64_t tenant_id,
    const char *participant_key,
    const uint64_t log_id,
    const int32_t log_offset,
    const uint64_t row_no,
    const bool is_rollback,
    const int32_t row_sql_no)
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(br_)) {
    LOG_ERROR("ILogRecord has been created");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(participant_key)
        || OB_UNLIKELY(OB_INVALID_ID == log_id)
        || OB_UNLIKELY(log_offset < 0)
        || OB_UNLIKELY(OB_INVALID_ID == row_no)) {
    LOG_ERROR("invalid argument", K(participant_key), K(log_id), K(log_offset), K(row_no));
    ret = OB_INVALID_ARGUMENT;
  } else {
    tenant_id_ = tenant_id;
    participant_key_str_ = participant_key;
    log_id_ = log_id;
    log_offset_ = log_offset;
    row_no_ = row_no;
    is_rollback_ = is_rollback;
    row_sql_no_ = row_sql_no;
    set_next(NULL);
  }

  return ret;
}

bool ObLogRowDataIndex::is_valid() const
{
  bool bool_ret = false;

  bool_ret = (NULL != participant_key_str_)
    && (OB_INVALID_ID != log_id_)
    && (log_offset_ >= 0)
    && (OB_INVALID_ID != row_no_);

  return bool_ret;
}

int ObLogRowDataIndex::get_storage_key(std::string &key)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(participant_key_str_)) {
    LOG_ERROR("invalid argument");
    ret = OB_INVALID_ARGUMENT;
  } else {
    key.append(participant_key_str_);
    key.append("_");
    key.append(std::to_string(log_id_));
    key.append("_");
    key.append(std::to_string(log_offset_));
    key.append("_");
    key.append(std::to_string(row_no_));
  }

  return ret;
}


bool ObLogRowDataIndex::before(const ObLogRowDataIndex &row_index, const bool is_single_row)
{
  bool bool_ret = false;

  if (! is_single_row) {
    bool_ret = log_before_(row_index);
  } else {
    bool_ret = log_before_(row_index) || (log_equal_(row_index) && (row_no_ < row_index.row_no_));
  }

  return bool_ret;
}

bool ObLogRowDataIndex::equal(const ObLogRowDataIndex &row_index, const bool is_single_row)
{
  bool bool_ret = false;

  if (! is_single_row) {
    bool_ret = log_equal_(row_index);
  } else {
    bool_ret = log_equal_(row_index) && (row_no_ == row_index.row_no_);
  }

  return bool_ret;
}

int ObLogRowDataIndex::free_br_data()
{
  int ret = OB_SUCCESS;
  IObLogBRPool *br_pool = TCTX.br_pool_;
  IObLogMetaManager *meta_manager = TCTX.meta_manager_;
  ILogRecord *br_data = NULL;

  if (OB_ISNULL(br_)) {
    LOG_ERROR("invalid argument", K(*this));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(br_pool) || OB_ISNULL(meta_manager)) {
    LOG_ERROR("br_pool or meta_manager is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(br_data = br_->get_data())) {
    LOG_ERROR("binlog record data is invalid", K(br_));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ITableMeta *tblMeta = NULL;
    // recycle Table Meta
    if (0 != br_data->getTableMeta(tblMeta)) {
      LOG_ERROR("getTableMeta fail");
      ret = OB_ERR_UNEXPECTED;
    } else if (NULL != tblMeta) {
      meta_manager->revert_table_meta(tblMeta);
      br_data->setTableMeta(NULL);
    }

    // recycle DB Meta
    if (NULL != br_data->getDBMeta()) {
      meta_manager->revert_db_meta(br_data->getDBMeta());
      br_data->setDBMeta(NULL);
    }
  }

  if (OB_SUCC(ret)) {
    br_pool->free(br_);
    br_ = NULL;
  }

  return ret;
}

int ObLogRowDataIndex::construct_serilized_br_data(ObLogBR *&br)
{
  int ret = OB_SUCCESS;
  IObLogBRPool *br_pool = TCTX.br_pool_;

  if (OB_UNLIKELY(NULL != br_)) {
    LOG_ERROR("invalid argument", K(*this));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(br_pool)) {
    LOG_ERROR("br_pool is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(br_pool->alloc(true/*is_serilized*/, br_, this))) {
    LOG_ERROR("br_pool alloc fail", KPC(this), K(br_));
  } else {
    br = br_;
  }

  return ret;
}

int64_t ObLogRowDataIndex::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (NULL != buf && buf_len > 0) {
    (void)common::databuff_printf(buf, buf_len, pos, "key:%s_", participant_key_str_);
    (void)common::databuff_printf(buf, buf_len, pos, "%lu_", log_id_);
    (void)common::databuff_printf(buf, buf_len, pos, "%d_", log_offset_);
    (void)common::databuff_printf(buf, buf_len, pos, "%lu,", row_no_);
    (void)common::databuff_printf(buf, buf_len, pos, "is_rollback=%d,", is_rollback_);
    (void)common::databuff_printf(buf, buf_len, pos, "row_sql_no=%d,", row_sql_no_);
    (void)common::databuff_printf(buf, buf_len, pos, "br_seq=%ld", br_commit_seq_);
  }

  return pos;
}

}
}
