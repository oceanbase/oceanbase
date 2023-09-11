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

#define USING_LOG_PREFIX COMMON

#include "share/ob_scanner.h"

#include "lib/alloc/alloc_assist.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase
{
namespace common
{

///////////////////////////////////////////////////////////////////////////////////////////////////

ObScanner::ObScanner(const char *label /*= ObModIds::OB_NEW_SCANNER*/,
                     ObIAllocator *allocator /*= NULL*/,
                     int64_t mem_size_limit /*= DEFAULT_MAX_SERIALIZE_SIZE*/,
                     uint64_t tenant_id /*= OB_INVALID_TENANT_ID*/,
                     bool use_row_compact/*= true*/)
    : row_store_(label, tenant_id, use_row_compact),
      mem_size_limit_(mem_size_limit),
      tenant_id_(tenant_id),
      label_(label),
      affected_rows_(0),
      last_insert_id_to_client_(0),
      last_insert_id_session_(0),
      last_insert_id_changed_(false),
      found_rows_(0),
      user_var_map_(),
      is_inited_(false),
      row_matched_count_(0),
      row_duplicated_count_(0),
      inner_allocator_(ObModIds::OB_SCANNER,
                       OB_MALLOC_NORMAL_BLOCK_SIZE,
                       tenant_id),
      is_result_accurate_(true),
      implicit_cursors_(inner_allocator_),
      datum_store_(label),
      rcode_(),
      fb_info_()
{
  UNUSED(allocator);
}

ObScanner::ObScanner(ObIAllocator &allocator,
                     const char *label,
                     int64_t mem_size_limit,
                     uint64_t tenant_id,
                     bool use_row_compact)
    : row_store_(allocator, label, tenant_id, use_row_compact),
      mem_size_limit_(mem_size_limit),
      tenant_id_(tenant_id),
      label_(label),
      affected_rows_(0),
      last_insert_id_to_client_(0),
      last_insert_id_session_(0),
      last_insert_id_changed_(false),
      found_rows_(0),
      user_var_map_(),
      is_inited_(false),
      row_matched_count_(0),
      row_duplicated_count_(0),
      inner_allocator_(ObModIds::OB_SCANNER,
                       OB_MALLOC_NORMAL_BLOCK_SIZE,
                       tenant_id),
      is_result_accurate_(true),
      implicit_cursors_(allocator),
      datum_store_(label, &allocator),
      rcode_(),
      fb_info_()
{
}

ObScanner::~ObScanner()
{
  // empty
}

int ObScanner::set_extend_info(const ObString &extend_info)
{
  int ret = OB_SUCCESS;
  if OB_FAIL(ob_write_string(inner_allocator_, extend_info, extend_info_)) {
    COMMON_LOG(WARN, "fail to write extend info", K(ret));
  }
  return ret;
}

void ObScanner::reuse()
{
  row_store_.reuse();
  datum_store_.reset();
  // don't reset mem_size_limit_
  affected_rows_ = 0;
  last_insert_id_to_client_ = 0;
  last_insert_id_session_ = 0;
  last_insert_id_changed_ = false;
  found_rows_ = 0;
  rcode_.reset();
  user_var_map_.reuse();
  row_matched_count_ = 0;
  row_duplicated_count_ = 0;
  extend_info_.reset();
  inner_allocator_.reuse();
  table_row_counts_.reset();
  is_result_accurate_ = true;
  trans_result_.reset();
  fb_info_.reset();
}

int ObScanner::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("user var map has been inited already", K(ret));
  } else if (OB_FAIL(datum_store_.init(UINT64_MAX, tenant_id_,
                                       ObCtxIds::DEFAULT_CTX_ID, label_, false/*enable_dump*/))) {
    LOG_WARN("fail to init datum store", K(ret));
  } else {
    //FIXME qianfu is too big, optimized away
//    ret = user_var_map_.init(1024 * 1024 * 2, 256, NULL);
//    if (OB_FAIL(ret)) {
//      LOG_WARN("init user var map failed.", K(ret));
//    } else {
//      is_inited_ = true;
//    }
    is_inited_ = true;
  }
  return ret;
}

void ObScanner::reset()
{
  row_store_.reset();
  datum_store_.reset();
  // don't reset mem_size_limit_
  affected_rows_ = 0;
  last_insert_id_to_client_ = 0;
  last_insert_id_session_ = 0;
  last_insert_id_changed_ = false;
  found_rows_ = 0;
  rcode_.reset();
  user_var_map_.reset();
  row_matched_count_ = 0;
  row_duplicated_count_ = 0;
  extend_info_.reset();
  inner_allocator_.reset();
  table_row_counts_.reset();
  is_result_accurate_ = true;
  trans_result_.reset();
  fb_info_.reset();
}

int ObScanner::add_row(const ObNewRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(row_store_.add_row(row))) {
    LOG_WARN("fail to add_row to row store.", K(ret));
  } else if (row_store_.get_data_size() > mem_size_limit_) {
    LOG_WARN("row store data size", "rowstore_data_size", row_store_.get_data_size(), K_(mem_size_limit), K(ret));
    if (OB_FAIL(row_store_.rollback_last_row())) {
      LOG_WARN("fail to rollback last row", K(ret));
    } else {
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

int ObScanner::try_add_row(const common::ObIArray<sql::ObExpr *> &exprs,
                           sql::ObEvalCtx *ctx,
                           bool &row_added)
{
  int ret = OB_SUCCESS;
  row_added = false;
  if (OB_FAIL(datum_store_.try_add_row(exprs, ctx, mem_size_limit_, row_added))) {
    LOG_WARN("fail to add_row to row store.", K(ret));
  }

  return ret;
}

int ObScanner::assign(const ObScanner &other)
{
  int ret = OB_SUCCESS;
  if (other.get_datum_store().get_row_cnt() > 0) {
    if (OB_FAIL(datum_store_.assign(other.datum_store_))) {
      LOG_WARN("fail to assign datum store", K(ret));
    }
  }
  if (other.get_row_count() > 0) {
    if (OB_FAIL(row_store_.assign(other.row_store_))) {
      LOG_WARN("assign rowstore failed", K(ret));
    }
  }
  tenant_id_ = other.tenant_id_;
  label_ = other.label_;
  mem_size_limit_ = other.mem_size_limit_;
  last_insert_id_to_client_ = other.last_insert_id_to_client_;
  last_insert_id_session_ = other.last_insert_id_session_;
  last_insert_id_changed_ = other.last_insert_id_changed_;
  affected_rows_  = other.affected_rows_;
  found_rows_ = other.found_rows_;
  row_matched_count_ = other.row_matched_count_;
  row_duplicated_count_ = other.row_duplicated_count_;

  if (OB_SUCC(ret) && other.user_var_map_.size() > 0) {
    if (OB_FAIL(user_var_map_.init(1024 * 1024 * 2, 256, NULL))) {
      LOG_WARN("init user_var_map failed", K(ret));
    } else if (OB_FAIL(user_var_map_.assign(other.user_var_map_))) {
      LOG_WARN("assign user var failed", K(ret));
    }
  }
  is_inited_ = other.is_inited_;
  is_result_accurate_ = other.is_result_accurate_;
  OZ(trans_result_.assign(other.trans_result_), other);
  OZ(implicit_cursors_.assign(other.implicit_cursors_));
  STRNCPY(rcode_.msg_, other.rcode_.msg_, common::MAX_SQL_ERR_MSG_LENGTH - 1);
  rcode_.rcode_ = other.rcode_.rcode_;
  OZ(fb_info_.assign(other.fb_info_));
  return ret;
}

int ObScanner::set_session_var_map(const sql::ObSQLSessionInfo *p_session_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(p_session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session pointer is null", K(ret));
  } else {
    const sql::ObSessionValMap &current_map = p_session_info->get_user_var_val_map();
    if (current_map.size() > 0) {
      //Init user var map on demand when setting to avoid wasting CPU and memory when there is no user var synchronization
      if (!user_var_map_.get_val_map().created()) {
        OZ (user_var_map_.init(1024 * 1024 * 2, 256, NULL));
      }
      for (sql::ObSessionValMap::VarNameValMap::const_iterator iter = current_map.get_val_map().begin();
        OB_SUCC(ret) && iter != current_map.get_val_map().end(); ++iter) {
        if (iter->first.prefix_match("pkg.") // For package variables, only changes will be synchronized
            && !p_session_info->is_already_tracked(
                  iter->first, p_session_info->get_changed_user_var())) {
          // do nothing ...
        } else {
          OZ (user_var_map_.set_refactored(iter->first, iter->second));
        }
      }
    }
  }
  return ret;
}

void ObScanner::dump() const
{
  LOG_DEBUG("[SCANNER]", "meta", S(*this));
  row_store_.dump();
}

int ObScanner::set_row_matched_count(int64_t row_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(row_count));
  } else {
    row_matched_count_ = row_count;
  }
  return ret;
}

int ObScanner::set_row_duplicated_count(int64_t row_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(row_count));
  } else {
    row_duplicated_count_ = row_count;
  }
  return ret;
}

void ObScanner::log_user_error_and_warn() const
{
  if (OB_UNLIKELY(OB_SUCCESS != rcode_.rcode_)) {
    FORWARD_USER_ERROR(rcode_.rcode_, rcode_.msg_);
  }
  for (int i = 0; i < rcode_.warnings_.count(); ++i) {
    const common::ObWarningBuffer::WarningItem &warning_item = rcode_.warnings_.at(i);
    if (ObLogger::USER_WARN == warning_item.log_level_) {
      FORWARD_USER_WARN(warning_item.code_, warning_item.msg_);
    } else if (ObLogger::USER_NOTE == warning_item.log_level_) {
      FORWARD_USER_NOTE(warning_item.code_, warning_item.msg_);
    }
  }
}

int ObScanner::store_warning_msg(const ObWarningBuffer &wb)
{
  int ret = OB_SUCCESS;
  bool not_null = true;
  for (uint32_t idx = 0; OB_SUCC(ret) && not_null && idx < wb.get_readable_warning_count(); idx++) {
    const common::ObWarningBuffer::WarningItem *item = wb.get_warning_item(idx);
    if (item != NULL) {
      if (OB_FAIL(rcode_.warnings_.push_back(*item))) {
        RPC_OBRPC_LOG(WARN, "Failed to add warning", K(ret));
      }
    } else {
      not_null = false;
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObScanner)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              row_store_,
              mem_size_limit_,
              affected_rows_,
              last_insert_id_to_client_,
              last_insert_id_session_,
              last_insert_id_changed_,
              found_rows_,
              rcode_.rcode_,
              rcode_.msg_,
              user_var_map_,
              row_matched_count_,
              row_duplicated_count_,
              extend_info_,
              is_result_accurate_,
              trans_result_,
              table_row_counts_,
              implicit_cursors_,
              rcode_.warnings_,
              tenant_id_,
              datum_store_,
              fb_info_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObScanner)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              row_store_,
              mem_size_limit_,
              affected_rows_,
              last_insert_id_to_client_,
              last_insert_id_session_,
              last_insert_id_changed_,
              found_rows_,
              rcode_.rcode_,
              rcode_.msg_,
              user_var_map_,
              row_matched_count_,
              row_duplicated_count_,
              extend_info_,
              is_result_accurate_,
              trans_result_,
              table_row_counts_,
              implicit_cursors_,
              rcode_.warnings_,
              tenant_id_,
              datum_store_,
              fb_info_);
  return len;
}

OB_DEF_DESERIALIZE(ObScanner)
{
  int ret = OB_SUCCESS;
  ObString extend_info;
  LST_DO_CODE(OB_UNIS_DECODE,
              row_store_,
              mem_size_limit_,
              affected_rows_,
              last_insert_id_to_client_,
              last_insert_id_session_,
              last_insert_id_changed_,
              found_rows_,
              rcode_.rcode_,
              rcode_.msg_,
              user_var_map_,
              row_matched_count_,
              row_duplicated_count_,
              extend_info,
              is_result_accurate_,
              trans_result_,
              table_row_counts_,
              implicit_cursors_,
              rcode_.warnings_,
              tenant_id_)
  if (OB_SUCC(ret)) {
    if (!datum_store_.is_inited()) {
      // When reverse serialization from ob_rpc_proxy, the init interface of obscanner is not called, datum_store_ relies on init when deserializing,
      // So here to judge, if there is no init, then init, the existing logic is not changed for the time being
      if (OB_FAIL(datum_store_.init(UINT64_MAX, tenant_id_,
                                    ObCtxIds::DEFAULT_CTX_ID, label_, false/*enable_dump*/))) {
        LOG_WARN("fail to init datum store", K(ret));
      }
    }
    OB_UNIS_DECODE(datum_store_);
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ob_write_string(inner_allocator_, extend_info, extend_info_))) {
      LOG_WARN("fail to write string", K(ret));
    }
  }
  OB_UNIS_DECODE(fb_info_);
  return ret;
}

} // namespace common
} // namespace oceanbase
