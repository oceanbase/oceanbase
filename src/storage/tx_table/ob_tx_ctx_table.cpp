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

#include "storage/tx_table/ob_tx_ctx_table.h"
#include "storage/tx/ob_trans_service.h"
#include "lib/oblog/ob_log_module.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx_table/ob_tx_table_iterator.h"
#include "storage/ls/ob_ls.h"

namespace oceanbase
{
using namespace transaction;
using namespace share;
namespace storage
{

void ObTxCtxTableRecoverHelper::reset()
{
  in_multi_row_state_ = false;
  prev_meta_.reset();
  buf_.reset();
  prev_end_pos_ = -1;
  ctx_info_.reset();
}

void ObTxCtxTableRecoverHelper::destroy()
{
  reset();
}

int ObTxCtxTableRecoverHelper::validate_extend_meta_(const ObTxCtxTableMeta &curr_meta)
{
  int ret = OB_SUCCESS;

  if (!in_multi_row_state_) {
    ret = OB_ERR_UNEXPECTED;
  } else if (!prev_meta_.is_multi_row_next_extent(curr_meta)) {
    ret = OB_ERR_UNEXPECTED;
  }

  return ret;
}

int ObTxCtxTableRecoverHelper::append_curr_value_buf_(const char* in_buf,
                                                      const int64_t in_buf_length)
{
  int ret = OB_SUCCESS;

  if (prev_end_pos_ + in_buf_length < buf_.get_length()) {
    memcpy(buf_.get_ptr() + prev_end_pos_ + 1, in_buf, in_buf_length);
    prev_end_pos_ += in_buf_length;
  } else {
    ret = OB_BUF_NOT_ENOUGH;
  }
  return ret;
}

int ObTxCtxTableRecoverHelper::buf_reserve_(const int64_t buf_length)
{
  return buf_.reserve(buf_length);
}

char* ObTxCtxTableRecoverHelper::get_buf_ptr_()
{
  return buf_.get_ptr();
}

void ObTxCtxTableRecoverHelper::set_in_multi_row_state_()
{
  in_multi_row_state_ = true;
}

void ObTxCtxTableRecoverHelper::clear_in_multi_row_state_()
{
  in_multi_row_state_ = false;
}

bool ObTxCtxTableRecoverHelper::is_in_multi_row_state_() const
{
  return in_multi_row_state_;
}

void ObTxCtxTableRecoverHelper::finish_recover_one_tx_ctx_()
{
  buf_.reset();
  prev_end_pos_ = -1;
  ctx_info_.reset();
}

int ObTxCtxTableRecoverHelper::recover_one_tx_ctx_(transaction::ObLSTxCtxMgr* ls_tx_ctx_mgr,
                                                   ObTxCtxTableInfo& ctx_info)
{
  int ret = OB_SUCCESS;
  transaction::ObPartTransCtx *tx_ctx = NULL;
  bool tx_ctx_existed = true;
  common::ObAddr scheduler;
  // since 4.3 cluster_version in ctx_info
  uint64_t cluster_version = ctx_info.cluster_version_;
  transaction::ObTxCreateArg arg(true,  /* for_replay */
                                 PartCtxSource::RECOVER,
                                 MTL_ID(),
                                 ctx_info.tx_id_,
                                 ctx_info.ls_id_,
                                 ctx_info.cluster_id_,     /* cluster_id */
                                 cluster_version,
                                 0, /*session_id*/
                                 0, /*associated_session_id*/
                                 scheduler,
                                 INT64_MAX,
                                 MTL(ObTransService*));
  if (OB_FAIL(ls_tx_ctx_mgr->create_tx_ctx(arg,
                                           tx_ctx_existed, /*tx_ctx_existed*/
                                           tx_ctx))) {
    STORAGE_LOG(WARN, "failed to create tx ctx", K(ret));
  } else if (OB_FAIL(tx_ctx->recover_tx_ctx_table_info(ctx_info))) {
    STORAGE_LOG(WARN, "recover from trans sstable durable ctx info failed", K(ret), K(*tx_ctx));
  } else {
    STORAGE_LOG(INFO, "restore trans state in memory", K(ctx_info));
  }

  if (NULL != tx_ctx) {
    int tmp_ret = 0;
    if (OB_TMP_FAIL(ls_tx_ctx_mgr->revert_tx_ctx(tx_ctx))) {
      STORAGE_LOG(WARN, "failed to revert trans ctx", K(ret));
    }
    tx_ctx = NULL;
  }
  return ret;
}

int ObTxCtxTableRecoverHelper::recover(const blocksstable::ObDatumRow &row,
                                       ObTxDataTable &tx_data_table,
                                       transaction::ObLSTxCtxMgr *ls_tx_ctx_mgr)
{
  int ret = OB_SUCCESS;

  ObTxCtxTableMeta curr_meta;
  bool buf_completed = false;
  char* deserialize_buf = NULL;
  int64_t deserialize_buf_length = 0;

  if (NULL == ls_tx_ctx_mgr) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ls tx ctx mgr is null", K(ret));
  }

  if (OB_SUCC(ret)) {
    int64_t idx = 0;
    idx = row.storage_datums_[TX_CTX_TABLE_KEY_COLUMN].get_int();

    ObString meta_str = row.storage_datums_[TX_CTX_TABLE_META_COLUMN].get_string();
    ObString value_str = row.storage_datums_[TX_CTX_TABLE_VAL_COLUMN].get_string();

    bool need_to_append_buf = false;
    int64_t pos = 0;
    if (OB_FAIL(curr_meta.deserialize(meta_str.ptr(), meta_str.length(), pos))) {
      STORAGE_LOG(WARN, "failed to deserialize ctx meta", K(ret), K(curr_meta));
    } else {
      STORAGE_LOG(INFO, "deserialize ctx meta succ", K(ret), K(curr_meta));
      if (is_in_multi_row_state_()) {
        if (OB_FAIL(validate_extend_meta_(curr_meta))) {
          STORAGE_LOG(WARN, "validate_extend_meta failed", K(ret), K(*this));
        } else {
          need_to_append_buf = true;
        }
      } else {
        if (curr_meta.is_single_row_tx_ctx()) {
          // do nothing
        } else {
          set_in_multi_row_state_();
          if (OB_FAIL(buf_reserve_(curr_meta.get_tx_ctx_serialize_size()))) {
            STORAGE_LOG(WARN, "Failed to reserve tx local buffer", K(ret));
          } else {
            need_to_append_buf = true;
          }
        }
      }
    }

    if (OB_SUCC(ret) && need_to_append_buf) {
      if (OB_FAIL(append_curr_value_buf_(value_str.ptr(), value_str.length()))) {
        STORAGE_LOG(WARN, "append buf failed", K(ret), K(*this));
      }
    }

    if (OB_SUCC(ret)) {
      if (curr_meta.is_multi_row_last_extent()) {
        buf_completed = true;
        deserialize_buf = get_buf_ptr_();
        deserialize_buf_length = curr_meta.get_tx_ctx_serialize_size();
        clear_in_multi_row_state_();
        STORAGE_LOG(INFO, "curr meta is multi row last extent", K(curr_meta));
      } else if (curr_meta.is_single_row_tx_ctx()) {
        buf_completed = true;
        deserialize_buf = value_str.ptr();
        deserialize_buf_length = value_str.length();
        STORAGE_LOG(INFO, "curr meta is is_single_row_tx_ctx", K(curr_meta));
      } else {
        // do nothing
      }
    }
  }

  if (OB_SUCC(ret) && buf_completed) {
    ctx_info_.reset();
    transaction::ObPartTransCtx *tx_ctx = NULL;
    int64_t pos = 0;
    bool tx_ctx_existed = true;
    ctx_info_.set_compatible_version(curr_meta.get_version());
    if (OB_FAIL(ctx_info_.deserialize(deserialize_buf, deserialize_buf_length, pos, tx_data_table))) {
      STORAGE_LOG(WARN, "failed to deserialize status_info", K(ret), K_(ctx_info));
    } else if (FALSE_IT(ctx_info_.exec_info_.mrege_buffer_ctx_array_to_multi_data_source())) {
    } else if (OB_FAIL(recover_one_tx_ctx_(ls_tx_ctx_mgr, ctx_info_))) {
      // heap memory needed be freed, but can not do this in destruction, cause tx_buffer_node has no value sematics
      ctx_info_.exec_info_.clear_buffer_ctx_in_multi_data_source();
      STORAGE_LOG(WARN, "failed to recover_one_tx_ctx_", K(ret), K(ctx_info_));
    } else {
      // heap memory needed be freed, but can not do this in destruction, cause tx_buffer_node has no value sematics
      ctx_info_.exec_info_.clear_buffer_ctx_in_multi_data_source();
      finish_recover_one_tx_ctx_();
    }
  }

  if (OB_SUCC(ret)) {
    prev_meta_ = curr_meta;
  }

  return ret;
}

ObTxCtxTable::ObTxCtxTable()
  : ls_id_(),
    ls_tx_ctx_mgr_(NULL)
{
  ls_id_.reset();
}

ObTxCtxTable::~ObTxCtxTable()
{
  reset();
}

int ObTxCtxTable::init(const ObLSID& ls_id) {
  return acquire_ref_(ls_id);
}

int ObTxCtxTable::acquire_ref_(const ObLSID& ls_id)
{
  reset();
  int ret = OB_SUCCESS;
  transaction::ObTransService *txs = NULL;
  ls_id_ = ls_id;

  if (NULL == ls_tx_ctx_mgr_) {
    if (OB_ISNULL(txs = MTL(ObTransService*))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "trans_service get fail", K(ret));
    } else if (OB_FAIL(txs->get_tx_ctx_mgr().get_ls_tx_ctx_mgr(ls_id, ls_tx_ctx_mgr_))) {
      TRANS_LOG(ERROR, "get ls tx ctx mgr with ref failed", KP(txs));
    } else if (NULL == ls_tx_ctx_mgr_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "ls tx ctx mgr is null", KP(txs));
    } else {
      TRANS_LOG(INFO, "get ls tx ctx mgr successfully", KP(txs));
    }
  }

  return ret;
}

int ObTxCtxTable::release_ref_()
{
  int ret = OB_SUCCESS;
  transaction::ObTransService *txs = NULL;

  if (NULL != ls_tx_ctx_mgr_) {
    if (OB_ISNULL(txs = MTL(ObTransService*))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "trans_service get fail", K(ret));
    } else if (OB_FAIL(txs->get_tx_ctx_mgr().revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr_))) {
      TRANS_LOG(ERROR, "revert ls tx ctx mgr with ref failed", KP(txs));
    } else {
      TRANS_LOG(INFO, "revert ls tx ctx mgr successfully", K(ls_id_), K(this));
      ls_tx_ctx_mgr_ = NULL;
    }
  }

  return ret;
}

ObLSTxCtxMgr *ObTxCtxTable::get_ls_tx_ctx_mgr()
{
  return ls_tx_ctx_mgr_;
}

ObTxCtxTable& ObTxCtxTable::operator=(const ObTxCtxTable& other)
{
  reset();
  ls_id_ = other.ls_id_;
  acquire_ref_(ls_id_);

  return *this;
}

void ObTxCtxTable::reset()
{
  recover_helper_.reset();
  release_ref_();
  ls_id_.reset();
  ls_tx_ctx_mgr_ = NULL;
}

int ObTxCtxTable::offline()
{
  int ret = OB_SUCCESS;

  if (NULL != ls_tx_ctx_mgr_) {
    ret = ls_tx_ctx_mgr_->offline();
  }

  return ret;
}

int ObTxCtxTable::recover(const blocksstable::ObDatumRow &row, ObTxDataTable &tx_data_table)
{
  return recover_helper_.recover(row, tx_data_table, ls_tx_ctx_mgr_);
}

int ObTxCtxTable::check_with_tx_data(const transaction::ObTransID tx_id, ObITxDataCheckFunctor &fn)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(get_ls_tx_ctx_mgr()->check_with_tx_data(tx_id, fn))) {
    if (ret == OB_TRANS_CTX_NOT_EXIST) {
      if (fn.recheck()) {
        // It may be the case that the tx hasn't written any log and exists. And before
        // the tx exists, the check_with_tx_state catches the tnode and does not find
        // any ctx in tx_ctx_table and tx_data_table. So we need recheck the state to
        // prevent the incorrect error reporting
        // TODO(handora.qc): let upper layer to correctly handle the error?
        ret = OB_SUCCESS;
      }
    } else {
      TRANS_LOG(WARN, "check with tx data failed", KR(ret), K(tx_id));
    }
  } else {
    TRANS_LOG(DEBUG, "check with tx data in tx ctx table successfully", K(tx_id));
  }

  return ret;
}

int ObTxCtxTable::dump_single_tx_data_2_text(const int64_t tx_id_int, FILE *fd)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(get_ls_tx_ctx_mgr())) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "tx ctx table is not inited", KR(ret), K(tx_id_int));
  } else {
    ret = get_ls_tx_ctx_mgr()->dump_single_tx_data_2_text(tx_id_int, fd);
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
