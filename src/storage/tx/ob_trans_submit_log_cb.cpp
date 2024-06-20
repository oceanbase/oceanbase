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

#include "ob_trans_submit_log_cb.h"
#include "lib/stat/ob_session_stat.h"
#include "share/ob_cluster_version.h"
#include "ob_trans_service.h"
#include "ob_trans_part_ctx.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;
using namespace palf;

namespace transaction
{

void ObTxBaseLogCb::reset()
{
  AppendCb::reset();
  base_ts_.reset();
  log_ts_.reset();
  lsn_.reset();
  submit_ts_ = 0;
}

void ObTxBaseLogCb::reuse()
{
  base_ts_.reset();
  log_ts_.reset();
  lsn_.reset();
  submit_ts_ = 0;
}

int ObTxBaseLogCb::set_log_ts(const SCN &log_ts)
{
  int ret = OB_SUCCESS;

  if (!log_ts.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(log_ts));
  } else {
    log_ts_ = log_ts;
  }

  return ret;
}

int ObTxBaseLogCb::set_lsn(const LSN &lsn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!lsn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(lsn));
  } else {
    lsn_ = lsn;
  }
  return ret;
}

int ObTxLogCb::init(const ObLSID &key,
    const ObTransID &trans_id, ObTransCtx *ctx, const bool is_dynamic)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTxLogCb inited twice");
    ret = OB_INIT_TWICE;
  } else if (!key.is_valid() || !trans_id.is_valid() || OB_ISNULL(ctx)) {
    TRANS_LOG(WARN, "invalid argument", K(key), K(trans_id), KP(ctx));
    ret = OB_INVALID_ARGUMENT;
  } else {
    is_inited_ = true;
    ls_id_ = key;
    trans_id_ = trans_id;
    ctx_ = ctx;
    tx_data_guard_.reset();
    is_callbacked_ = false;
    is_dynamic_ = is_dynamic;
  }

  return ret;
}

void ObTxLogCb::reset_tx_op_array()
{
  if (OB_NOT_NULL(tx_op_array_)) {
    for (int64_t idx = 0; idx < tx_op_array_->count(); idx++) {
      tx_op_array_->at(idx).release();
    }
    tx_op_array_->~ObTxOpArray();
    mtl_free(tx_op_array_);
    tx_op_array_ = nullptr;
  }
}

void ObTxLogCb::reset_undo_node()
{
  if (OB_NOT_NULL(undo_node_)) {
    MTL(share::ObSharedMemAllocMgr*)->tx_data_allocator().free(undo_node_);
    undo_node_ = NULL;
  }
}

void ObTxLogCb::reset()
{
  ObTxBaseLogCb::reset();
  ObDLinkBase<ObTxLogCb>::reset();
  is_inited_ = false;
  ls_id_.reset();
  trans_id_.reset();
  ctx_ = NULL;
  tx_data_guard_.reset();
  callbacks_.reset();
  is_callbacked_ = false;
  is_dynamic_ = false;
  cb_arg_array_.reset();
  mds_range_.reset();

  if (OB_NOT_NULL(extra_cb_) && need_free_extra_cb_) {
    mtl_free(extra_cb_);
  }
  need_free_extra_cb_ = false;

  // is_callbacking_ = false;
  first_part_scn_.invalid_scn();
  reset_tx_op_array();
  reset_undo_node();
}

void ObTxLogCb::reuse()
{
  ObTxBaseLogCb::reuse();
  tx_data_guard_.reset();
  callbacks_.reset();
  is_callbacked_ = false;
  cb_arg_array_.reset();
  mds_range_.reset();

  if (OB_NOT_NULL(extra_cb_) && need_free_extra_cb_) {
    mtl_free(extra_cb_);
  }
  need_free_extra_cb_ = false;

  first_part_scn_.invalid_scn();
  reset_tx_op_array();
  reset_undo_node();
}

ObTxLogType ObTxLogCb::get_last_log_type() const
{
  ObTxLogType log_type = ObTxLogType::UNKNOWN;
  if (cb_arg_array_.count() > 0) {
    log_type = cb_arg_array_.at(cb_arg_array_.count() - 1).get_log_type();
  }
  return log_type;
}

bool ObTxLogCb::is_valid() const
{
  return cb_arg_array_.count() > 0;
}

int ObTxLogCb::on_success()
{
  int ret = OB_SUCCESS;
  const ObTransID tx_id = trans_id_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTxLogCb not inited", K(ret));
  } else if (NULL == ctx_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "ctx is null", K(ret), K(tx_id), KP(ctx_));
  } else {
    ObPartTransCtx *part_ctx = static_cast<ObPartTransCtx *>(ctx_);
    if (OB_FAIL(part_ctx->on_success(this))) {
      TRANS_LOG(WARN, "sync log success callback error", K(ret), K(tx_id));
    }
  }

  return ret;
}

int ObTxLogCb::on_failure()
{
  int ret = OB_SUCCESS;
  const ObTransID tx_id = trans_id_;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTxLogCb not inited", K(*this));
  } else if (NULL == ctx_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "ctx is null", KR(ret), K(*this));
  } else {
    ObPartTransCtx *part_ctx = static_cast<ObPartTransCtx *>(ctx_);
    if (OB_FAIL(part_ctx->on_failure(this))) {
      TRANS_LOG(WARN, "sync log success callback error", KR(ret), K(tx_id));
    }
  }
  TRANS_LOG(INFO, "ObTxLogCb::on_failure end", KR(ret), K(tx_id));
  return ret;
}

int ObTxLogCb::copy(const ObTxLogCb &other)
{
  int ret = OB_SUCCESS;
  log_ts_ = other.log_ts_;
  lsn_ = other.lsn_;
  submit_ts_ = other.submit_ts_;
  is_inited_ = other.is_inited_;
  ls_id_ = other.ls_id_;
  trans_id_ = other.trans_id_;
  ctx_ = other.ctx_;
  if (OB_FAIL(callbacks_.assign(other.callbacks_))) {
    TRANS_LOG(WARN, "assign callbacks failed", K(ret), KPC(this));
  } else if (FALSE_IT(is_callbacked_ = other.is_callbacked_)) {
  // without txdata
  } else if (OB_FAIL(mds_range_.assign(other.mds_range_))) {
    TRANS_LOG(WARN, "assign mds range failed", K(ret), KPC(this));
  } else if (OB_FAIL(cb_arg_array_.assign(other.cb_arg_array_))) {
    TRANS_LOG(WARN, "assign cb_arg_array_ failed", K(ret), KPC(this));
  }
  first_part_scn_ = other.first_part_scn_;

  return ret;
}

} // transaction
} // oceanbase
