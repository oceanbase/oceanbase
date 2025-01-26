/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "ob_table_trans_utils.h"
#include "storage/tx/ob_trans_service.h"
#include "src/share/table/ob_table_util.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;
using namespace oceanbase::sql;
using namespace oceanbase::sql::stmt;
using namespace oceanbase::transaction;
using namespace oceanbase::rpc;

int ObTableTransParam::init(const ObTableConsistencyLevel consistency_level,
                            const ObLSID &ls_id,
                            int64_t timeout_ts,
                            bool need_global_snapshot)
{
  int ret = OB_SUCCESS;

  if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls id", K(ret), K(ls_id));
  } else {
    consistency_level_ = consistency_level;
    ls_id_ = ls_id;
    timeout_ts_ = timeout_ts;
    need_global_snapshot_ = need_global_snapshot;
  }

  return ret;
}

int ObTableTransParam::init(bool is_readonly,
                            const ObTableConsistencyLevel consistency_level,
                            const ObLSID &ls_id,
                            int64_t timeout_ts,
                            bool need_global_snapshot)
{
  int ret = OB_SUCCESS;

  if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls id", K(ret));
  } else {
    is_readonly_ = is_readonly;
    consistency_level_ = consistency_level;
    ls_id_ = ls_id;
    timeout_ts_ = timeout_ts;
    need_global_snapshot_ = need_global_snapshot;
  }

  return ret;
}

int ObTableTransUtils::setup_tx_snapshot(ObTableTransParam &trans_param)
{
  int ret = OB_SUCCESS;
  bool strong_read = ObTableConsistencyLevel::STRONG == trans_param.consistency_level_;
  ObTransService *txs = MTL(ObTransService*);

  if (strong_read) {
    if (trans_param.ls_id_.is_valid() && !trans_param.need_global_snapshot_) {
      if (OB_FAIL(txs->get_ls_read_snapshot(*trans_param.trans_desc_,
                                            ObTxIsolationLevel::RC,
                                            trans_param.ls_id_,
                                            trans_param.timeout_ts_,
                                            trans_param.tx_snapshot_))) {
        LOG_WARN("fail to get LS read snapshot", K(ret));
      }
    } else if (OB_FAIL(txs->get_read_snapshot(*trans_param.trans_desc_,
                                              ObTxIsolationLevel::RC,
                                              trans_param.timeout_ts_,
                                              trans_param.tx_snapshot_))) {
      LOG_WARN("fail to get global read snapshot", K(ret));
    }
  } else {
    SCN weak_read_snapshot;
    if (OB_FAIL(txs->get_weak_read_snapshot_version(-1, // system variable : max read stale time for user
                                                    false,
                                                    weak_read_snapshot))) {
      LOG_WARN("fail to get weak read snapshot", K(ret));
    } else {
      trans_param.tx_snapshot_.init_weak_read(weak_read_snapshot);
    }
  }

  return ret;
}

int ObTableTransUtils::init_read_trans(ObTableTransParam &trans_param)
{
  int ret = OB_SUCCESS;
  bool strong_read = ObTableConsistencyLevel::STRONG == trans_param.consistency_level_;
  ObTransService *txs = MTL(ObTransService*);

  if (OB_FAIL(txs->acquire_tx(trans_param.trans_desc_, OB_KV_DEFAULT_SESSION_ID))) {
    LOG_WARN("failed to acquire tx desc", K(ret));
  } else if (OB_FAIL(setup_tx_snapshot(trans_param))) {
    LOG_WARN("setup txn snapshot fail", K(ret), K(trans_param), K(strong_read));
    txs->release_tx(*trans_param.trans_desc_);
    trans_param.trans_desc_ = NULL;
  }

  return ret;
}

void ObTableTransUtils::release_read_trans(ObTxDesc *&trans_desc)
{
  if (OB_NOT_NULL(trans_desc)) {
    ObTransService *txs = MTL(ObTransService*);
    txs->release_tx(*trans_desc);
    trans_desc = NULL;
  }
}

int ObTableTransUtils::start_trans(ObTableTransParam &trans_param)
{
  int ret = OB_SUCCESS;
  NG_TRACE(T_start_trans_begin);
  bool strong_read = ObTableConsistencyLevel::STRONG == trans_param.consistency_level_;

  if ((!trans_param.is_readonly_) && (ObTableConsistencyLevel::EVENTUAL == trans_param.consistency_level_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "consistency level");
    LOG_WARN("unsupported consistency level", K(ret));
  }
  ObTransService *txs = MTL(ObTransService*);

  // 1. start transaction
  if (OB_SUCC(ret)) {
    ObTxParam tx_param;
    ObTxAccessMode access_mode = (trans_param.is_readonly_ ? ObTxAccessMode::RD_ONLY : ObTxAccessMode::RW);
    tx_param.access_mode_ = access_mode;
    tx_param.isolation_ = ObTxIsolationLevel::RC;
    tx_param.cluster_id_ = ObServerConfig::get_instance().cluster_id;
    tx_param.timeout_us_ = std::max(0l, trans_param.timeout_ts_ - ObClockGenerator::getClock());
    if (tx_param.timeout_us_ <= 0l) {
      ret = OB_TIMEOUT;
      LOG_WARN("timeout_us is less than 0, it has been timeout", K(ret), K(tx_param.timeout_us_), K(trans_param.timeout_ts_));
    } else if (OB_ISNULL(trans_param.trans_state_ptr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("trans_state_ptr is null", K(ret));
    } else if (true == trans_param.trans_state_ptr_->is_start_trans_executed()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("start_trans is executed", K(ret));
    } else {
      if (OB_FAIL(txs->acquire_tx(trans_param.trans_desc_, OB_KV_DEFAULT_SESSION_ID))) {
        LOG_WARN("failed to acquire tx desc", K(ret));
      } else if (OB_FAIL(txs->start_tx(*trans_param.trans_desc_, tx_param))) {
        LOG_WARN("failed to start trans", K(ret), KPC(trans_param.trans_desc_));
        txs->release_tx(*trans_param.trans_desc_);
        trans_param.trans_desc_ = NULL;
      }
      trans_param.trans_state_ptr_->set_start_trans_executed(OB_SUCC(ret));
    }
  }
  NG_TRACE(T_start_trans_end);

  // 2. acquire snapshot
  if (OB_SUCC(ret)) {
    if (OB_FAIL(setup_tx_snapshot(trans_param))) {
      LOG_WARN("setup txn snapshot fail", K(ret));
    }
  }

  return ret;
}

int ObTableTransUtils::end_trans(ObTableTransParam &trans_param)
{
  int ret = OB_SUCCESS;
  ObTxExecResult trans_result;
  TransState *trans_state_ptr = trans_param.trans_state_ptr_;

  if (OB_NOT_NULL(trans_param.req_)) {
    trans_param.req_->set_trace_point(rpc::ObRequest::OB_EASY_REQUEST_TABLE_API_END_TRANS);
  }

  if (OB_ISNULL(trans_state_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trans_state_ptr is null", K(ret));
  } else if (trans_state_ptr->is_start_trans_executed() && trans_state_ptr->is_start_trans_success()) {
    if (OB_FAIL(MTL(ObTransService*)->collect_tx_exec_result(*trans_param.trans_desc_, trans_result))) {
      LOG_WARN("fail to get trans_result", KR(ret), KPC(trans_param.trans_desc_));
    }
  }

  NG_TRACE(T_end_trans_begin);
  if (OB_SUCC(ret)) {
    if (trans_state_ptr->is_start_trans_executed() && trans_state_ptr->is_start_trans_success()) {
      if (trans_param.trans_desc_->is_rdonly() || trans_param.use_sync_) {
        ret = sync_end_trans(trans_param);
      } else {
        if (trans_param.is_rollback_) {
          ret = sync_end_trans(trans_param);
        } else {
          ret = async_commit_trans(trans_param);
        }
      }
      trans_state_ptr->clear_start_trans_executed();
    } else if (OB_NOT_NULL(trans_param.lock_handle_)) {
      HTABLE_LOCK_MGR->release_handle(*trans_param.lock_handle_); // also release lock when start trans failed
    }
    trans_state_ptr->reset();
  }
  NG_TRACE(T_end_trans_end);

  return ret;
}

int ObTableTransUtils::sync_end_trans(ObTableTransParam &trans_param)
{
  int ret = OB_SUCCESS;
  ObTransService *txs = MTL(ObTransService*);
  const int64_t stmt_timeout_ts = trans_param.timeout_ts_;

  if (trans_param.is_rollback_) {
    if (OB_FAIL(txs->rollback_tx(*trans_param.trans_desc_))) {
      LOG_WARN("fail rollback trans when session terminate", K(ret), K(trans_param));
    }
  } else {
    ACTIVE_SESSION_FLAG_SETTER_GUARD(in_committing);
    if (OB_FAIL(txs->commit_tx(*trans_param.trans_desc_,
                               stmt_timeout_ts,
                               &ObTableUtils::get_kv_normal_trace_info()))) {
      LOG_WARN("fail commit trans when session terminate", K(ret), KPC_(trans_param.trans_desc), K(stmt_timeout_ts));
    }
  }

  int tmp_ret = ret;
  if (OB_FAIL(txs->release_tx(*trans_param.trans_desc_))) {
    // overwrite ret
    LOG_ERROR("release tx failed", K(ret), K(trans_param));
  }
  if (trans_param.lock_handle_ != nullptr) {
    HTABLE_LOCK_MGR->release_handle(*trans_param.lock_handle_);
  }
  ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
  trans_param.trans_desc_ = NULL;
  LOG_DEBUG("ObTableApiProcessorBase::sync_end_trans", K(ret), K(trans_param.is_rollback_), K(stmt_timeout_ts));

  return ret;
}

int ObTableTransUtils::async_commit_trans(ObTableTransParam &trans_param)
{
  int ret = OB_SUCCESS;
  ObTransService *txs = MTL(ObTransService*);
  const bool is_rollback = false;
  if (NULL == trans_param.create_cb_functor_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("create callback functor is null", K(ret));
  } else {
    ObTableAPITransCb *callback = trans_param.create_cb_functor_->new_callback();
    if (OB_ISNULL(callback)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new callback", K(ret));
    } else {
      if (OB_NOT_NULL(trans_param.req_)) {
        trans_param.req_->set_trace_point(rpc::ObRequest::OB_EASY_REQUEST_TABLE_API_ACOM_TRANS);
      }
      callback->set_is_need_rollback(is_rollback);
      callback->set_end_trans_type(sql::ObExclusiveEndTransCallback::END_TRANS_TYPE_IMPLICIT);
      callback->set_lock_handle(trans_param.lock_handle_);
      callback->handout();
      callback->set_tx_desc(trans_param.trans_desc_);
      const int64_t stmt_timeout_ts = trans_param.timeout_ts_;
      // clear thread local variables used to wait in queue
      request_finish_callback();
      // callback won't been called if any error occurred
      if (OB_FAIL(txs->submit_commit_tx(*trans_param.trans_desc_,
                                        stmt_timeout_ts,
                                        *callback,
                                        &ObTableUtils::get_kv_normal_trace_info()))) {
        LOG_WARN("fail end trans when session terminate", K(ret), K(trans_param));
        callback->callback(ret);
      }
      trans_param.did_async_commit_ = true;
      // ignore the return code of end_trans
      trans_param.had_do_response_ = true; // don't send response in this worker thread
      // @note after this code, the callback object can NOT be read any more!!!
      callback->destroy_cb_if_no_ref();
      trans_param.trans_desc_ = NULL;
    }
  }

  return ret;
}
