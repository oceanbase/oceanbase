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

#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_trans_service.h"

namespace oceanbase

{

namespace transaction
{

int ObPartTransCtx::init_log_cbs_(const share::ObLSID &ls_id, const ObTransID &tx_id)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(reserve_log_cb_group_.check_and_reset_log_cbs(false))) {
    TRANS_LOG(ERROR, "reset reserve_log_cb_group failed in the init function", K(ret), K(tx_id),
              K(ls_id_), K(reserve_log_cb_group_));
  } else if (OB_FAIL(reserve_log_cb_group_.init(ObTxLogCbGroup::RESERVED_LOG_CB_GROUP_NO))) {
    TRANS_LOG(WARN, "init a log cb group failed", K(ret), K(ls_id), K(tx_id),
              K(reserve_log_cb_group_));
  } else if (OB_FAIL(reserve_log_cb_group_.occupy_by_tx(this))) {
    TRANS_LOG(WARN, "set tx id in log cb group failed", K(ret), K(ls_id), K(tx_id),
              K(reserve_log_cb_group_));
  } else {
    ATOMIC_STORE(&has_extra_log_cb_group_, false);
    ObSpinLockGuard guard(log_cb_lock_);
    for (int i = 0; i < ObTxLogCbGroup::MAX_LOG_CB_COUNT_IN_GROUP; i++) {
      if (i != ObTxLogCbGroup::FREEZE_LOG_CB_INDEX) {
        TRANS_LOG(DEBUG, "init reserved log cb into free_list", K(ret), K(ls_id), K(tx_id), K(i),
                  KPC(reserve_log_cb_group_.get_log_cb_by_index(i)));
        free_cbs_.add_last(reserve_log_cb_group_.get_log_cb_by_index(i));
      }
    }
  }

  return ret;
}

int ObPartTransCtx::extend_log_cb_group_()
{
  int ret = OB_SUCCESS;

  // lock with log_cb_lock_
  ObTxLogCbGroup *group_ptr = nullptr;
  if (OB_FAIL(
          get_ls_tx_ctx_mgr()->get_log_cb_pool_mgr().acquire_idle_log_cb_group(group_ptr, this))) {
    TRANS_LOG(WARN, "acquire a idle log cb group failed", K(ret), KPC(group_ptr), K(trans_id_),
              K(ls_id_));
  } else if (false == (extra_cb_group_list_.add_last(group_ptr))) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "insert into extra_cb_group_list_ failed", K(ret), KPC(group_ptr),
              K(trans_id_), K(ls_id_));
  } else {
    ATOMIC_STORE(&has_extra_log_cb_group_, true);
    for (int i = 0; i < ObTxLogCbGroup::MAX_LOG_CB_COUNT_IN_GROUP; i++) {
      free_cbs_.add_last(group_ptr->get_log_cb_by_index(i));
    }
  }

  TRANS_LOG(INFO, "extend a log cb group", K(ret), K(trans_id_), K(ls_id_), KPC(group_ptr));

  return ret;
}

void ObPartTransCtx::reset_log_cb_list_(common::ObDList<ObTxLogCb> &cb_list) { cb_list.clear(); }

void ObPartTransCtx::reset_log_cbs_()
{
  int tmp_ret = OB_SUCCESS;

  ObSpinLockGuard guard(log_cb_lock_);
  reset_log_cb_list_(free_cbs_);
  reset_log_cb_list_(busy_cbs_);

  ObTxLogCbGroup *cb_group = nullptr;
  const int64_t extra_cb_group_cnt = extra_cb_group_list_.get_size();
  for (int i = 0; i < extra_cb_group_cnt; i++) {
    cb_group = nullptr;
    if (OB_ISNULL(cb_group = extra_cb_group_list_.remove_first())) {
      tmp_ret = OB_ERR_UNEXPECTED;
      TRANS_LOG_RET(ERROR, tmp_ret, "remove extra cb failed", K(tmp_ret), KPC(cb_group), KPC(this));
    } else if (OB_TMP_FAIL(ObTxLogCbPool::free_target_group(cb_group))) {
      TRANS_LOG_RET(ERROR, tmp_ret, "free a target group failed", K(tmp_ret), KPC(cb_group),
                    KPC(this));
    }
    if (OB_SUCCESS == tmp_ret) {
      get_ls_tx_ctx_mgr()->get_log_cb_pool_mgr().dec_ls_occupying_cnt();
    }
  }
}

int ObPartTransCtx::prepare_log_cb_(const bool need_freeze_cb, ObTxLogCb *&log_cb)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_log_cb_(need_freeze_cb, log_cb)) && REACH_TIME_INTERVAL(100 * 1000)) {
    TRANS_LOG(WARN, "failed to get log_cb", KR(ret), K(*this));
  }
  return ret;
}

int ObPartTransCtx::get_log_cb_(const bool need_freeze_cb, ObTxLogCb *&log_cb)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_NOT_NULL(log_cb)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid log cb", K(ret), K(need_freeze_cb), KP(log_cb), K(trans_id_),
              K(ls_id_));
  } else {

    if (need_freeze_cb) {
      ObTxLogCb *tmp_cb = nullptr;
      if (OB_ISNULL(tmp_cb = reserve_log_cb_group_.get_log_cb_by_index(
                        ObTxLogCbGroup::FREEZE_LOG_CB_INDEX))) {
        ret = OB_TX_NOLOGCB;
        TRANS_LOG(WARN, "none freeze log cb ", K(ret), KPC(tmp_cb), K(reserve_log_cb_group_));
      } else if (tmp_cb->is_busy()) {
        ret = OB_TX_NOLOGCB;
        TRANS_LOG(WARN, "the freeze log cb is busy", K(ret), KPC(tmp_cb), K(reserve_log_cb_group_));
      } else {
        log_cb = tmp_cb;
        TRANS_LOG(DEBUG, "get a freeze log cb", K(ret), K(trans_id_), K(ls_id_), KPC(log_cb),
                  K(reserve_log_cb_group_));
      }
    }

    if (OB_TX_NOLOGCB == ret || (OB_SUCCESS == ret && OB_ISNULL(log_cb))) {
      ObSpinLockGuard guard(log_cb_lock_);
      if (free_cbs_.is_empty()) {
        const int64_t busy_cbs_cnt = busy_cbs_.get_size();
        const int64_t trx_max_log_cb_limit =
            trans_service_->get_tx_elr_util().get_trx_max_log_cb_limit() >= 0
                ? trans_service_->get_tx_elr_util().get_trx_max_log_cb_limit()
                : 16;
        if (busy_cbs_cnt < trx_max_log_cb_limit || trx_max_log_cb_limit <= 0) {
          if (OB_TMP_FAIL(extend_log_cb_group_())) {
            TRANS_LOG(WARN, "extend a log cb group  failed", K(ret), K(tmp_ret), K(trans_id_),
                      K(ls_id_));
          } else {
            TRANS_LOG(INFO, "extend log cb group success", K(ret), K(tmp_ret), K(trans_id_),
                      K(ls_id_), K(busy_cbs_cnt), K(busy_cbs_.get_size()), K(free_cbs_.get_size()));
          }
        } else if (EXECUTE_COUNT_PER_SEC(10)) {
          TRANS_LOG(INFO, "The configured limit of log_cbs has been reached", K(ret), K(tmp_ret),
                    K(trans_id_), K(ls_id_), K(busy_cbs_cnt), K(trx_max_log_cb_limit),
                    K(free_cbs_.get_size()));
        }
      }

      if (OB_ISNULL(log_cb = free_cbs_.remove_first())) {
        ret = OB_TX_NOLOGCB;
        TRANS_LOG(WARN, "no free cbs in ctx", KR(ret), K(free_cbs_.get_size()), K(*this));
      } else {
        TRANS_LOG(DEBUG, "get a extra log cb", K(ret), K(trans_id_), K(ls_id_), KPC(log_cb),
                  K(reserve_log_cb_group_));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(log_cb)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "unexpected log callback", K(ret), K(trans_id_), K(ls_id_), KPC(log_cb));
      } else {
        log_cb->reuse();
        log_cb->set_busy();
      }
    }
  }
  return ret;
}

int ObPartTransCtx::return_redo_log_cb(ObTxLogCb *log_cb)
{
  int ret = OB_SUCCESS;
  ret = return_log_cb_(log_cb);
  return ret;
}

int ObPartTransCtx::return_log_cb_(ObTxLogCb *log_cb, bool release_final_cb)
{
  int ret = OB_SUCCESS;

  UNUSED(release_final_cb);

  const bool release_freeze_cb =
      (reserve_log_cb_group_.get_log_cb_by_index(ObTxLogCbGroup::FREEZE_LOG_CB_INDEX) == log_cb);

  if (nullptr != log_cb) {
    TRANS_LOG(DEBUG, "return a log cb", K(ret), KPC(log_cb), K(release_final_cb),
              K(release_freeze_cb));
    log_cb->reuse();
    if (release_freeze_cb) {
      // do nothing
    } else {
      ObSpinLockGuard guard(log_cb_lock_);
      free_cbs_.add_first(log_cb);
    }
  }

  return ret;
}

} // namespace transaction

} // namespace oceanbase
