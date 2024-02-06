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

#include "ob_xa_ctx.h"
#include "ob_xa_rpc.h"
#include "ob_xa_service.h"
#include "ob_xa_ctx_mgr.h"
#include "ob_trans_service.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;

namespace transaction
{

//ATTENTION, init order
ObXACtx::ObXACtx() : xa_branch_info_(NULL)
{
  reset();
}

void ObXACtx::destroy()
{
  if (is_inited_) {
    if (OB_NOT_NULL(xa_branch_info_)) {
      xa_branch_info_->~ObXABranchInfoArray();
      mtl_free(xa_branch_info_);
      xa_branch_info_ = NULL;
    }
    for (int i = 0; i < dblink_client_array_.count(); i++) {
      ObDBLinkClient *client = dblink_client_array_.at(i);
      if (NULL != client) {
        client->~ObDBLinkClient();
        mtl_free(client);
      }
      client = NULL;
    }
    dblink_client_array_.reset();
    REC_TRACE_EXT(tlog_, destroy, OB_ID(ctx_ref), get_uref());
    if (need_print_trace_log_) {
      FORCE_PRINT_TRACE(&tlog_, "[xa trans]");
    }
    is_inited_ = false;
  }
}

void ObXACtx::reset()
{
  xid_.reset();
  xa_service_ = NULL;
  original_sche_addr_.reset();
  is_exiting_ = false;
  trans_id_.reset();
  is_executing_ = false;
  is_xa_end_trans_ = false;
  is_xa_readonly_ = false;
  xa_trans_state_ = ObXATransState::UNKNOWN;
  is_xa_one_phase_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  xa_rpc_ = NULL;
  timer_ = NULL;
  timeout_task_.reset();
  xa_start_cond_.reset();
  xa_sync_status_cond_.reset();
  xa_branch_count_ = 0;
  xa_ref_count_ = 0;
  lock_grant_ = 0;
  is_tightly_coupled_ = false;
  lock_xid_.reset();
  if (OB_NOT_NULL(xa_branch_info_)) {
    xa_branch_info_->reset();
  }
  xa_stmt_info_.reset();
  is_terminated_ = false;
  tlog_.reset();
  need_print_trace_log_ = true;
  tx_desc_ = NULL;
  is_xa_one_phase_ = false;
  for (int i = 0; i < dblink_client_array_.count(); i++) {
    ObDBLinkClient *client = dblink_client_array_.at(i);
    if (NULL != client) {
      client->~ObDBLinkClient();
      mtl_free(client);
    }
    client = NULL;
  }
  dblink_client_array_.reset();
  has_tx_level_temp_table_ = false;
  local_lock_level_ = -1;
  executing_xid_.reset();
  need_stmt_lock_ = true;
  is_inited_ = false;
}

//is_original_ could be replaced with GCTX.self_addr() == original_sche_addr_
//init at least need to handle tight_couple and is_original
//Members which changes every time xa start is called should be handled in xa_start,
//others should be inited in init() call
int ObXACtx::init(const ObXATransID &xid,
                  const ObTransID &trans_id,
                  const uint64_t tenant_id,
                  const common::ObAddr &scheduler_addr,
                  const bool is_tightly_coupled,
                  ObXAService *xa_service,
                  ObXACtxMgr *xa_ctx_mgr,
                  ObXARpc *xa_rpc,
                  ObITransTimer *timer)
{
  int ret = OB_SUCCESS;
  
  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "ObXACtx init twice", K(xid));
  } else if (!xid.is_valid() ||
             !trans_id.is_valid() ||
             !is_valid_tenant_id(tenant_id) ||
             !scheduler_addr.is_valid() ||
             OB_ISNULL(xa_service) ||
             OB_ISNULL(xa_ctx_mgr) ||
             OB_ISNULL(xa_rpc) ||
             OB_ISNULL(timer)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(xid), K(trans_id), K(tenant_id),
                                        K(scheduler_addr), KP(xa_service),
                                        KP(xa_rpc), KP(xa_ctx_mgr), KP(timer));
  } else if (OB_FAIL(timeout_task_.init(this))) {
    TRANS_LOG(WARN, "timeout task init failed", K(ret), K(xid), K(trans_id));
  } else {
    xid_ = xid;
    trans_id_ = trans_id;
    original_sche_addr_ = scheduler_addr;
    is_tightly_coupled_ = is_tightly_coupled;
    xa_service_ = xa_service;
    xa_ctx_mgr_ = xa_ctx_mgr;
    xa_rpc_ = xa_rpc;
    timer_ = timer;
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  REC_TRACE_EXT(tlog_, init, OB_Y(ret), OB_ID(trans_id), trans_id_, OB_ID(xid), xid_,
      OB_ID(ctx_ref), get_uref());

  return ret;
}

int ObXACtx::handle_timeout(const int64_t delay)
{
  int ret = OB_SUCCESS;
  TRANS_LOG(INFO, "start to handle timeout for xa trans", K(*this), "lbt", lbt());

  if (OB_SUCC(lock_.wrlock(common::ObLatchIds::XA_CTX_LOCK, 5000000/*5 seconds*/))) {
    if (is_exiting_) {
      ret = OB_TRANS_IS_EXITING;
      TRANS_LOG(WARN, "xa ctx is exiting", K(ret));
    } else if (is_terminated_) {
      ret = OB_TRANS_IS_EXITING;
      TRANS_LOG(WARN, "xa trans has terminated", K(ret));
    } else if (ObXATransState::has_submitted(xa_trans_state_) && !is_xa_one_phase_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "xa trans has entered commit phase, unexpected", K(ret), K(*this));
    } else if (ObXATransState::has_submitted(xa_trans_state_)) {
      // do nothing
    } else {
      timeout_task_.set_running(true);
      if (get_original_sche_addr() == GCONF.self_addr_) {
        if (OB_FAIL(xa_rollback_terminate_(ObTxAbortCause::IMPLICIT_ROLLBACK))) {
          TRANS_LOG(WARN, "xa rollback terminate failed", K(ret), K(*this));
        }
        if (is_tightly_coupled_) {
          if (OB_FAIL(xa_service_->delete_xa_all_tightly_branch(tenant_id_, xid_))) {
            TRANS_LOG(WARN, "delete all tightlu branch failed", K(ret), K(*this));
          }
        } else {
          if (OB_FAIL(xa_service_->delete_xa_record(tenant_id_, xid_))) {
            TRANS_LOG(WARN, "delete xa record failed", K(ret), K(*this));
          }
        }
      } else {
        set_terminated_();
      }
      try_exit_();
      timeout_task_.set_running(false);
    }
    lock_.unlock();
  } else {
    TRANS_LOG(WARN, "xa trans handle timeout failed", K(ret), K(*this));
    if (OB_FAIL(register_timeout_task_(delay))) {
      TRANS_LOG(WARN, "register timeout handler error", K(ret), K(*this));
    }
  }

  REC_TRACE_EXT(tlog_, handle_timeout, OB_Y(ret), OB_ID(ctx_ref), get_uref());

  TRANS_LOG(INFO, "xa trans timeout", K(*this));

  return ret;
}

int ObXACtx::wait_xa_start_complete()
{
  int ret = OB_SUCCESS;
  const int64_t wait_time = 10000000;//10s
  int result = OB_SUCCESS;

  if (OB_FAIL(xa_start_cond_.wait(wait_time, result)) || OB_FAIL(result)) {
    TRANS_LOG(WARN, "wait xa start complete failed", K(ret), K(result));
  }
  return ret;
}

int ObXACtx::kill()
{
  REC_TRACE_EXT(tlog_, kill, OB_ID(ctx_ref), get_uref());
  return OB_NOT_SUPPORTED;
}

int ObXACtx::check_terminated_() const
{
  int ret = OB_SUCCESS;
  if (is_terminated_) {
    ret = OB_TRANS_XA_BRANCH_FAIL;
  } else if (is_exiting_) {
    if (is_tightly_coupled_) {
      ret = OB_TRANS_XA_BRANCH_FAIL;
    } else {
      ret = OB_TRANS_IS_EXITING;
    }
  }

  return ret;
}

int ObXACtx::is_one_phase_end_trans_allowed_(const ObXATransID &xid, const bool is_rollback)
{
  int ret = OB_SUCCESS;

  if (!is_rollback) {
    for (int64_t i = 0; i < xa_branch_info_->count() && OB_SUCC(ret); ++i) {
      const ObXABranchInfo &info = xa_branch_info_->at(i);
      if (info.xid_.all_equal_to(xid)) {
        if (ObXATransState::IDLE != info.state_) {
          ret = OB_TRANS_XA_PROTO;
          print_branch_info_();
        }
      } else if (ObXATransState::PREPARED != info.state_) {
        ret = OB_TRANS_XA_PROTO;
        print_branch_info_();
      }
    }
  } else {
    for (int64_t i = 0; i < xa_branch_info_->count() && OB_SUCC(ret); ++i) {
      const ObXABranchInfo &info = xa_branch_info_->at(i);
      if (info.xid_.all_equal_to(xid)) {
        if (ObXATransState::PREPARING == info.state_) {
          //in preparing state, the previous xa prepare req is still processing, need retry
          ret = OB_EAGAIN;
        } else if (ObXATransState::PREPARED == info.state_) {
          ret = OB_TRANS_ROLLBACKED;
        } else if (ObXATransState::IDLE != info.state_ && ObXATransState::ACTIVE != info.state_) {
          ret = OB_TRANS_XA_PROTO;
          print_branch_info_();
        }
      }
    }
  }

  return ret;
}

int ObXACtx::wait_xa_sync_status_(const int64_t expired_time)
{
  int ret = OB_SUCCESS;
  int result = OB_SUCCESS;
  if (0 > expired_time) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(expired_time));
  } else {
    if (OB_FAIL(xa_sync_status_cond_.wait(expired_time, result)) || OB_FAIL(result)) {
      TRANS_LOG(WARN, "wait xa sync status failed", K(ret), "context", *this, K(expired_time), K(result));
    }
  }
  //TRANS_LOG(INFO, "wait_xa_sync_status completed", K(ret), K(this));
  return ret;
}

int ObXACtx::init_xa_branch_info_()
{
  int ret = OB_SUCCESS;

  if (NULL == xa_branch_info_) {
    void *ptr = NULL;
    if (NULL == (ptr = mtl_malloc(sizeof(ObXABranchInfoArray), "XABranchInfo"))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "allocate memory failed", K(ret), K(*this));
    } else {
      xa_branch_info_ = new(ptr) ObXABranchInfoArray();
    }
  }
  return ret;
}

int ObXACtx::update_xa_branch_info_(const ObXATransID &xid,
                                    const int64_t to_state,
                                    const ObAddr &addr,
                                    const int64_t timeout_seconds,
                                    const int64_t end_flag)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(xa_branch_info_) && OB_FAIL(init_xa_branch_info_())) {
    TRANS_LOG(WARN, "init xa branch timeout array failed", K(ret), K(xid), K(*this));
  } else {
    bool found = false;
    int64_t now = ObTimeUtil::current_time();
    for (int64_t i = 0; !found && i < xa_branch_info_->count(); ++i) {
      ObXABranchInfo &info = xa_branch_info_->at(i);
      if (info.xid_.all_equal_to(xid)) {
        info.state_ = to_state;
        info.unrespond_msg_cnt_ = 0;
        info.last_hb_ts_ = now;
        if (ObXATransState::ACTIVE == to_state) {
          //only when xa start is called may addr be updated
          info.addr_ = addr;
        }
        if (ObXATransState::IDLE == to_state) {
          //already contains loose flag if needed
          info.end_flag_ = ObXAFlag::add_end_flag(info.end_flag_, end_flag);
          info.abs_expired_time_ = now + info.timeout_seconds_ * 1000000;
        } else {
          info.abs_expired_time_ = INT64_MAX;
        }
        found = true;
      }
    }
    if (!found) {
      if (ObXATransState::ACTIVE != to_state) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "branch info not found in array", K(ret), K(xid), K(*this));
      } else {
        ObXABranchInfo info;
        if (OB_FAIL(info.init(xid, to_state, timeout_seconds, INT64_MAX,
                              addr, 0, ObTimeUtil::current_time(), end_flag))) {
          TRANS_LOG(WARN, "branch info init failed", K(ret), K(*this));
        } else if (OB_FAIL(xa_branch_info_->push_back(info))) {
          TRANS_LOG(WARN, "push xa branch info failed", K(ret), K(info), K(*this));
        } else {
          TRANS_LOG(INFO, "add new branch info", K(info), K(*this), "lbt", lbt());
        }
      }
    }
    has_tx_level_temp_table_ = ObXAFlag::contain_temp_table(end_flag) ? true : has_tx_level_temp_table_;
  }

  return ret;
}

int ObXACtx::register_timeout_task_(const int64_t interval_us)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (interval_us < 0) {
    TRANS_LOG(WARN, "invalid argument", K(interval_us));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(timer_)) {
    TRANS_LOG(ERROR, "transaction timer is null", K_(trans_id));
    ret = OB_ERR_UNEXPECTED;
    //first add ref and then register timeout task
  } else if (NULL == xa_ctx_mgr_) {
    TRANS_LOG(ERROR, "partition mgr is null, unexpected error", KP_(xa_ctx_mgr), K_(trans_id));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(xa_ctx_mgr_->acquire_ctx_ref(trans_id_))) {
    TRANS_LOG(WARN, "get transaction ctx for inc ref error", K(ret), K_(trans_id));
  } else {
    if (OB_FAIL(timer_->register_timeout_task(timeout_task_, interval_us))) {
      TRANS_LOG(WARN, "register timeout task error", K(ret), K(interval_us), K_(trans_id));
      //when register failed, release ref
      (void)xa_ctx_mgr_->release_ctx_ref(this);
    }
  }

  return ret;
}

int ObXACtx::unregister_timeout_task_()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(timer_)) {
    TRANS_LOG(ERROR, "transaction timer is null", K_(trans_id));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(timer_->unregister_timeout_task(timeout_task_))) {
    // rewrite ret
    if (OB_TIMER_TASK_HAS_NOT_SCHEDULED == ret) {
      ret = OB_SUCCESS;
    }
  } else {
    //just dec ctx ref
    if (OB_ISNULL(xa_ctx_mgr_)) {
      TRANS_LOG(ERROR, "partition mgr is null, unexpected error", KP_(xa_ctx_mgr), K_(trans_id));
      ret = OB_ERR_UNEXPECTED;
    } else {
      (void)xa_ctx_mgr_->release_ctx_ref(this);
    }
  }

  return ret;
}

int ObXACtx::register_xa_timeout_task_()
{
  int ret = OB_SUCCESS;
  int64_t target = INT64_MAX;

  if (OB_ISNULL(xa_branch_info_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "branch info array is null", K(ret), K(*this));
  } else {
    for (int64_t i = 0; i < xa_branch_info_->count(); ++i) {
      const ObXABranchInfo &info = xa_branch_info_->at(i);
      if (ObXATransState::IDLE == info.state_ && info.abs_expired_time_ < target) {
        target = info.abs_expired_time_;
      }
    }
    if (target > tx_desc_->get_expire_ts()) {
      target = tx_desc_->get_expire_ts();
    }
    (void)unregister_timeout_task_();
    if (INT64_MAX != target) {
      target = target - ObTimeUtil::current_time();
      if (OB_LIKELY(target > 0)) {
        if (OB_FAIL(register_timeout_task_(target))) {
          TRANS_LOG(WARN, "register xa timeout task failed", K(ret), K(target), K(*this));
        }
      }
    }
  }
  TRANS_LOG(INFO, "register xa timeout task", K(target), K(ret), K(*this));
  return ret;
}

// for two cases
// case 1: first xa start
// case 2: xa start remote first
void ObXACtx::notify_xa_start_complete_(int ret_code)
{
  xa_start_cond_.notify(ret_code);
}

int ObXACtx::get_branch_info_(const ObXATransID &xid,
                              ObXABranchInfo &info) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(xa_branch_info_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa branch info is null", K(ret));
  } else {
    const int64_t count = xa_branch_info_->count();
    bool found = false;
    for (int64_t i = 0; i < count && !found; ++i) {
      const ObXABranchInfo &local_info = xa_branch_info_->at(i);
      if (local_info.xid_.all_equal_to(xid)) {
        info = local_info;
        found = true;
      }
    }
    if (!found) {
      ret = OB_ERR_UNEXPECTED;
    }
  }

  return ret;
}

void ObXACtx::set_terminated_()
{
  TRANS_LOG(INFO, "set terminated", K_(is_terminated), K(*this), "lbt", lbt());
  is_terminated_ = true;
  need_print_trace_log_ = true;
  REC_TRACE_EXT(tlog_, terminate, OB_ID(ctx_ref), get_uref());
}

// this is used to roll back xa trans for exceptions
// e.g., timeout, session terminate, protocol error
int ObXACtx::xa_rollback_terminate_(const int cause)
{
  int ret = OB_SUCCESS;

  (void)unregister_timeout_task_();
  if (OB_FAIL(MTL(ObTransService*)->abort_tx(*tx_desc_, cause))) {
    TRANS_LOG(WARN, "abort tx for session terminate failed", K(ret), K(*this));
  }
  set_terminated_();

  return ret;
}

int ObXACtx::check_join_(const ObXATransID &xid) const
{
  int ret = OB_SUCCESS;
  ObXABranchInfo info;

  if (OB_FAIL(get_branch_info_(xid, info))) {
    TRANS_LOG(WARN, "get branch info failed", K(ret), K(xid));
  } else if (ObXATransState::IDLE != info.state_) {
    ret = OB_TRANS_XA_PROTO;
    TRANS_LOG(WARN, "xa branch state should be idle", K(ret), K(xid), K(info));
  } else if (!ObXAFlag::is_valid_inner_flag(info.end_flag_)) {
    ret = OB_TRANS_XA_PROTO;
    TRANS_LOG(WARN, "unexpected xa trans end flag", K(ret), K(xid), K(info));
  } else if (is_tightly_coupled_ == ObXAFlag::contain_loosely(info.end_flag_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected tight couple flag", K(ret), K(xid), K(*this));
  }

  return ret;
}

int ObXACtx::stmt_lock_with_guard(const ObXATransID &xid)
{
  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  return stmt_lock_(xid);
}

int ObXACtx::stmt_lock_(const ObXATransID &xid)
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS != (ret = xa_stmt_lock_.try_wrlock(common::ObLatchIds::XA_STMT_LOCK))) {
    if (OB_UNLIKELY(!lock_xid_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected lock xid", K(*this));
    } else if (OB_UNLIKELY(lock_xid_.all_equal_to(xid))) {
      //hold the lock itself
      TRANS_LOG(INFO, "xa global lock hold by self", K(xid), K(lock_xid_), K(lock_grant_));
      ret = OB_SUCCESS;
    } else {
      ret = OB_TRANS_STMT_NEED_RETRY;
      TRANS_LOG(INFO, "xa get global lock failed", K(*this), K(xid));
    }
  } else {
    lock_xid_ = xid;
    ++lock_grant_;
    TRANS_LOG(INFO, "xa grant global lock", K(xid), K(*this), K(lock_grant_));
    REC_TRACE_EXT(tlog_, stmt_lock, OB_ID(bqual), xid.get_bqual_hash());
  }

  return ret;
}

int ObXACtx::stmt_unlock_with_guard(const ObXATransID &xid)
{
  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  return stmt_unlock_(xid);
}

int ObXACtx::stmt_unlock_(const ObXATransID &xid)
{
  int ret = OB_SUCCESS;

  if (!lock_xid_.all_equal_to(xid)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected error when release xa global lock", K(ret), K(xid), K(*this));
  } else if (OB_FAIL(xa_stmt_lock_.unlock())) {
    TRANS_LOG(WARN, "unexpected lock state", K(ret), K(xid), K(*this));
  } else {
    --lock_grant_;
    lock_xid_.reset();
    TRANS_LOG(INFO, "xa release global lock", K(xid), K(*this), K(lock_grant_));
    REC_TRACE_EXT(tlog_, stmt_unlock, OB_ID(bqual), xid.get_bqual_hash());
  }

  return ret;
}

static const int64_t MAX_UNRESPOND_XA_HB_CNT = 3;
static const int64_t XA_HB_THRESHOLD = 3000000;//3S

int ObXACtx::xa_scheduler_hb_req()
{
  int ret = OB_SUCCESS;
  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  if (!is_tightly_coupled_) {
    // do nothing
  } else if (OB_ISNULL(xa_branch_info_)) {
    // mostly a temproray scheduler, do nothing
  } else {
    int64_t now = ObTimeUtil::current_time();
    for (int64_t i = 0; i < xa_branch_info_->count(); ++i) {
      ObXABranchInfo &branch_info = xa_branch_info_->at(i);
      if (branch_info.addr_ == GCTX.self_addr()) {
        //do nothing
      } else if (ObXATransState::ACTIVE != branch_info.state_) {
        //do nothing
      } else if (branch_info.unrespond_msg_cnt_ > MAX_UNRESPOND_XA_HB_CNT) {
        const bool is_rollback = true;
        //if (OB_FAIL(one_phase_xa_end_trans(xid_, is_rollback, 0))) {
        //  TRANS_LOG(WARN, "rollback xa trans failed", KR(ret), K(*this));
        //} else {
        //  // is_terminated_ = true;
        //  set_terminated_();
        //}
        TRANS_LOG(INFO, "scheduler unrespond, rollbacked", K(ret), K(branch_info), K(*this));
        break;
      } else if (now > branch_info.last_hb_ts_ + XA_HB_THRESHOLD) {
        ObXAHbRequest req;
        if (OB_FAIL(req.init(trans_id_, branch_info.xid_, GCTX.self_addr()))) {
          TRANS_LOG(WARN, "xa hb request init failed", KR(ret), K(branch_info), K(*this));
        } else if (OB_FAIL(xa_rpc_->xa_hb_req(tenant_id_, branch_info.addr_, req, NULL))) {
          TRANS_LOG(WARN, "post xa  hb req failed", KR(ret), K(branch_info), K(*this));
        } else {
          branch_info.unrespond_msg_cnt_++;
        }
        TRANS_LOG(INFO, "post heartbeat for xa trans branch", K(ret), K(branch_info), K(*this));
      }
    }
  }
  return ret;
}

//also considers the situation where original scheduler and
//xid trans are on same machine
int ObXACtx::update_xa_branch_hb_info_(const ObXATransID &xid)
{
  int ret = OB_SUCCESS;

  if (is_exiting_ || is_terminated_) {
    //do nothing
  } else if (OB_ISNULL(xa_branch_info_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "branch info array is null", K(ret), K(xid), K(*this));
  } else {
    bool found = false;
    for (int64_t i = 0; !found && i < xa_branch_info_->count(); ++i) {
      ObXABranchInfo &info = xa_branch_info_->at(i);
      if (info.xid_.all_equal_to(xid)) {
        info.unrespond_msg_cnt_ = 0;
        info.last_hb_ts_ = ObTimeUtil::current_time();
        found = true;
      }
    }
    if (OB_UNLIKELY(!found)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "branch info not found in array", K(ret), K(xid), K(*this));
    }
  }
  return ret;
}

void ObXACtx::print_branch_info_() const
{
  if (OB_NOT_NULL(xa_branch_info_)) {
    for (int64_t i = 0; i < xa_branch_info_->count(); ++i) {
      const ObXABranchInfo &info = xa_branch_info_->at(i);
      TRANS_LOG(INFO, "branch", K(i), K(info));
    }
  }
  return;
}

// process xa start request from temporary scheduler
// if branch fail and ref count is zero, exit
int ObXACtx::process_xa_start(const obrpc::ObXAStartRPCRequest &req)
{
  int ret = OB_SUCCESS;

  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  if (OB_UNLIKELY(!req.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(req));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa ctx not inited", K(ret), K(req));
  } else if (req.is_tightly_coupled() != is_tightly_coupled_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected xa trans mode", K(ret), K(*this), K(req));
  } else if (is_tightly_coupled_) {
    if (OB_FAIL(process_xa_start_tightly_(req))) {
      TRANS_LOG(WARN, "xa start tightly mode failed", K(ret), K(*this));
    }
  } else {
    if (OB_FAIL(process_xa_start_loosely_(req))) {
      TRANS_LOG(WARN, "xa start loosely mode failed", K(ret), K(*this));
    }
  }

  TRANS_LOG(INFO, "xa start", K(ret), K(req), K_(xa_ref_count));

  return ret;
}

int ObXACtx::process_xa_start_tightly_(const obrpc::ObXAStartRPCRequest &req)
{
  int ret = OB_SUCCESS;
  const ObXATransID &xid = req.get_xid();
  const ObAddr &sender = req.get_sender();
  const bool is_new_branch = req.is_new_branch();
  const int64_t flags = req.get_flags();
  const int64_t timeout_seconds = req.get_timeout_seconds();

  if (!is_new_branch && OB_FAIL(check_join_(xid))) {
    TRANS_LOG(WARN, "check join failed", K(ret), K(xid), K(*this));
  } else if (OB_FAIL(check_for_execution_(xid, is_new_branch))) {
    // include branch fail
    TRANS_LOG(WARN, "check for execution failed", K(ret), K(xid), K(*this));
  } else if (OB_ISNULL(tx_desc_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "tx desc is null", K(ret), K(*this), K(xid));
  } else {
    if (OB_FAIL(update_xa_branch_info_(xid, 
                                       ObXATransState::ACTIVE,
                                       sender,
                                       timeout_seconds,
                                       flags))) {
      TRANS_LOG(WARN, "update xa branch info failed", K(ret), K(*this), K(xid));
    } else if (OB_FAIL(register_xa_timeout_task_())) {
      TRANS_LOG(WARN, "register xa timeout task failed", K(ret), K(xid), K(*this));
    } else if (is_new_branch) {
      ++xa_branch_count_;
    }
    if (OB_SUCC(ret)) {
      SMART_VAR(ObXAStartRPCResponse, response) {
        if (OB_FAIL(response.init(trans_id_, *tx_desc_, req.is_first_branch()))) {
          TRANS_LOG(WARN, "init xa start response failed", K(ret));
        } else if (OB_FAIL(xa_rpc_->xa_start_response(tenant_id_, sender, response, NULL))) {
          TRANS_LOG(WARN, "xa start response failed", K(ret));
        }
      }
    }
  }

  REC_TRACE_EXT(tlog_, xa_start_request, OB_Y(ret), OB_ID(bqual), xid.get_bqual_hash(),
      OB_ID(ctx_ref), get_uref());
  return ret;
}

int ObXACtx::process_xa_start_loosely_(const obrpc::ObXAStartRPCRequest &req)
{
  int ret = OB_SUCCESS;
  const ObXATransID &xid = req.get_xid();
  const ObAddr &sender = req.get_sender();
  const int64_t timeout_seconds = req.get_timeout_seconds();
  const int64_t unused_flag = ObXAFlag::OBTMNOFLAGS;
  const bool is_new_branch = req.is_new_branch();
  ObXABranchInfo info;

  if (OB_ISNULL(tx_desc_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "tx desc is null", K(ret), K(*this), K(xid));
  } else if (!is_new_branch && OB_FAIL(check_join_(xid))) {
    TRANS_LOG(WARN, "check join failed", K(ret), K(xid), K(*this));
  } else if (OB_FAIL(check_for_execution_(xid, false))) {
    TRANS_LOG(WARN, "check for execution failed", K(ret), K(xid), K(*this));
  } else if (OB_FAIL(update_xa_branch_info_(xid,
                                            ObXATransState::ACTIVE,
                                            sender,
                                            timeout_seconds,
                                            unused_flag))) {
    TRANS_LOG(WARN, "update xa branch info failed", K(ret), K(xid), K(*this));
  } else if (OB_FAIL(register_xa_timeout_task_())) {
    TRANS_LOG(WARN, "register xa trans timeout task failed", K(ret), K(xid), K(*this));
  } else {
    SMART_VAR(ObXAStartRPCResponse, response) {
      if (OB_FAIL(response.init(trans_id_, *tx_desc_, req.is_first_branch()))) {
        TRANS_LOG(WARN, "init xa start response failed", K(ret));
      } else if (OB_FAIL(xa_rpc_->xa_start_response(tenant_id_, sender, response, NULL))) {
        TRANS_LOG(WARN, "xa start response failed", K(ret));
      }
    }
  }
  REC_TRACE_EXT(tlog_, xa_start_request, OB_Y(ret), OB_ID(ctx_ref), get_uref());

  return ret;
}

int ObXACtx::process_xa_start_response(const obrpc::ObXAStartRPCResponse &resp)
{
  int ret = OB_SUCCESS;
  const ObTxInfo &tx_info = resp.get_tx_info();
  // no need guard
  if (resp.is_first_branch() && NULL != tx_desc_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "trans desc is NULL", K(ret));
  } else if (!resp.is_first_branch()) {
    if (OB_FAIL(MTL(ObTransService *)->update_user_savepoint(*tx_desc_, tx_info.savepoints_))) {
      TRANS_LOG(WARN, "update user sp fail", K(ret), K(*this), K(tx_info));
    }
  } else if (OB_FAIL(MTL(ObTransService *)->recover_tx(tx_info, tx_desc_))) {
    TRANS_LOG(WARN, "recover tx failed", K(ret), K(*this), K(tx_info));
  } else {
    // do nothing
  }

  TRANS_LOG(INFO, "xa start response", K(ret), K(*this));
  xa_sync_status_cond_.notify(ret);
  REC_TRACE_EXT(tlog_, xa_start_response, OB_Y(ret), OB_ID(ctx_ref), get_uref());

  return ret;
}

int ObXACtx::process_xa_end(const obrpc::ObXAEndRPCRequest &req)
{
  int ret = OB_SUCCESS;
  const ObXATransID &xid = req.get_xid();
  const ObTxStmtInfo &stmt_info = req.get_stmt_info();
  // already contains loose flag if needed
  const int64_t end_flag = req.get_end_flag();
  //const int64_t timeout_seconds = trans_desc.get_xa_end_timeout_seconds();
  const int64_t fake_timeout_seconds = 60;
  ObAddr fake_addr;

  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  if (OB_UNLIKELY(!req.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(req), K(*this));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa ctx not inited", K(ret), K(req), K(*this));
  } else if (is_exiting_) {
    ret = OB_TRANS_IS_EXITING;
    TRANS_LOG(WARN, "xa trans is exiting", K(ret), K(req), K(*this));
  } else if (NULL == tx_desc_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected tx desc", K(ret), K(req), K(*this));
  } else if (OB_FAIL(check_for_execution_(xid, false))) {
    // include branch fail
    TRANS_LOG(WARN, "check for execution failed", K(ret), K(xid), K(*this));
  } else {
    if (OB_FAIL(update_xa_branch_info_(xid,
                                       ObXATransState::IDLE,
                                       fake_addr,
                                       fake_timeout_seconds,
                                       end_flag))) {
      TRANS_LOG(WARN, "update xa branch info failed", K(ret), K(xid), K(*this));
    } else if (OB_FAIL(register_xa_timeout_task_())) {
      TRANS_LOG(WARN, "register xa timeout task failed", K(ret), K(xid), K(*this));
    }
    if (!is_tightly_coupled_) {
      // if loosely coupled, need to update tx desc
      if (OB_FAIL(MTL(ObTransService*)->update_tx_with_stmt_info(stmt_info, tx_desc_))) {
        TRANS_LOG(WARN, "update tx desc with stmt info failed", K(ret), K(req), K(*this));
      }
    } else {
      // if tightly coupled, need to update user savepoint
      if (OB_FAIL(MTL(ObTransService *)->update_user_savepoint(*tx_desc_, stmt_info.savepoints_))) {
        TRANS_LOG(WARN, "update user sp fail", K(ret), K(*this), K(req));
      } else if (!tx_desc_->need_rollback() && stmt_info.need_rollback()) {
        if (OB_FAIL(MTL(ObTransService *)->abort_tx(*tx_desc_, ObTxAbortCause::IMPLICIT_ROLLBACK))) {
          TRANS_LOG(WARN, "abort tx fail", K(ret), K(*this), K(req));
        }
      }
    }
  }
  if (OB_TRANS_XA_BRANCH_FAIL == ret) {
    try_exit_();
  }
  TRANS_LOG(INFO, "process xa end", K(ret), K(*this));

  return ret;
}

// handle start stmt request in original scheduler
// @param [in] req
int ObXACtx::process_start_stmt(const obrpc::ObXAStartStmtRPCRequest &req)
{
  int ret = OB_SUCCESS;

  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  if (OB_UNLIKELY(!req.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(req));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa ctx not inited", K(ret), K(req));
  } else {
    const ObXATransID &xid = req.get_xid();
    const ObAddr &sender = req.get_sender();
    const bool is_new_branch = false;

    if (OB_FAIL(check_for_execution_(xid, is_new_branch))) {
      // include branch fail
      TRANS_LOG(WARN, "check for execution failed", K(ret), K(xid), K(*this));
    } else if (OB_ISNULL(tx_desc_)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "trans desc is null", K(ret), K(xid), K(*this));
    } else {
      if (OB_FAIL(update_xa_branch_hb_info_(xid))) {
        TRANS_LOG(WARN, "update xa branch hb info failed", KR(ret), K(xid));
      } else if (OB_SUCCESS != (ret = stmt_lock_(xid))) {
        TRANS_LOG(INFO, "xa get global lock failed", K(ret), K(*this));
      } else {
        SMART_VAR(ObXAStartStmtRPCResponse, response) {
          if (OB_FAIL(response.init(trans_id_, *tx_desc_, req.get_id()))) {
            TRANS_LOG(WARN, "init start stmt response failed", K(ret));
          } else if (OB_FAIL(xa_rpc_->xa_start_stmt_response(tenant_id_, sender, response, NULL))) {
            TRANS_LOG(WARN, "xa start stmt response failed", K(ret));
          } else {
            // xa_trans_state_ = ObXATransState::ACTIVE;
            is_executing_ = true;
            executing_xid_ = xid;
          }
          if (OB_FAIL(ret)) {
            stmt_unlock_(xid);
          }
        }
      }
    }
  }
  if (OB_TRANS_XA_BRANCH_FAIL == ret) {
    try_exit_();
  }
  REC_TRACE_EXT(tlog_, xa_start_stmt_request, OB_Y(ret), OB_ID(ctx_ref), get_uref());
  TRANS_LOG(INFO, "process start stmt", K(ret), K(req), K(*this));

  return ret;
}

// handle start stmt response in non-original scheduler
// @param [in] res
int ObXACtx::process_start_stmt_response(const obrpc::ObXAStartStmtRPCResponse &res)
{
  int ret = OB_SUCCESS;
  const bool is_start_stmt_response = true;

  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  if (OB_UNLIKELY(!res.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(res), K(*this));
  } else if (NULL == tx_desc_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "trans desc is NULL", K(ret), K(res), K(*this));
  } else if (!check_response_(res.get_id(), is_start_stmt_response)) {
    ret = OB_TRANS_RPC_TIMEOUT;
    TRANS_LOG(WARN, "response is unexptected", K(ret), K_(request_id), K(res), K(*this));
  } else {
    const ObTxStmtInfo &stmt_info = res.get_stmt_info();
    if (OB_FAIL(MTL(ObTransService*)->update_tx_with_stmt_info(stmt_info, tx_desc_))) {
      TRANS_LOG(WARN, "update tx desc with stmt info failed", K(ret), K(res), K(*this));
    }
  }

  TRANS_LOG(INFO, "process start stmt response", K(ret), K(res));
  xa_sync_status_cond_.notify(ret);
  REC_TRACE_EXT(tlog_, xa_start_stmt_response, OB_Y(ret), OB_ID(ctx_ref), get_uref());

  return ret;
}

// handle end stmt in original scheduler
// @param [in] req
int ObXACtx::process_end_stmt(const obrpc::ObXAEndStmtRPCRequest &req)
{
  int ret = OB_SUCCESS;
  const ObXATransID &xid = req.get_xid();

  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  if (OB_UNLIKELY(!req.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(req), K(*this));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa ctx not inited", K(ret), K(req), K(*this));
  } else if (OB_ISNULL(tx_desc_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "trans_desc_ is null", K(ret), K(req), K(*this));
  } else if (OB_FAIL(check_for_execution_(xid, false))) {
    // include branch fail
    TRANS_LOG(WARN, "check for execution failed", K(ret), K(xid), K(*this));
  } else if (lock_xid_.empty()) {
    // The rpc timeout may trigger repeated unlocking operations,
    // which should be considered successful at this time
  } else {
    const ObTxStmtInfo &stmt_info = req.get_stmt_info();
    if (OB_FAIL(MTL(ObTransService*)->update_tx_with_stmt_info(stmt_info, tx_desc_))) {
      TRANS_LOG(WARN, "update tx desc with stmt info failed", K(ret), K(xid), K(*this));
    } else if (OB_FAIL(update_xa_branch_hb_info_(xid))) {
      TRANS_LOG(WARN, "update xa branch hb info failed", KR(ret), K(xid), K(*this));
    } else if (OB_FAIL(stmt_unlock_(xid))) {
      TRANS_LOG(WARN, "xa release global lock failed", K(ret), K(xid), K(*this));
    } else {
      is_executing_ = false;
      executing_xid_.reset();
    }
  }
  if (OB_TRANS_XA_BRANCH_FAIL == ret) {
    try_exit_();
  }
  return ret;
}

// xa start for promoting normal trans
// the tx_desc should be valid
// @param [in] xid
// @param [in] flags
// @param [in] timeout_seconds
// @param [in] tx_desc
int ObXACtx::xa_start_for_dblink(const ObXATransID &xid,
                                 const int64_t flags,
                                 const int64_t timeout_seconds,
                                 const bool need_promote,
                                 ObTxDesc *tx_desc)
{
  int ret = OB_SUCCESS;

  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  if (OB_UNLIKELY(!xid.is_valid()) || OB_ISNULL(tx_desc) || !tx_desc->is_valid()
      || 0 > timeout_seconds) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid), K(timeout_seconds));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa ctx not inited", K(ret), K(xid));
  } else if (is_exiting_) {
    ret = OB_TRANS_IS_EXITING;
    TRANS_LOG(WARN, "xa trans is exiting", K(ret), K(xid), K(flags), K(*this));
  } else if (!xid.gtrid_equal_to(xid_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected xid", K(xid), K(xid_), K(*this));
  } else if (OB_FAIL(xa_start_(xid, flags, timeout_seconds, tx_desc))) {
    TRANS_LOG(WARN, "xa start promotion failed", K(ret), K(xid), K(flags), K(*this));
  } else {
    // set global trans type to dblink trans
    tx_desc->set_global_tx_type(ObGlobalTxType::DBLINK_TRANS);
    tx_desc->set_explicit();
  }

  TRANS_LOG(INFO, "xa start for dblink", K(ret), K(xid), K(flags), K(need_promote), K(*this));
  return ret;
}

// get dblink client
// case 1, if dblink client exists, return the client
// case 2, if dblink client does not exist, generate a new one and return it
// NOTE that we can not call xa subprograms of dblink client in this funciton
// @param[in] dblink_type
// @param[in] dblink_conn
// @param[out] client
int ObXACtx::get_dblink_client(const DblinkDriverProto dblink_type,
                               ObISQLConnection *dblink_conn,
                               ObDBLinkClient *&client)
{
  int ret = OB_SUCCESS;
  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  if (OB_ISNULL(dblink_conn)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(dblink_conn));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa ctx not inited", K(ret), K(*this));
  } else if (is_exiting_) {
    ret = OB_TRANS_IS_EXITING;
    TRANS_LOG(WARN, "xa trans is exiting", K(ret), K(*this));
  } else {
    ObDBLinkClient *tmp_client = NULL;
    if (OB_FAIL(get_dblink_client_(dblink_type, dblink_conn, tmp_client))) {
      TRANS_LOG(WARN, "fail to get dblink client", K(ret), K(*this));
    } else if (NULL == tmp_client) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected dblink client", K(ret), K(*this), KP(tmp_client));
    } else {
      client = tmp_client;
    }
  }
  return ret;
}

// get dblink client
// if not exist, generate a dblink client and add it into dblink array
// @param[in] dblink_type
// @param[in] dblink_conn
// @param[out] dblink_client
int ObXACtx::get_dblink_client_(const DblinkDriverProto dblink_type,
                                ObISQLConnection *dblink_conn,
                                ObDBLinkClient *&dblink_client)
{
  int ret = OB_SUCCESS;
  static const int64_t OB_MAX_DBLINK_CLIENT_COUNT = 10;
  uint32_t client_max_index = 0;
  ObDBLinkClient *client = NULL;
  for (int i = 0; i < dblink_client_array_.count(); i++) {
    ObDBLinkClient *tmp_client = dblink_client_array_.at(i);
    if (NULL != tmp_client && tmp_client->equal(dblink_conn)) {
      client = tmp_client;
      break;
    } else {
      client_max_index = max(client_max_index, tmp_client->get_index());
    }
  }
  if (NULL == client) {
    void *ptr = NULL;
    if (dblink_client_array_.count() >= OB_MAX_DBLINK_CLIENT_COUNT) {
      ret = OB_SIZE_OVERFLOW;
      TRANS_LOG(WARN, "create unexpected dblink client num", K(ret), K(*this));
    } else if (NULL == (ptr = mtl_malloc(sizeof(ObDBLinkClient), "ObDBLinkClient"))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "allocate memory failed", K(ret), K(*this));
    } else {
      client = new(ptr) ObDBLinkClient();
      const int64_t timeout_us = tx_desc_->get_timeout_us();
      if (OB_FAIL(client->init(client_max_index + 1, dblink_type, timeout_us,
              dblink_conn))) {
        TRANS_LOG(WARN, "fail to init dblink client", K(ret), K(*this));
      } else if (OB_FAIL(dblink_client_array_.push_back(client))) {
        TRANS_LOG(WARN, "fail to push dblink client to array", K(ret), K(*this));
      }
      if (OB_SUCCESS != ret) {
        client->~ObDBLinkClient();
        mtl_free(client);
        client = NULL;
      } else {
        dblink_client = client;
      }
    }
  } else {
    dblink_client = client;
  }
  return ret;
}

// remove dblink client
// when dblink_xa_start failed, should remove dblink_client
// NOTE that we can not call xa subprograms of dblink client in this funciton
// @param[int] client_ptr
int ObXACtx::remove_dblink_client(ObDBLinkClient *client)
{
  int ret = OB_SUCCESS;
  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  if (OB_ISNULL(client)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(client));
  } else if (OB_UNLIKELY(!is_inited_) || OB_UNLIKELY(!client->is_inited())) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa ctx not inited or client not inited", K(ret), K(*this), KPC(client));
  } else if (is_exiting_) {
    ret = OB_TRANS_IS_EXITING;
    TRANS_LOG(WARN, "xa trans is exiting", K(ret), K(*this));
  } else {
    // free dblinck_client
    const int64_t array_count = dblink_client_array_.count();
    for (int64_t i = 0; i < array_count; i++) {
      if (client == dblink_client_array_.at(i)) {
        if (OB_FAIL(dblink_client_array_.remove(i))) {
          TRANS_LOG(WARN, "fail to remove dblink_client in array", K(ret), K(i), K(*this), KPC(client));
        } else {
          client->~ObDBLinkClient();
          mtl_free(client);
          client = NULL;
        }
        break;
      }
    }
    if (OB_SUCC(ret) && array_count == dblink_client_array_.count()) {
      ret = OB_ENTRY_NOT_EXIST;
      TRANS_LOG(WARN, "dblink client not hold in tx", K(ret), K(*this), KPC(client));
    }
  }
  return ret;
}

int ObXACtx::recover_tx_for_dblink_callback(ObTxDesc *&tx_desc)
{
  int ret = OB_SUCCESS;

  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  if (NULL != tx_desc) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(tx_desc), K(tx_desc->tid()));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa ctx not inited", K(ret), K(*this));
  } else if (is_exiting_) {
    ret = OB_TRANS_IS_EXITING;
    TRANS_LOG(WARN, "xa trans is exiting", K(ret), K(*this));
  } else if (is_terminated_ || 0 == xa_ref_count_) {
    ret = OB_TRANS_XA_BRANCH_FAIL;
    TRANS_LOG(WARN, "xa trans is terminated", K(ret), K(*this));
    is_terminated_ = true;
    try_exit_();
  } else if (NULL == tx_desc_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected local tx desc", K(ret), K(*this));
  } else {
    ++xa_ref_count_;
    tx_desc = tx_desc_;
  }

  TRANS_LOG(INFO, "recover tx for dblink callback", K(ret), K(*this));
  return ret;
}

int ObXACtx::revert_tx_for_dblink_callback(ObTxDesc *&tx_desc)
{
  int ret = OB_SUCCESS;

  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  if (NULL == tx_desc) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(tx_desc));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa ctx not inited", K(ret), K(*this));
  } else if (is_exiting_) {
    ret = OB_TRANS_IS_EXITING;
    TRANS_LOG(WARN, "xa trans is exiting", K(ret), K(*this));
  } else {
    --xa_ref_count_;
    tx_desc = NULL;
  }

  if (is_terminated_) {
    try_exit_();
  }

  TRANS_LOG(INFO, "revert tx for dblink callback", K(ret), K(*this));
  return ret;
}

// the first xa start of the xa trans
// the tx_desc should be valid
// @param [in] xid
// @param [in] flags
// @param [in] timeout_seconds
// @param [in] tx_desc
int ObXACtx::xa_start(const ObXATransID &xid,
                      const int64_t flags,
                      const int64_t timeout_seconds,
                      ObTxDesc *tx_desc)
{
  int ret = OB_SUCCESS;

  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  if (OB_UNLIKELY(!xid.is_valid()) || OB_ISNULL(tx_desc) || 0 > timeout_seconds) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid), K(timeout_seconds));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa ctx not inited", K(ret), K(xid));
  } else if (is_exiting_) {
    ret = OB_TRANS_IS_EXITING;
    TRANS_LOG(WARN, "xa trans is exiting", K(ret), K(xid), K(flags), K(*this));
  } else if (!xid.gtrid_equal_to(xid_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected xid", K(xid), K(xid_), K(*this));
  } else if (OB_FAIL(xa_start_(xid, flags, timeout_seconds, tx_desc))) {
    TRANS_LOG(WARN, "xa start failed", K(ret), K(xid), K(flags), K(*this));
  } else {
    // set global trans type to xa trans
    tx_desc->set_global_tx_type(ObGlobalTxType::XA_TRANS);
  }

  TRANS_LOG(INFO, "first xa start", K(ret), K(xid), K(flags), K(*this));
  return ret;
}

// not first xa start
// @param [in] xid
// @param [in] flags
// @param [in] timeout_seconds
// @param [out] tx_desc
int ObXACtx::xa_start_second(const ObXATransID &xid,
                             const int64_t flags,
                             const int64_t timeout_seconds,
                             ObTxDesc *&tx_desc)
{
  int ret = OB_SUCCESS;
  // const bool is_tightly_coupled = !ObXAFlag::contain_loosely(flags);
  const bool is_join = ObXAFlag::is_tmjoin(flags) || ObXAFlag::is_tmresume(flags);
  const bool is_original = (GCTX.self_addr() == original_sche_addr_);
  const bool is_new_branch = !is_join;

  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  if (OB_UNLIKELY(!xid.is_valid()) || 0 > timeout_seconds) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa ctx not inited", K(ret), K(xid));
  } else if (is_exiting_) {
    ret = OB_TRANS_IS_EXITING;
    TRANS_LOG(WARN, "xa trans is exiting", K(ret), K(xid), K(flags), K(*this));
  } else if (OB_FAIL(check_for_execution_(xid, is_new_branch))) {
    //include branch fail
    TRANS_LOG(WARN, "check for execution failed", K(ret), K(xid), K(*this));
  } else {
    if (is_original) {
      if (OB_FAIL(xa_start_local_(xid, flags, timeout_seconds, is_new_branch, tx_desc))) {
        TRANS_LOG(WARN, "xa start local failed", K(ret), K(xid), K(flags), K(*this));
      }
    } else {
      if (!is_tightly_coupled_) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "unexpected xa start for loosely coupled mode", K(ret), K(xid),
            K(flags), K(*this));
      } else if (OB_FAIL(xa_start_remote_second_(xid, flags, timeout_seconds, is_new_branch,
              tx_desc))) {
        TRANS_LOG(WARN, "xa start remote failed", K(ret), K(xid), K(flags), K(*this));
      } else {
        // do nothing
      }
    }
  }

  TRANS_LOG(INFO, "second xa start", K(ret), K(xid), K(flags), K(is_join), K(is_original), K(*this));
  return ret;
}

// the first xa start in non-original scheduler
// @param [in] xid
// @param [in] flags
// @param [in] timeout_seconds
// @param [out] tx_desc
int ObXACtx::xa_start_remote_first(const ObXATransID &xid,
                                   const int64_t flags,
                                   const int64_t timeout_seconds,
                                   ObTxDesc *&tx_desc)
{
  int ret = OB_SUCCESS;
  // const bool is_tightly_coupled = !ObXAFlag::contain_loosely(flags);
  const bool is_join = ObXAFlag::is_tmjoin(flags) || ObXAFlag::is_tmresume(flags);
  const bool is_original = (GCTX.self_addr() == original_sche_addr_);
  const bool is_new_branch = !is_join;

  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  if (OB_UNLIKELY(!xid.is_valid()) || 0 > timeout_seconds) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid), K(timeout_seconds));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa ctx not inited", K(ret), K(xid));
  } else if (is_exiting_) {
    ret = OB_TRANS_IS_EXITING;
    TRANS_LOG(WARN, "xa trans is exiting", K(ret), K(xid), K(flags), K(*this));
  } else if (!xid.gtrid_equal_to(xid_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected xid", K(xid), K(xid_), K(*this));
  } else if (OB_FAIL(xa_start_remote_first_(xid, flags, timeout_seconds, is_new_branch,
          tx_desc))) {
    TRANS_LOG(WARN, "xa start remote failed", K(ret), K(xid), K(flags), K(*this));
  } else {
    // set global trans type to xa trans
    tx_desc->set_global_tx_type(ObGlobalTxType::XA_TRANS);
    TRANS_LOG(WARN, "xa start remote success", K(ret), K(xid), K(flags), K(*tx_desc), K(*this));
  }
  TRANS_LOG(INFO, "xa start remote first", K(ret), K(xid), K(flags), K(*this));
  return ret;
}

// first xa start
// case 1: loosely coupled, xa start noflags
// case 2: tightly coupled, xa start noflags, first branch
int ObXACtx::xa_start_(const ObXATransID &xid,
                       const int64_t flags,
                       const int64_t timeout_seconds,
                       ObTxDesc *tx_desc)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(update_xa_branch_info_(xid,
                                     ObXATransState::ACTIVE,
                                     GCTX.self_addr(),
                                     timeout_seconds,
                                     flags))) {
    TRANS_LOG(WARN, "update branch info failed", K(ret), K(*this));
  } else if (OB_FAIL(save_tx_desc_(tx_desc))) {
    TRANS_LOG(WARN, "save trans desc failed", K(ret), K(*this));
  } else if (OB_FAIL(update_xa_stmt_info_(xid))) {
    TRANS_LOG(WARN, "update xa stmt info failed", K(ret), K(xid), K(*this));
  } else if (OB_FAIL(register_xa_timeout_task_())) {
    TRANS_LOG(WARN, "register xa timeout task failed", K(ret), K(*this));
  } else {
    xa_trans_state_ = ObXATransState::ACTIVE;
    //still maintains following members even in loose mode
    ++xa_branch_count_;
    ++xa_ref_count_;
    //set trans_desc members
    tx_desc->set_xid(xid);
    tx_desc->set_xa_ctx(this);
  }

  notify_xa_start_complete_(ret);

  if (OB_FAIL(ret)) {
    tx_desc->set_xa_ctx(NULL);
    // if fail, the local variable tx_desc_ should be NULL
    tx_desc_ = NULL;
    //xa_ref_count_ is added only when success is returned
  }

  return ret;
}

// xa start in original scheduler
// case 1: loosely coupled, xa start join
// case 2: tightly coupled, xa start join
// case 3: tightly coupled, xa start noflags, not first branch
int ObXACtx::xa_start_local_(const ObXATransID &xid,
                             const int64_t flags,
                             const int64_t timeout_seconds,
                             const bool is_new_branch,
                             ObTxDesc *&tx_desc)
{
  int ret = OB_SUCCESS;

  if (!is_new_branch && OB_FAIL(check_join_(xid))) {
    TRANS_LOG(WARN, "check join failed", K(ret), K(xid), K(*this));
  } else if (OB_FAIL(update_xa_branch_info_(xid,
                                            ObXATransState::ACTIVE,
                                            GCTX.self_addr(),//ATTENTION, check here
                                            timeout_seconds,
                                            flags))) {
    TRANS_LOG(WARN, "update xa branch info failed", K(ret), K(xid), K(*this));
  } else if (OB_FAIL(update_xa_stmt_info_(xid))) {
    TRANS_LOG(WARN, "update xa stmt info failed", K(ret), K(xid), K(*this));
  } else if (OB_FAIL(register_xa_timeout_task_())) {
    TRANS_LOG(WARN, "register xa timeout task failed", K(ret), K(xid), K(*this));
  } else {
    //is it necessary?
    //xa_trans_state_ = ObXATransState::ACTIVE;
    ++xa_ref_count_;
    if (is_new_branch) {
      ++xa_branch_count_;
    }
    tx_desc = tx_desc_;
    // tx_desc.set_xid(xid);
    // tx_desc.set_trans_type(TransType::DIST_TRANS);
  }

  //don't need special error handling, including OB_TRANS_XA_BRANCH_FAIL

  return ret;
}

int ObXACtx::update_xa_stmt_info_(const ObXATransID &xid)
{
  int ret = OB_SUCCESS;
  bool found = false;
  if (!xa_stmt_info_.empty()) {
    for (int64_t i = 0; !found && i < xa_stmt_info_.count(); ++i) {
      ObXAStmtInfo &info = xa_stmt_info_.at(i);
      if (info.xid_.all_equal_to(xid)) {
        found = true;
        break;
      }
    }
  }
  if (!found) {
    ObXAStmtInfo stmt_info(xid);
    if (OB_FAIL(xa_stmt_info_.push_back(stmt_info))) {
      TRANS_LOG(WARN, "add stmt info failed", K(ret), K(stmt_info), K(*this));
    }
  } else {
    TRANS_LOG(WARN, "xa stmt info already exists ", K(ret), K(found), K(xid), K(*this));
  }
  return ret;
}

// xa start in non-original scheduler
// case 1: loosely coupled, xa start join
// case 2: tightly coupled, xa start join
// case 3: tightly coupled, xa start noflags, not first branch
int ObXACtx::xa_start_remote_first_(const ObXATransID &xid,
                                    const int64_t flags,
                                    const int64_t timeout_seconds,
                                    const bool is_new_branch,
                                    ObTxDesc *&tx_desc)
{
  int ret = OB_SUCCESS;

  int tmp_ret = OB_SUCCESS;
  int result = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  const bool is_first_branch = true;  // need tx desc
  ObXAStartRPCRequest xa_start_request;
  obrpc::ObXARPCCB<obrpc::OB_XA_START_REQ> cb;
  ObTransCond cond;
  const int64_t wait_time = (INT64_MAX / 2 ) - now;
  xa_sync_status_cond_.reset();

  if (OB_ISNULL(xa_rpc_) || OB_ISNULL(xa_service_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa_rpc_ is null", K(ret), K(xid), KP(xa_rpc_), KP(xa_service_));
  } else if (OB_FAIL(xa_start_request.init(trans_id_,
                                           xid,
                                           GCTX.self_addr(),
                                           is_new_branch,
                                           is_tightly_coupled_,
                                           timeout_seconds,
                                           flags,
                                           is_first_branch))) {
    TRANS_LOG(WARN, "init sync request failed", K(ret), K(*this));
  } else if (OB_FAIL(cb.init(&cond))) {
    TRANS_LOG(WARN, "init cb failed", K(ret), K(xid), K(*this));
  } else if (OB_FAIL(xa_rpc_->xa_start(tenant_id_,
                                       original_sche_addr_,
                                       xa_start_request,
                                       &cb))) {
    TRANS_LOG(WARN, "post xa start request failed", K(ret), K(xid), K(*this));
  } else {
    if (OB_FAIL(cond.wait(wait_time, result)) || OB_FAIL(result)) {
      if (is_tightly_coupled_ && (OB_TRANS_XA_BRANCH_FAIL == ret || OB_TRANS_CTX_NOT_EXIST == ret)) {
        TRANS_LOG(INFO, "xa trans has terminated", K(ret), K(xid), K(result));
        //rewrite code
        ret = OB_TRANS_XA_BRANCH_FAIL;
      } else {
        TRANS_LOG(WARN, "wait cond failed", K(ret), K(xid), K(result));
      }
    } else if (OB_FAIL(wait_xa_sync_status_(wait_time))) {
      TRANS_LOG(WARN, "wait xa sync status failed", K(ret), K(xid));
    } else if (NULL == tx_desc_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected tx desc", K(ret), K(xid), K(*this));
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_TRANS_XA_BRANCH_FAIL == ret) {
      set_terminated_();
    }
  } else if (OB_FAIL(update_xa_stmt_info_(xid))) {
    TRANS_LOG(WARN, "update xa stmt info failed", K(ret), K(xid), K(*this));
  } else {
    // xa_ref_count_ is added only when success is returned
    xa_trans_state_ = ObXATransState::ACTIVE;
    ++xa_ref_count_;
    tx_desc_->set_xid(xid);
    tx_desc_->set_xa_ctx(this);
    tx_desc = tx_desc_;
  }

  notify_xa_start_complete_(ret);
  return ret;
}

// xa start in non-original scheduler
// case 1: tightly coupled, xa start join
// case 2: tightly coupled, xa start noflags, not first branch
int ObXACtx::xa_start_remote_second_(const ObXATransID &xid,
                                     const int64_t flags,
                                     const int64_t timeout_seconds,
                                     const bool is_new_branch,
                                     ObTxDesc *&tx_desc)
{
  int ret = OB_SUCCESS;

  int tmp_ret = OB_SUCCESS;
  int result = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  const bool is_first_branch = false;  // no need tx desc
  ObXAStartRPCRequest xa_start_request;
  obrpc::ObXARPCCB<obrpc::OB_XA_START_REQ> cb;
  ObTransCond cond;
  const int64_t wait_time = (INT64_MAX / 2 ) - now;
  xa_sync_status_cond_.reset();

  if (OB_ISNULL(xa_rpc_) || OB_ISNULL(xa_service_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa rpc is null", K(ret), K(xid), KP(xa_rpc_), KP(xa_service_));
  } else if (NULL == tx_desc_ || NULL == tx_desc_->get_xa_ctx() || !tx_desc_->is_xa_trans()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected tx desc", K(ret), K(xid), K(*this));
  } else if (!is_tightly_coupled_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected coupled mode", K(ret), K(xid), K(*this));
  } else if (OB_FAIL(xa_start_request.init(trans_id_,
                                           xid,
                                           GCTX.self_addr(),
                                           is_new_branch,
                                           is_tightly_coupled_,
                                           timeout_seconds,
                                           flags,
                                           is_first_branch))) {
    TRANS_LOG(WARN, "init sync request failed", K(ret), K(*this));
  } else if (OB_FAIL(cb.init(&cond))) {
    TRANS_LOG(WARN, "init cb failed", K(ret), K(xid), K(*this));
  } else if (OB_FAIL(xa_rpc_->xa_start(tenant_id_,
                                       original_sche_addr_,
                                       xa_start_request,
                                       &cb))) {
    TRANS_LOG(WARN, "post xa start request failed", K(ret), K(xid), K(*this));
  } else {
    if (OB_FAIL(cond.wait(wait_time, result)) || OB_FAIL(result)) {
      if (OB_TRANS_XA_BRANCH_FAIL == ret || OB_TRANS_CTX_NOT_EXIST == ret) {
        TRANS_LOG(INFO, "xa trans has terminated", K(ret), K(xid), K(result));
        //rewrite code
        ret = OB_TRANS_XA_BRANCH_FAIL;
      } else {
        TRANS_LOG(WARN, "wait cond failed", K(ret), K(xid), K(result));
      }
    } else if (OB_FAIL(wait_xa_sync_status_(wait_time))) {
      TRANS_LOG(WARN, "wait xa sync status failed", K(ret), K(xid));
    } else {
      // do nothing
    }
  }

  if (OB_FAIL(ret)) {
    // OB_TRANS_XA_BRANCH_FAIL,
    if (OB_TRANS_XA_BRANCH_FAIL == ret) {
      set_terminated_();
    }
  } else if (OB_FAIL(update_xa_stmt_info_(xid))) {
    TRANS_LOG(WARN, "update xa stmt info failed", K(ret), K(xid), K(*this));
  } else {
    // xa_ref_count_ is increased only when success is returned
    ++xa_ref_count_;
    // set tx_desc in session
    tx_desc = tx_desc_;
  }
  return ret;
}

int ObXACtx::save_tx_desc_(ObTxDesc *tx_desc)
{
  int ret = OB_SUCCESS;
  tx_desc_ = tx_desc;
  return ret;
}

// xa end in any scheduler
// regardless of the return code, decrease ref count
// if xa end fails, xa trans will fail. Therefore, branch fail is returned.
// @param [in] xid
// @param [in] flags
// @param [in/out] tx_desc
int ObXACtx::xa_end(const ObXATransID &xid,
                    const int64_t flags,
                    ObTxDesc *&tx_desc)
{
  int ret = OB_SUCCESS;
  const bool is_original = (GCTX.self_addr() == original_sche_addr_);

  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  if (OB_UNLIKELY(!xid.is_valid()) || NULL == tx_desc || OB_UNLIKELY(!tx_desc->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid), K(flags));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa ctx not inited", K(ret), K(xid), K(flags));
  } else if (OB_ISNULL(xa_ctx_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa ctx mgr is null", K(ret), K(xid), K(*this));
  } else if (OB_ISNULL(xa_service_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa service is null", K(ret), K(xid), K(*this));
  } else if (OB_ISNULL(tx_desc_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "tx desc is null", K(ret), K(xid), K(*this));
  } else if (tx_desc_ != tx_desc) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected tx desc", K(ret), K(xid), K(*this));
  } else if (OB_FAIL(check_for_execution_(xid, false))) {
    // include branch fail
    TRANS_LOG(WARN, "check for execution failed", K(ret), K(xid), K(*this));
  } else {
    if (is_executing_ && xid.all_equal_to(executing_xid_)) {
      TRANS_LOG(ERROR, "unexpected local lock", K(xid), K(*this));
    }
    if (ObXAFlag::contain_tmfail(flags) && !tx_desc_->need_rollback()) {
      if (OB_FAIL(MTL(ObTransService *)->abort_tx(*tx_desc_, ObTxAbortCause::IMPLICIT_ROLLBACK))) {
        TRANS_LOG(WARN, "abort tx fail", K(ret), K(*this));
      }
    }
    if (OB_SUCC(ret)) {
      if (!is_tightly_coupled_) {
        // loosely coupled mode
        if (is_original) {
          if (OB_FAIL(xa_end_loose_local_(xid, flags, tx_desc))) {
            TRANS_LOG(WARN, "xa end loose local failed", K(ret), K(xid), K(*this));
          }
        } else {
          if (OB_FAIL(xa_end_loose_remote_(xid, flags, tx_desc))) {
            TRANS_LOG(WARN, "xa end loose remote failed", K(ret), K(xid), K(*this));
          }
        }
      } else {
        //tightly coupled mode
        if (is_original) {
          if (OB_FAIL(xa_end_tight_local_(xid, flags, tx_desc))) {
            TRANS_LOG(WARN, "xa end tight local failed", K(ret), K(xid), K(*this));
          }
        } else {
          if (OB_FAIL(xa_end_tight_remote_(xid, flags, tx_desc))) {
            TRANS_LOG(WARN, "xa end tight remote failed", K(ret), K(xid), K(*this));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(remove_xa_stmt_info_(xid))) {
    TRANS_LOG(WARN, "remove xa stmt info failed", K(ret), K(xid), K(*this));
  }

  --xa_ref_count_;
  // if fail, force terminate
  if (OB_FAIL(ret)) {
    if (OB_TRANS_XA_BRANCH_FAIL != ret) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = xa_rollback_terminate_(ObTxAbortCause::IMPLICIT_ROLLBACK))) {
        TRANS_LOG(WARN, "abort tx for session terminate failed", K(tmp_ret), K(*this));
      }
      // return branch fail
      ret = OB_TRANS_XA_BRANCH_FAIL;
    }
  }
  if (OB_SUCC(ret) && is_original) {
    // if succeed in original scheduler, do not exit
  } else {
    try_exit_();
  }

  REC_TRACE_EXT(tlog_, xa_end, OB_Y(ret), OB_ID(bqual), xid_.get_bqual_hash(),
      OB_ID(ctx_ref), get_uref());
  return ret;
}

int ObXACtx::remove_xa_stmt_info_(const ObXATransID &xid)
{
  int ret = OB_SUCCESS;
  bool found = false;
  if (!xa_stmt_info_.empty()) {
    for (int64_t i = 0; !found && i < xa_stmt_info_.count(); ++i) {
      ObXAStmtInfo &info = xa_stmt_info_.at(i);
      if (info.xid_.all_equal_to(xid)) {
        found = true;
        xa_stmt_info_.remove(i);
        break;
      }
    }
  }
  if (!found) {
    ret = OB_ERR_UNEXPECTED;
  }
  TRANS_LOG(INFO, "remove xa stmt info", K(ret), K(found), K(xid), K(*this));
  return ret;
}

// start stmt
// if tightly coupled mode, acquire lock and get tx info from original scheduler
// if loosely coupled mode, do nothing
// @param [in] xid, this is from session
int ObXACtx::start_stmt(const ObXATransID &xid, const uint32_t session_id)
{
  int ret = OB_SUCCESS;
  const bool is_original = (GCTX.self_addr() == original_sche_addr_);
  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);

  if (OB_UNLIKELY(!xid.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa ctx not inited", K(ret), K(*this));
  } else if (OB_ISNULL(xa_service_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa service is null", K(ret), K(*this));
  } else if (OB_ISNULL(tx_desc_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "trans desc is null", K(ret), K(*this));
  } else if (is_exiting_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa ctx is exiting", K(ret), K(*this));
  } else if (is_terminated_) {
    // NOTE that anohter error code maybe required for loosely coupled mode
    ret = OB_TRANS_XA_BRANCH_FAIL;
    TRANS_LOG(INFO, "xa trans has terminated", K(ret), K(xid), K(*this));
  } else if (!need_stmt_lock_) {
    // only for dblink
    if (!is_executing_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected stmt lock", K(xid), K(*this));
    } else if (xid.all_equal_to(executing_xid_)) {
      local_lock_level_++;
      TRANS_LOG(INFO, "acquire local lock repeatedly", K(xid), K_(local_lock_level), K(*this));
      // return OB_SUCCESS
    } else {
      TRANS_LOG(INFO, "no need stmt lock", K(xid), K(*this));
    }
  } else if (!is_tightly_coupled_) {
    // loosely coupled mode
    if (OB_FAIL(create_xa_savepoint_if_need_(xid, session_id))) {
      TRANS_LOG(WARN, "check xa savepoint fail", K(ret), K(xid), K(session_id), K(*this));
    }
  } else {
    // tightly coupled mode
    if (is_executing_) {
      if (xid.all_equal_to(executing_xid_)) {
        local_lock_level_++;
        TRANS_LOG(INFO, "acquire local lock repeatedly", K(xid), K_(local_lock_level), K(*this));
        // return OB_SUCCESS;
      } else {
        ret = OB_TRANS_STMT_NEED_RETRY;
        TRANS_LOG(INFO, "another branch is executing stmt, try again", K(ret), K(*this));
      }
    } else {
      // this flag indicates that a branch is executing normal stmt
      is_executing_ = true;
      start_stmt_cond_.reset();
      xa_sync_status_cond_.reset();
      if (is_original) {
        // local
        if (OB_FAIL(start_stmt_local_(xid))) {
          if (OB_TRANS_XA_BRANCH_FAIL == ret) {
            TRANS_LOG(WARN, "original scheduler has terminated", K(ret), K(*this));
          } else {
            TRANS_LOG(WARN, "xa trans start stmt failed", K(ret), K(*this));
          }
        }
      } else {
        // remote
        if (OB_FAIL(start_stmt_remote_(xid))) {
          if (OB_TRANS_XA_BRANCH_FAIL == ret) {
            TRANS_LOG(WARN, "original scheduler has terminated", K(ret), K(*this));
          } else {
            TRANS_LOG(WARN, "xa trans end stmt failed", K(ret), K(*this));
          }
        }
      }
      if (OB_SUCC(ret)) {
        executing_xid_ = xid;
        local_lock_level_ = 0;
      } else {
        is_executing_ = false;
      }
    }
  }

  TRANS_LOG(INFO, "xa trans start stmt", K(ret), K(xid), K_(trans_id), K_(is_terminated));
  return ret;
}

int ObXACtx::create_xa_savepoint_if_need_(const ObXATransID &xid, const uint32_t session_id)
{
  int ret = OB_SUCCESS;
  bool found = false;
  if (!xa_stmt_info_.empty()) {
    for (int64_t i = 0; !found && i < xa_stmt_info_.count(); ++i) {
      ObXAStmtInfo &info = xa_stmt_info_.at(i);
      if (info.xid_.all_equal_to(xid)) {
        found = true;
        TRANS_LOG(INFO, "find info", K(ret), K(xid), K(info));
        if (info.is_first_stmt_) {
          if (OB_FAIL(MTL(transaction::ObTransService *)->create_explicit_savepoint(*tx_desc_, PL_XA_IMPLICIT_SAVEPOINT, session_id, false))) {
            TRANS_LOG(WARN, "create xa savepoint fail", K(ret), K(xid), K(session_id), K(*this));
          } else {
            TRANS_LOG(INFO, "create pl xa savepoint success", K(ret), K(xid), K(session_id), K(*this));
          }
          info.is_first_stmt_ = false;
        }
        break;
      }
    }
  }
  if (!found) {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

int ObXACtx::start_stmt_local_(const ObXATransID &xid)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_for_execution_(xid, false))) {
    // include branch fail
    TRANS_LOG(WARN, "unexpected scheduler for xa execution", K(ret), K(xid), K(*this));
  } else if (OB_SUCCESS != (ret = stmt_lock_(xid))) {
    TRANS_LOG(INFO, "xa trans try get global lock failed, retry stmt", K(ret), K(xid), K(*this));
  } else {
    // notify SUCCESS
    start_stmt_cond_.notify(ret);
    xa_sync_status_cond_.notify(ret);
    TRANS_LOG(INFO, "succeed to start stmt local", K(ret), K(xid));
  }

  return ret;
}

// start stmt in non-original scheduler
// only for tightly coupled mode
int ObXACtx::start_stmt_remote_(const ObXATransID &xid)
{
  int ret = OB_SUCCESS;
  int result = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  ObXAStartStmtRPCRequest start_stmt_request;
  obrpc::ObXARPCCB<obrpc::OB_XA_START_STMT_REQ> cb;

  if (OB_ISNULL(xa_rpc_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa rpc is null", K(ret), K(xid), K(*this));
  } else if (is_terminated_) {
    ret = OB_TRANS_XA_BRANCH_FAIL;
    TRANS_LOG(INFO, "xa trans has terminated", K(ret), K(xid), K(*this));
  } else {
    if (OB_FAIL(start_stmt_request.init(trans_id_,
                                        xid,
                                        GCTX.self_addr(),
                                        now))) {
      TRANS_LOG(WARN, "fail to init start stmt request", K(ret), K(xid), K(*this));
    } else if (OB_FAIL(cb.init(&start_stmt_cond_))) {
      TRANS_LOG(WARN, "fail to init cb", K(ret), K(xid), K(*this));
    } else if (OB_FAIL(xa_rpc_->xa_start_stmt(tenant_id_,
                                              original_sche_addr_,
                                              start_stmt_request,
                                              &cb))) {
      TRANS_LOG(WARN, "fail to post xa start stmt request", K(ret), K(xid), K(*this));
    } else {
      request_id_ = now;
      TRANS_LOG(INFO, "succeed to post start stmt request", K(xid), K(*this), K(start_stmt_request));
    }
  }

  return ret;
}

// end stmt
// if tightly coupled mode, release lock and sync stmt info to original scheduler
// if loosely coupled mode, do nothing
// @param [in] xid, this is from session
int ObXACtx::end_stmt(const ObXATransID &xid)
{
  int ret = OB_SUCCESS;
  const bool is_original = (GCTX.self_addr() == original_sche_addr_);
  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);

  if (!xid.is_valid() || xid.empty()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argumrnt", K(ret), K(xid));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa ctx not inited", K(ret), K(*this));
  } else if (is_exiting_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa ctx is exiting", K(ret), K(*this));
  } else if (OB_ISNULL(tx_desc_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "trans descriptor is null", K(ret), K(xid), K(*this));
  } else if (!need_stmt_lock_) {
    // only for dblink
    if (!is_executing_ || local_lock_level_ < 0) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected lock lock", K(xid), K(*this));
    } else if (xid.all_equal_to(executing_xid_)) {
      // for case of repeated start stmt, local_lock_level mush be greater than 0
      if (0 == local_lock_level_) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unexpected lock lock", K(xid), K(*this));
      } else {
        local_lock_level_--;
        TRANS_LOG(INFO, "release local lock", K(xid), K_(local_lock_level), K(*this));
      }
    } else {
      TRANS_LOG(INFO, "no need stmt lock", K(xid), K(*this));
    }
  } else if (!is_tightly_coupled_) {
    // do nothing
  } else if (OB_UNLIKELY(!is_executing_)) {
    const ObGlobalTxType global_tx_type = tx_desc_->get_global_tx_type(xid);
    if (ObGlobalTxType::DBLINK_TRANS == global_tx_type) {
      TRANS_LOG(INFO, "xa ctx is not executing", K(ret), K(global_tx_type), K(*this));
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "xa ctx is not executing", K(ret), K(global_tx_type), K(*this));
    }
  } else if (!xid.all_equal_to(executing_xid_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected local lock", K(ret), K(xid), K(*this));
  } else if (is_executing_ && local_lock_level_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected local lock", K(ret), K(xid), K(*this));
  } else if (is_executing_ && local_lock_level_ > 0) {
    local_lock_level_--;
    TRANS_LOG(INFO, "release local lock", K(xid), K_(local_lock_level), K(*this));
  } else {
    if (is_original) {
      // local
      if (OB_FAIL(end_stmt_local_(executing_xid_))) {
        TRANS_LOG(WARN, "xa end stmt local lock failed", K(ret), K(xid), K(*this));
      }
    } else {
      // remote
      if (OB_FAIL(end_stmt_remote_(executing_xid_))) {
        TRANS_LOG(WARN, "xa end stmt remote lock failed", K(ret), K(xid), K(*this));
      }
    }
  }

  TRANS_LOG(INFO, "xa trans end stmt", K(ret), K(xid), K_(trans_id), K_(is_terminated));
  return ret;
}

// end stmt in original scheduler
// only for tightly coupled mode
int ObXACtx::end_stmt_local_(const ObXATransID &xid)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_for_execution_(xid, false))) {
    TRANS_LOG(WARN, "fail to check for execution", K(ret), K(xid), K(*this));
  } else if (OB_FAIL(update_xa_branch_hb_info_(xid))) {
    TRANS_LOG(WARN, "fail to update xa branch hb info", K(ret), K(xid), K(*this));
  } else if (OB_FAIL(stmt_unlock_(xid))) {
    TRANS_LOG(WARN, "fail to unlock", K(ret), K(xid), K(*this));
  } else {
    is_executing_ = false;
    executing_xid_.reset();
    local_lock_level_ = -1;
    TRANS_LOG(INFO, "succeed to end stmt local", K(ret), K(xid));
  }

  return ret;
}

// end stmt in non-original scheduler
// only for tightly coupled mode
int ObXACtx::end_stmt_remote_(const ObXATransID &xid)
{
  int ret = OB_SUCCESS;
  int result= OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  const int64_t wait_time = (INT64_MAX / 2) - now;
  int64_t retry_times = 5;
  const uint64_t tenant_id = tenant_id_;
  const ObAddr origin_sche_addr = original_sche_addr_;

  if (OB_ISNULL(xa_rpc_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa rpc is null", K(ret), K(xid), K(*this));
  } else if (OB_ISNULL(tx_desc_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "trans descriptor is null", K(ret), K(xid), K(*this));
  } else if (!is_executing_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa ctx is executing stmt", K(ret), K(xid), K(*this));
  } else if (is_terminated_) {
    ret = OB_TRANS_XA_BRANCH_FAIL;
    TRANS_LOG(WARN, "xa branch has terminated", K(ret), K(xid), K(*this));
  } else {
    const int64_t seq_no = tx_desc_->get_op_sn();
    SMART_VAR(ObXAEndStmtRPCRequest, req) {
      if (OB_FAIL(req.init(trans_id_,
                           *tx_desc_,
                           xid,
                           seq_no))) {
        TRANS_LOG(WARN, "fail to init end stmt request", K(ret), K(xid), K(*this));
      } else {
        do {
          obrpc::ObXARPCCB<obrpc::OB_XA_END_STMT_REQ> cb;
          ObTransCond cond;
          if (OB_FAIL(cb.init(&cond))) {
            TRANS_LOG(WARN, "fail to init cb", K(ret), K(xid), K(*this));
          } else if (OB_FAIL(xa_rpc_->xa_end_stmt(tenant_id,
                                                  origin_sche_addr,
                                                  req,
                                                  &cb))) {
            TRANS_LOG(WARN, "fail to post end stmt request", K(ret), K(xid), K(req), K(*this));
          } else if (OB_FAIL(cond.wait(wait_time, result))) {
            TRANS_LOG(WARN, "fail to wait cond", K(ret), K(result), K(xid), K(*this));
          }
        } while (OB_SUCC(ret) && (--retry_times > 0 && OB_TIMEOUT == result));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(result)) {
      set_terminated_();
      ret = OB_TRANS_XA_BRANCH_FAIL;
      if (OB_TRANS_XA_BRANCH_FAIL == result || OB_TRANS_CTX_NOT_EXIST == result) {
        TRANS_LOG(INFO, "original scheduler has terminated", K(ret), K(xid), K(*this));
      } else {
        TRANS_LOG(WARN, "fail to end stmt remote", K(ret), K(xid), K(*this));
      }
    } else {
      is_executing_ = false;
      executing_xid_.reset();
      local_lock_level_ = -1;
      TRANS_LOG(INFO, "succeed to end stmt remote", K(xid), K(*this));
    }
  } else {
    TRANS_LOG(WARN, "fail to end stmt remote", K(ret), K(xid), K(*this));
  }

  return ret;
}

int ObXACtx::xa_end_loose_local_(const ObXATransID &xid,
                                 const int64_t flags,
                                 ObTxDesc *&tx_desc)
{
  int ret = OB_SUCCESS;
  const int64_t fake_timeout = 60;
  //const int64_t end_flag = flags | ObXAFlag::LOOSELY;

  if (OB_FAIL(update_xa_branch_info_(xid,
                                     ObXATransState::IDLE,
                                     GCTX.self_addr(),
                                     fake_timeout,
                                     flags))) {
    TRANS_LOG(WARN, "update xa branch info failed", K(ret), K(xid), K(*this));
  } else if (OB_FAIL(register_xa_timeout_task_())) {
    TRANS_LOG(WARN, "register xa timeout task failed", K(ret), K(xid), K(*this));
  }
  /*
  else set xa trans state
  */

  return ret;
}

int ObXACtx::xa_end_loose_remote_(const ObXATransID &xid,
                                  const int64_t flags,
                                  ObTxDesc *&tx_desc)
{
  int ret = OB_SUCCESS;
  int result = OB_SUCCESS;
  obrpc::ObXARPCCB<obrpc::OB_XA_END_REQ> cb;
  ObTransCond cond;
  const int64_t now = ObTimeUtility::current_time();
  // greater than rpc timeout
  const int64_t wait_time = (INT64_MAX / 2 ) - now;
  const int64_t seq_no = tx_desc->get_op_sn();
  //const int64_t end_flag = flags | ObXAFlag::LOOSELY;
  SMART_VAR(ObXAEndRPCRequest, req) {
    if (OB_ISNULL(xa_rpc_)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "xa rpc is null", K(ret), K(xid), K(*this));
    } else if (OB_FAIL(req.init(trans_id_,
                                *tx_desc,
                                xid,
                                is_tightly_coupled_,
                                seq_no,
                                flags))) {
      TRANS_LOG(WARN, "init merge status request failed", K(ret), K(xid), K(*this));
    } else if (OB_FAIL(cb.init(&cond))) {
      TRANS_LOG(WARN, "init cb failed", K(ret), K(xid), K(*this));
    } else if (OB_FAIL(xa_rpc_->xa_end(tenant_id_,
                                       original_sche_addr_,
                                       req,
                                       &cb))) {
      TRANS_LOG(WARN, "post xa merge status failed", K(ret), K(req), K(*this));
    } else if (OB_FAIL(cond.wait(wait_time, result)) || OB_FAIL(result)) {
      TRANS_LOG(WARN, "wait cond failed", K(ret), K(result), K(*this));
    }
    if (OB_SUCC(ret)) {
      ret = result;
    }
  }

  return ret;
}

int ObXACtx::xa_end_tight_local_(const ObXATransID &xid,
                                 const int64_t flags,
                                 ObTxDesc *&tx_desc)
{
  int ret = OB_SUCCESS;
  const int64_t fake_timeout = 60;

  if (OB_FAIL(check_for_execution_(xid, false))) {
    if (OB_TRANS_XA_BRANCH_FAIL == ret) {
      TRANS_LOG(WARN, "xa trans has terminated", K(ret), K(xid), K(*this));
    } else {
      TRANS_LOG(WARN, "check for execution failed", K(ret), K(xid), K(*this));
    }
  } else if (OB_FAIL(update_xa_branch_info_(xid,
                                            ObXATransState::IDLE,
                                            GCTX.self_addr(),
                                            fake_timeout,
                                            flags))) {
    TRANS_LOG(WARN, "update xa branch info failed", K(ret), K(xid), K(*this));
  } else if (OB_FAIL(register_xa_timeout_task_())) {
    TRANS_LOG(WARN, "register xa timeout task failed", K(ret), K(xid), K(*this));
  }

  return ret;
}

int ObXACtx::xa_end_tight_remote_(const ObXATransID &xid,
                                  const int64_t flags,
                                  ObTxDesc *&tx_desc)
{
  int ret = OB_SUCCESS;
  int result = OB_SUCCESS;
  obrpc::ObXARPCCB<obrpc::OB_XA_END_REQ> cb;
  ObTransCond cond;
  const int64_t now = ObTimeUtility::current_time();
  // greater than rpc timeout
  const int64_t wait_time = (INT64_MAX / 2 ) - now;
  const int64_t seq_no = tx_desc->get_op_sn();
  //const int64_t end_flag = flags | ObXAFlag::LOOSELY;
  SMART_VAR(ObXAEndRPCRequest, req) {
    if (OB_ISNULL(xa_rpc_)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "xa rpc is null", K(ret), K(xid), K(*this));
    } else if (OB_FAIL(req.init(trans_id_,
                                *tx_desc,
                                xid,
                                is_tightly_coupled_,
                                seq_no,
                                flags))) {
      TRANS_LOG(WARN, "init merge status request failed", K(ret), K(xid), K(*this));
    } else if (OB_FAIL(cb.init(&cond))) {
      TRANS_LOG(WARN, "init cb failed", K(ret), K(xid), K(*this));
    } else if (OB_FAIL(xa_rpc_->xa_end(tenant_id_,
                                       original_sche_addr_,
                                       req,
                                       &cb))) {
      TRANS_LOG(WARN, "post xa merge status failed", K(ret), K(req), K(*this));
    } else if (OB_FAIL(cond.wait(wait_time, result)) || OB_FAIL(result)) {
      TRANS_LOG(WARN, "wait cond failed", K(ret), K(result), K(*this));
    }
    if (OB_SUCC(ret)) {
      ret = result;
    }
  }

  return ret;
}

// this is ONLY used for session terminate
// subtract ref count
// if ref count is zero, exit
int ObXACtx::clear_branch_for_xa_terminate(const ObXATransID &xid)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);

  if (OB_ISNULL(xa_ctx_mgr_) || OB_ISNULL(xa_service_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected null ptr", K(ret), KP(xa_ctx_mgr_), KP(xa_service_));
  } else {
    --xa_ref_count_;
    if (!is_tightly_coupled_ && 0 != xa_ref_count_) {
      // if loosely coupled mode, ref count must be zero
      tmp_ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected xa ref count", K(tmp_ret), K_(xa_ref_count),
          K(xid), K(*this));
    }
    try_exit_();
  }

  return ret;
}

// if ref count is zero, exit
void ObXACtx::try_exit(const bool need_decrease_ref)
{
  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  if (need_decrease_ref) {
    --xa_ref_count_;
  }
  try_exit_();
}

void ObXACtx::try_exit_()
{
  if (0 == xa_ref_count_) {
    set_exiting_();
  }
}

int ObXACtx::set_exiting_()
{
  int ret = OB_SUCCESS;

  if (is_exiting_) {
    // do nothing
  } else if (OB_ISNULL(xa_ctx_mgr_) || OB_ISNULL(xa_service_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected null ptr", K(ret), KP(xa_ctx_mgr_), KP(xa_service_));
  } else if (0 != xa_ref_count_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected xa ref count", K(ret), K(xa_ref_count_), K_(xid), K(*this));
  } else {
    is_exiting_ = true;
    if (NULL != tx_desc_) {
      tx_desc_->reset_for_xa();
      MTL(ObTransService *)->release_tx(*tx_desc_);
      tx_desc_ = NULL;
    }
    if (OB_FAIL(xa_ctx_mgr_->erase_xa_ctx(trans_id_))) {
      TRANS_LOG(WARN, "erase xa ctx failed", K(ret), K_(xid), K(*this));
    }
  }
  TRANS_LOG(INFO, "xa ctx set exiting", K(ret), K_(xid), K(*this));

  return ret;
}

// if response is unexpected, return false
bool ObXACtx::check_response_(const int64_t response_id,
                              const bool is_start_stmt_response) const
{
  bool ret_bool = true;
  if (is_start_stmt_response) {
    if (!is_executing_) {
      ret_bool = false;
    } else if (response_id != request_id_) {
      ret_bool = false;
    } else {
      ret_bool = true;
    }
  } else {
    if (response_id != request_id_) {
      ret_bool = false;
    } else {
      ret_bool = true;
    }
  }
  return ret_bool;
}

// wait the result of start stmt
// this function is called only when SUCCESS is returned by start_stmt (local/remote)
int ObXACtx::wait_start_stmt(const uint32_t session_id)
{
  int ret = OB_SUCCESS;
  int result = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  const int64_t wait_time = (INT64_MAX / 2) - now;
  const bool is_executing = is_executing_;
  // NOTE that the cond must be notified by rpc
  if (is_tightly_coupled_) {
    if (!is_executing) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected status", K(ret), K(is_executing));
    } else if (OB_FAIL(start_stmt_cond_.wait(wait_time, result)) || OB_FAIL(result)) {
      if (OB_TRANS_STMT_NEED_RETRY == ret) {
        if (REACH_TIME_INTERVAL(1000 * 1000)) {
          TRANS_LOG(INFO, "fail to execute start stmt remote", K(ret));
        }
      } else if (OB_TIMEOUT == ret) {
        TRANS_LOG(WARN, "start stmt remote rpc timeout, need retry", K(ret));
        ret = OB_TRANS_STMT_NEED_RETRY;
      } else if (OB_TRANS_XA_BRANCH_FAIL == ret || OB_TRANS_CTX_NOT_EXIST == ret) {
        ret = OB_TRANS_XA_BRANCH_FAIL;
        set_terminated_();
        TRANS_LOG(INFO, "original scheduler has terminated", K(ret));
      } else {
        TRANS_LOG(WARN, "fail to wait cond", K(ret), K(result));
      }
    } else if (OB_FAIL(wait_xa_sync_status_(wait_time + 500000))) {
      // TRANS_LOG(WARN, "unexpected status", K(ret));
      if (OB_TIMEOUT == ret) {
        TRANS_LOG(WARN, "wait xa stmt info timeout, need retry", K(ret));
        ret = OB_TRANS_STMT_NEED_RETRY;
      } else {
        TRANS_LOG(WARN, "fail to wait xa stmt info", K(ret));
      }
    }

    ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
    if (OB_SUCCESS != ret) {
      TRANS_LOG(WARN, "fail to wait start stmt", K(ret), K(*this));
      is_executing_ = false;
      executing_xid_.reset();
      local_lock_level_ = -1;
    } else if (OB_FAIL(create_xa_savepoint_if_need_(executing_xid_, session_id))) {
      TRANS_LOG(WARN, "check xa savepoint fail", K(ret), K(session_id), K(*this));
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObXACtx::one_phase_end_trans(const ObXATransID &xid,
                                 const bool is_rollback,
                                 const int64_t timeout_us,
                                 const int64_t request_id)
{
  int ret = OB_SUCCESS;

  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa ctx not inited", K(ret), K(*this));
  } else if (is_exiting_) {
    ret = OB_TRANS_IS_EXITING;
    TRANS_LOG(WARN, "xa ctx is exiting", K(ret), K(*this));
  } else if (OB_ISNULL(xa_branch_info_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "branch info array is null", K(ret), KP(xa_branch_info_), K(xid));
  } else if (OB_ISNULL(tx_desc_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "tx desc is null", K(ret), KP(tx_desc_), K(xid));
  } else if (!is_rollback && tx_desc_->need_rollback()) {
    ret = OB_TRANS_NEED_ROLLBACK;
    TRANS_LOG(WARN, "transaction need rollback", K(ret), K(xid), K(*this));
  } else if (OB_FAIL(check_terminated_())) {
    TRANS_LOG(WARN, "check terminated failed", K(ret), K(*this));
  } else if (OB_FAIL(check_trans_state_(is_rollback, request_id, true))) {
    if (!((is_rollback && OB_TRANS_ROLLBACKED == ret)
        || (!is_rollback && OB_TRANS_COMMITED == ret))) {
      TRANS_LOG(WARN, "check trans state failed", K(ret), K(*this));
    }
  } else if (OB_FAIL(is_one_phase_end_trans_allowed_(xid, is_rollback))) {
    TRANS_LOG(WARN, "one phase xa end trans is not allowed", K(ret), K(xid), K(*this));
  } else {
    if (OB_FAIL(one_phase_end_trans_(is_rollback, timeout_us, request_id))) {
      TRANS_LOG(WARN, "one phase xa end trans failed", K(ret), K(*this));
      // in this case, the timeout task has been unregistered.
      // therefore, if fail, xa ctx should exit
      try_exit_();
    }
  }

  if (OB_FAIL(ret) && OB_EAGAIN != ret) {
    if (is_rollback && is_tightly_coupled_) {
      set_terminated_();
    }
  }

  REC_TRACE_EXT(tlog_, xa_one_phase, OB_Y(ret), OB_ID(is_rollback), is_rollback,
      OB_ID(ctx_ref), get_uref());

  return ret;
}

int ObXACtx::one_phase_end_trans_(const bool is_rollback, const int64_t timeout_us, const int64_t request_id)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  (void)unregister_timeout_task_();
  if (OB_ISNULL(xa_service_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa service is null", K(ret), K(*this));
  } else if (OB_FAIL(MTL(ObTransService*)->end_1pc_trans(*tx_desc_, &end_trans_cb_, is_rollback,
          now + timeout_us))) {
    TRANS_LOG(WARN, "end 1pc trans failed", K(ret), K(*this));
  } else {
    request_id_ = request_id;
    is_xa_one_phase_ = true;
    if (is_rollback) {
      xa_trans_state_ = ObXATransState::ROLLBACKING;
    } else {
      xa_trans_state_ = ObXATransState::COMMITTING;
    }
  }

  TRANS_LOG(INFO, "one phase end trans", K(ret), K(is_rollback), K(*this));

  return ret;
}

int ObXACtx::wait_one_phase_end_trans(const bool is_rollback, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  int result = 0;

  if (!is_rollback && (OB_FAIL(end_trans_cb_.wait(timeout_us + 10 * 1000 * 1000, result)) || OB_FAIL(result))) {
    TRANS_LOG(WARN, "wait sub2pc end failed", K(ret), K(is_rollback), K(*this));
  }
  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  if (OB_SUCC(ret) && OB_SUCC(result)) {
    if (is_rollback) {
      xa_trans_state_ = ObXATransState::ROLLBACKED;
    } else {
      xa_trans_state_ = ObXATransState::COMMITTED;
    }
  }
  if (is_rollback && is_tightly_coupled_) {
    set_terminated_();
  }
  if (0 == xa_ref_count_) {
    set_exiting_();
  }
  TRANS_LOG(INFO, "wait one phase end trans", K(ret), K(is_rollback), K(*this));

  return ret;
}

// this is ONLY used for session terminate
// DO NOT decrease ref count
int ObXACtx::xa_rollback_session_terminate(bool &is_first_terminate)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObXACtx not inited", K(ret));
  } else if (is_terminated_) {
    is_first_terminate = false;
    TRANS_LOG(INFO, "transaction is terminating", K(ret), "context", *this);
  } else if (ObXATransState::ACTIVE != xa_trans_state_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected state", K(ret), K(xa_trans_state_), K(*this));
  } else {
    is_first_terminate = true;
    // regardless of coupled mode, set terminate
    // regardless of original scheduler, abort trans
    if (OB_SUCCESS != (tmp_ret = xa_rollback_terminate_(ObTxAbortCause::SESSION_DISCONNECT))) {
      TRANS_LOG(WARN, "abort tx for session terminate failed", K(tmp_ret), K(*this));
    }
  }
  TRANS_LOG(INFO, "rollback xa trans when session terminate", K(ret), K(*this));
  return ret;
}

// process terminate request from temporary scheduler
// DO NOT decrease ref count
// if ref count is zero, exit
int ObXACtx::process_terminate(const ObXATransID &xid)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObXACtx not inited", K(ret));
  } else if (is_terminated_) {
    TRANS_LOG(INFO, "transaction is terminating", K(ret), "context", *this);
  } else {
    // regardless of coupled mode, set terminate
    // regardless of original scheduler, abort trans
    if (OB_SUCCESS != (tmp_ret = xa_rollback_terminate_(ObTxAbortCause::SESSION_DISCONNECT))) {
      TRANS_LOG(WARN, "abort tx for session terminate failed", K(tmp_ret), K(*this));
    }
  }
  try_exit_();
  TRANS_LOG(INFO, "process terminate in original scheduler", K(ret), K(xid));
  return ret;
}

int ObXACtx::try_heartbeat()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  const int64_t now = ObTimeUtility::current_time();
  if (is_exiting_ || is_terminated_) {
    // do nothing
  } else if (original_sche_addr_ != GCTX.self_addr()) {
    // temproray scheduler, do nothing
  } else if (OB_ISNULL(xa_branch_info_)
      && (ObXATransState::IDLE < xa_trans_state_ || ObXATransState::UNKNOWN == xa_trans_state_)) {
    // do nothing
  } else if (OB_ISNULL(xa_branch_info_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected branch count", K(ret), K(*this));
  } else if (!is_tightly_coupled_) {
    // loosely coupled
    // original scheduler posts heartbeat to tmp scheduler
    // if heartbeat fails, original scheduler exits only
    const int64_t LOOSELY_COUPLED_BRANCH_COUNT = 1;
    if (LOOSELY_COUPLED_BRANCH_COUNT != xa_branch_info_->count()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected branch count", K(ret), K(*this));
    } else if (1 < xa_ref_count_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected reference count", K(ret), K(*this));
    } else {
      ObXABranchInfo &branch_info = xa_branch_info_->at(0);
      if (branch_info.addr_ == GCTX.self_addr()) {
        // do nothing
      } else if (ObXATransState::ACTIVE != branch_info.state_) {
        // do nothing
      } else if (branch_info.unrespond_msg_cnt_ > MAX_UNRESPOND_XA_HB_CNT) {
        // exit only
        // no need to delete xa records
        // if xa records exist, we can rely on garbage collection
        if (OB_SUCCESS != (tmp_ret = xa_rollback_terminate_(ObTxAbortCause::IMPLICIT_ROLLBACK))) {
          TRANS_LOG(WARN, "fail to stop xa trans", K(tmp_ret), K(*this));
        }
        (void)set_exiting_();
      } else if (now > branch_info.last_hb_ts_ + XA_HB_THRESHOLD) {
        branch_info.unrespond_msg_cnt_++;
        ObXAHbRequest req;
        if (OB_FAIL(req.init(trans_id_, branch_info.xid_, GCTX.self_addr()))) {
          TRANS_LOG(WARN, "xa hb request init failed", KR(ret), K(branch_info), K(*this));
        } else if (OB_FAIL(xa_rpc_->xa_hb_req(tenant_id_, branch_info.addr_, req, NULL))) {
          TRANS_LOG(WARN, "fail to post heartbeat for xa trans branch", KR(ret),
              K(branch_info), K(*this));
        } else {
          // do nothing
        }
        TRANS_LOG(INFO, "post heartbeat for xa trans branch", K(ret), K(branch_info), K(*this));
      } else {
        // do nothing
      }
    }
  } else {
    // tightly coupled
    // original scheduler posts heartbeat to all tmp schedulers
    // if heartbeat fails, original scheduler not only exits but also executes rollback
    int64_t now = ObTimeUtil::current_time();
    for (int64_t i = 0; i < xa_branch_info_->count(); ++i) {
      ObXABranchInfo &branch_info = xa_branch_info_->at(i);
      if (branch_info.addr_ == GCTX.self_addr()) {
        //do nothing
      } else if (ObXATransState::ACTIVE != branch_info.state_) {
        //do nothing
      } else if (branch_info.unrespond_msg_cnt_ > MAX_UNRESPOND_XA_HB_CNT) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = xa_rollback_terminate_(ObTxAbortCause::IMPLICIT_ROLLBACK))) {
          TRANS_LOG(WARN, "fail to stop xa trans", K(tmp_ret), K(*this));
        }
        // if xa_ref_count is zero, xa ctx should exit
        try_exit_();
        TRANS_LOG(INFO, "scheduler unrespond, rollbacked", K(ret), K(branch_info), K(*this));
        break;
      } else if (now > branch_info.last_hb_ts_ + XA_HB_THRESHOLD) {
        branch_info.unrespond_msg_cnt_++;
        ObXAHbRequest req;
        if (OB_FAIL(req.init(trans_id_, branch_info.xid_, GCTX.self_addr()))) {
          TRANS_LOG(WARN, "xa hb request init failed", KR(ret), K(branch_info), K(*this));
        } else if (OB_FAIL(xa_rpc_->xa_hb_req(tenant_id_, branch_info.addr_, req, NULL))) {
          TRANS_LOG(WARN, "post xa  hb req failed", KR(ret), K(branch_info), K(*this));
        } else {
          // do nothing
        }
        TRANS_LOG(INFO, "post heartbeat for xa trans branch", K(ret), K(branch_info), K(*this));
      }
    }
  }
  return ret;
}

int ObXACtx::response_for_heartbeat(const ObXATransID &xid, const ObAddr &original_addr)
{
  int ret = OB_SUCCESS;
  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  if (!xid.is_valid() || !original_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid), K(original_addr));
  } else if (original_addr != original_sche_addr_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected original scheduler address", K(ret), K(xid),
        K(original_addr), K(*this));
  } else {
    ObXAHbResponse resp;
    if (OB_FAIL(resp.init(trans_id_, xid, GCTX.self_addr()))) {
      TRANS_LOG(WARN, "fail to init xa hb response", KR(ret), K(xid), K(original_addr), K(*this));
    } else if (OB_FAIL(xa_rpc_->xa_hb_resp(tenant_id_, original_addr, resp, NULL))) {
      TRANS_LOG(WARN, "fail to post heartbeat response for xa trans branch", KR(ret),
          K(resp), K(*this));
    } else {
      // do nothing
    }
    TRANS_LOG(INFO, "post heartbeat response for xa trans branch", K(ret), K(xid), K(*this));
  }
  return ret;
}

int ObXACtx::update_xa_branch_for_heartbeat(const ObXATransID &xid)
{
  int ret = OB_SUCCESS;
  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  if (!xid.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid));
  } else if (GCTX.self_addr() != original_sche_addr_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected request", K(ret), K(xid), K(*this));
  } else {
    ret = update_xa_branch_hb_info_(xid);
  }
  TRANS_LOG(INFO, "update xa branch info for heartbeat", K(ret), K(xid), K(*this));
  return ret;
}

int ObXACtx::check_for_execution_(const ObXATransID &xid, const bool is_new_branch)
{
  int ret = OB_SUCCESS;

  if (is_terminated_) {
    ret = OB_TRANS_XA_BRANCH_FAIL;
    TRANS_LOG(INFO, "xa trans is terminating", K(ret), K(xid));
  } else if (is_exiting_) {
    if (is_tightly_coupled_) {
      ret = OB_TRANS_XA_BRANCH_FAIL;
    } else {
      ret = OB_TRANS_IS_EXITING;
    }
  } else if (ObXATransState::has_submitted(xa_trans_state_)) {
    ret = OB_TRANS_XA_PROTO;
    TRANS_LOG(WARN, "xa trans has entered into commit phase", K(ret), K(*this));
  } else if (is_tightly_coupled_) {
    if (is_new_branch) {
      if (ObXATransState::PREPARING == xa_trans_state_) {
        ret = OB_TRANS_XA_PROTO;
        TRANS_LOG(WARN, "xa trans has entered into commit phase", K(ret), K(*this));
      }
    } else {
      //join, xa end, lock...
      if (ObXATransState::PREPARING == xa_trans_state_) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = xa_rollback_terminate_(ObTxAbortCause::IMPLICIT_ROLLBACK))) {
          TRANS_LOG(WARN, "rollback terminate failed", K(tmp_ret), K(xid), K(*this));
        }
        ret = OB_TRANS_XA_BRANCH_FAIL;
      }
    }
  } else {
    //loose couple mode, do nothing
  }

  return ret;
}

// if the last branch fails, xa ctx should exit
// OB_ERR_READ_ONLY_TRANSACTION, read only transaction
// OB_TRANS_XA_RDONLY, read only branch
int ObXACtx::xa_prepare(const ObXATransID &xid, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  bool need_exit = false;

  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  if (OB_UNLIKELY(!xid.is_valid()) || 0 > timeout_us) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid), K(timeout_us));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa ctx not inited", K(ret), K(xid), K(*this));
  } else if (OB_UNLIKELY(!xid_.gtrid_equal_to(xid))) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected xid", K(xid), K(xid_), K(*this));
  } else if (OB_UNLIKELY(GCTX.self_addr() != original_sche_addr_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "not original scheduler", K(ret), K(xid), K(*this));
  } else if (OB_ISNULL(xa_branch_info_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "branch info array is null", K(ret), K(xid), K(*this));
  } else if (OB_ISNULL(xa_service_) || OB_ISNULL(tx_desc_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected null ptr", K(ret), KP_(xa_service), KP_(tx_desc), K(*this));
  } else if (tx_desc_->need_rollback()) {
    ret = OB_TRANS_NEED_ROLLBACK;
    TRANS_LOG(WARN, "transaction need rollback", K(ret), K(xid), K(*this));
  } else if (OB_FAIL(check_terminated_())) {
    TRANS_LOG(WARN, "fail to check terminate", K(ret), K(xid), K(*this));
  } else if (OB_FAIL(xa_prepare_(xid, timeout_us, need_exit))) {
    TRANS_LOG(WARN, "xa prepare failed", K(ret), K(xid), K(need_exit), K(*this));
    if (need_exit) {
      if (OB_ERR_READ_ONLY_TRANSACTION != ret) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = MTL(ObTransService*)->abort_tx(*tx_desc_,
                ObTxAbortCause::IMPLICIT_ROLLBACK))) {
          TRANS_LOG(WARN, "fail to stop transaction", K(tmp_ret), K(*this));
        }
      }
      set_terminated_();
      set_exiting_();
    }
  }

  REC_TRACE_EXT(tlog_, xa_prepare, OB_Y(ret), OB_ID(bqual), xid_.get_bqual_hash(),
      OB_ID(ctx_ref), get_uref());

  TRANS_LOG(INFO, "xa prepare", K(ret), K(xid), K(timeout_us), K(*this));

  return ret;
}

int ObXACtx::xa_prepare_(const ObXATransID &xid, const int64_t timeout_us, bool &need_exit)
{
  int ret = OB_SUCCESS;
  const int64_t count = xa_branch_info_->count();
  int64_t unprepared_count = 0;
  int64_t target_idx = -1;

  for (int64_t i = 0; i < count; ++i) {
    const ObXABranchInfo &info = xa_branch_info_->at(i);
    if (ObXATransState::PREPARED != info.state_ && ObXATransState::PREPARING != info.state_) {
      ++unprepared_count;
    }
    if (info.xid_.all_equal_to(xid)) {
      target_idx = i;
    }
  }
  if (OB_UNLIKELY(target_idx < 0) || OB_UNLIKELY(target_idx >= count)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "branch info not found", K(ret), K(xid), K(*this));
    print_branch_info_();
  } else {
    ObXABranchInfo &target_info = xa_branch_info_->at(target_idx);
    if (unprepared_count > 0) {
      if (ObXATransState::IDLE == target_info.state_) {
        if (OB_UNLIKELY(!ObXAFlag::is_valid_inner_flag(target_info.end_flag_))) {
          ret = OB_TRANS_XA_RMFAIL;
          TRANS_LOG(WARN, "unexpected xa trans end_flag", K(ret), K(xid), K(target_info));
        } else if (unprepared_count > 1) {
          target_info.state_ = ObXATransState::PREPARED;
          xa_trans_state_ = ObXATransState::PREPARING;
          register_xa_timeout_task_();
          ret = OB_TRANS_XA_RDONLY;
        } else {
          // the last branch which do xa prepare
          int64_t affected_rows = 0;
          target_info.state_ = ObXATransState::PREPARING;
          xa_trans_state_ = ObXATransState::PREPARING;
          // update xid in local including tx desc
          xid_ = xid;
          tx_desc_->set_xid(xid);
          share::ObLSID coord;
          (void)unregister_timeout_task_();
          uint64_t data_version = 0;
          if (OB_FAIL(MTL(ObTransService*)->prepare_tx_coord(*tx_desc_, coord))) {
            if (OB_ERR_READ_ONLY_TRANSACTION == ret) {
              TRANS_LOG(INFO, "xa is read only", K(ret), K(*this));
            } else {
              TRANS_LOG(WARN, "fail to prepare tx coord", K(ret), K(*this));
            }
          } else if (!coord.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(WARN, "invalid coordinator", K(ret), K(xid), K(coord), K(*this));
          } else if (OB_FAIL(MTL(ObXAService*)->update_coord(tenant_id_, xid, coord, has_tx_level_temp_table_,
                  affected_rows))) {
            TRANS_LOG(WARN, "fail to update xa trans record", K(ret), K(xid), K(coord),
                K(affected_rows), K(*this));
          } else if (0 == affected_rows) {
            ret = OB_TRANS_XA_PROTO;
            TRANS_LOG(WARN, "no row is affected", K(ret), K(xid), K(coord),
                K(affected_rows), K(*this));
          } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
            TRANS_LOG(WARN, "fail to get min data version", KR(ret), K(tenant_id_));
          } else if (data_version < DATA_VERSION_4_2_0_0 && OB_FAIL(MTL(ObXAService*)->
            insert_xa_pending_record(tenant_id_, xid, trans_id_, coord, original_sche_addr_))) {
            TRANS_LOG(WARN, "fail to insert xa trans record", K(ret), K(xid), K(coord), K(*this));
          } else if (OB_FAIL(drive_prepare_(xid, timeout_us))) {
            TRANS_LOG(WARN, "drive prepare failed", K(ret), K(*this));
          }
          // if the last branch fails, need exit
          if (OB_FAIL(ret)) {
            need_exit = true;
          }
        }
      } else if (ObXATransState::PREPARED == target_info.state_) {
        ret = OB_TRANS_XA_RDONLY;
      } else if (ObXATransState::ACTIVE == target_info.state_) {
        ret = OB_TRANS_XA_PROTO;
        TRANS_LOG(WARN, "unexpected xa branch state", K(ret), K(xid), K(target_info), K(*this));
        print_branch_info_();
      } else {
        // including ObXATransState::PREPARING
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "unexpected error", K(ret), K(target_info), K(*this));
        print_branch_info_();
      }
    } else {
      if (xid_.all_equal_to(xid)) {
        if (ObXATransState::PREPARING == target_info.state_) {
          ret = OB_TRANS_XA_RETRY;
        } else if (ObXATransState::PREPARED == target_info.state_) {
          ret = OB_SUCCESS;
        }
      } else {
        ret = OB_TRANS_XA_RDONLY;
      }
    }
  }

  return ret;
}

// if fail, need exit
int ObXACtx::drive_prepare_(const ObXATransID &xid, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  share::ObLSID coordinator;
  // first update coordinator into inner table
  if (OB_FAIL(MTL(ObTransService*)->prepare_tx(*tx_desc_, timeout_us, end_trans_cb_))) {
    TRANS_LOG(WARN, "fail to prepare tx", K(ret), K(xid), K(*this));
  }

  TRANS_LOG(INFO, "drive xa prepare", K(ret), K(*this));
  return ret;
}

int ObXACtx::wait_xa_prepare(const ObXATransID &xid, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  int result = OB_SUCCESS;

  if (OB_FAIL(end_trans_cb_.wait(timeout_us + 10000000, result)) || OB_FAIL(result)) {
    // OB_ERR_READ_ONLY_TRANSACTION, submited to scheduler, and found it is read only
    if (OB_ERR_READ_ONLY_TRANSACTION != ret) {
      TRANS_LOG(WARN, "wait trans prepare failed", K(ret), K(xid), K(*this));
    }
  }
  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  if (OB_SUCC(ret) || OB_ERR_READ_ONLY_TRANSACTION == ret) {
    xa_trans_state_ = ObXATransState::PREPARED;
  }

  if (OB_LIKELY(!is_exiting_)) {
    is_exiting_ = true;
    if (OB_NOT_NULL(xa_ctx_mgr_)) {
      xa_ctx_mgr_->erase_xa_ctx(trans_id_);
    }
    // release tx desc
    MTL(ObTransService*)->release_tx(*tx_desc_);
    tx_desc_ = NULL;
  }

  TRANS_LOG(INFO, "wait xa prepare", K(ret), K(*this));
  return ret;
}

// two phase end trans
// this interface is ONLY for temporary ctx
int ObXACtx::two_phase_end_trans(const ObXATransID &xid,
                                 const share::ObLSID &coord,
                                 const bool is_rollback,
                                 const int64_t timeout_us,
                                 const int64_t request_id)
{
  int ret = OB_SUCCESS;

  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  if (!xid.is_valid() || !coord.is_valid() || 0 > timeout_us) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", K(ret), K(xid), K(is_rollback), K(timeout_us));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa ctx not inited", K(ret), K(*this));
  } else if (OB_FAIL(check_trans_state_(is_rollback, request_id, false))) {
    if (!((is_rollback && OB_TRANS_ROLLBACKED == ret)
        || (!is_rollback && OB_TRANS_COMMITED == ret))) {
      TRANS_LOG(WARN, "check trans state fail", K(ret), K(xid), K(is_rollback), K(timeout_us));
    }
  } else {
    ObTxDesc *tx = NULL;
    if (OB_FAIL(MTL(ObTransService*)->end_two_phase_tx(trans_id_, xid, coord, timeout_us,
            is_rollback, end_trans_cb_, tx))) {
      TRANS_LOG(WARN, "fail to end trans for two phase commit", K(ret), K(xid), K(coord),
          K(is_rollback), K(timeout_us), K(*this));
    } else if (OB_ISNULL(tx)) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid trans descriptor", K(ret), K(xid));
    } else {
      tx_desc_ = tx;
      request_id_ = request_id;
      if (is_rollback) {
        xa_trans_state_ = ObXATransState::ROLLBACKING;
      } else {
        xa_trans_state_ = ObXATransState::COMMITTING;
      }
    }
  }

  if (OB_FAIL(ret)) {
    set_exiting_();
  }

  REC_TRACE_EXT(tlog_, xa_end_trans, OB_Y(ret), OB_ID(is_rollback), is_rollback,
      OB_ID(ctx_ref), get_uref());
  return ret;
}

int ObXACtx::wait_two_phase_end_trans(const ObXATransID &xid,
                                      const bool is_rollback,
                                      const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  int result = OB_SUCCESS;

  if (OB_FAIL(end_trans_cb_.wait(timeout_us + 10000000, result)) || OB_FAIL(result)) {
    TRANS_LOG(WARN, "wait two phase end trans failed", K(ret), K(xid), K(*this));
  }
  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  if (OB_SUCC(ret) && OB_SUCC(result)) {
    if (is_rollback) {
      xa_trans_state_ = ObXATransState::ROLLBACKED;
    } else {
      xa_trans_state_ = ObXATransState::COMMITTED;
    }
  }

  if (OB_LIKELY(!is_exiting_)) {
    set_exiting_();
  }

  return ret;
}

/*
check_trans_state_ is called when executing one-phase and two-phase end trans
XA ROLLBACK:
1. If xa_trans_state = PREPARING, it means that a branch has done xa prepare,
   so it is necessary to judge whether the branch is the last branch to be processed separately.
2. if xa_trans_state = COMMITTING/COMMITTED,
   it means that the concurrency of xa commit and xa rollback has occurred
   and it cannot be rolled back and an error is reported.
3. If xa_trans_state = ACTIVE, it means that a branch has done xa start,
   and a one phase rollback needs to be performed at this time. 
   If it is a two phase rollback, it is an unexpected scenario.
4. If xa_trans_state = PREPARED, it means that all branches have completed xa prepare.
   At this time, a two phase rollback needs to be performed.
   if it is a one phase rollback, there may be concurrent xa prepare and xa rollback,
   return to the upper layer and try again.
5. If xa_trans_state = ROLLBACKED, it means that the transaction has been rolled back
   and returns success to the user.
6. If xa_trans_state = ROLLBACKING, it means that the transaction is being rolled back.
   if it is retried for its own message, it will return to retry;
   if it is a one phase rollback before, it will succeed, and two phase will fail.
7. If xa_trans_state = IDLE/NON_EXISTING, the IDLE state is not maintained in the existing implementation,
   and it is an unexpected scenario when it occurs.
8. If xa_trans_state = UNKNOWN, it means that the transaction has completed xa prepare,
   so a two phase rollback is required.
   If it is a one phase rollback, it is an unexpected scenario.

XA COMMIT:
1. If xa_trans_state = PREPARING, it means that a branch has done xa prepare,
   At this time, if it is a one phase commit, then judge whether the branch
   is the last branch to be processed separately.
   If it is a two phase commit, it is an unexpected scenario.
2. If xa_trans_state = COMMITTING, it means that the transaction is being committed.
   If it is retried for its own message, it will return to retry; Otherwise report an error.
3. If xa_trans_state = COMMITTED, it means the transaction has been committed.
   If it is retried for its own message, it will return success; Otherwise report an error.
4. If xa_trans_state = PREPARED, represents a concurrent scenario of xa commit and xa prepare,
   which does not meet expectations
5. If xa_trans_state = ROLLBACKED, it means that the transaction has been rolled back
   and reports error to the user.
6. If xa_trans_state = ROLLBACKING, it means that the transaction is being rolled back
   and reports error to the user.
6. If xa_trans_state = ACTIVE, it means that a branch has done xa start,
   and a one phase commit needs to be performed at this time.
   If it is a two phase commit, it is an unexpected scenario.
7. If xa_trans_state = IDLE/NON_EXISTING, the IDLE state is not maintained in the existing implementation,
   and it is an unexpected scenario when it occurs.
8. If xa_trans_state = UNKNOWN, it means that the transaction has completed xa prepare,
   so a two phase commit is required.
   If it is a one phase commit, it is an unexpected scenario. 
*/

int ObXACtx::check_trans_state_(const bool is_rollback,
                                const int64_t request_id,
                                const bool is_xa_one_phase)
{
  int ret = OB_SUCCESS;

  if (is_rollback) {
    switch (xa_trans_state_) {
      case ObXATransState::PREPARING: {
        ret = OB_SUCCESS;
        break;
      }
      case ObXATransState::COMMITTING:
      case ObXATransState::COMMITTED: {
        ret = OB_TRANS_XA_PROTO;
        break;
      }
      case ObXATransState::ACTIVE: {
        if (!is_xa_one_phase) {
          ret = OB_TRANS_XA_RMFAIL;
        } else {
          ret = OB_SUCCESS;
        }
        break;
      }
      case ObXATransState::PREPARED: {
        if (is_xa_one_phase) {
          ret = OB_EAGAIN;
        } else {
          ret = OB_SUCCESS;
        }
        break;
      }
      case ObXATransState::ROLLBACKED: {
        ret = OB_TRANS_ROLLBACKED;
        break;
      }
      case ObXATransState::ROLLBACKING: {
        if (request_id_ == request_id) {
          ret = OB_EAGAIN;
        } else {
          if (is_xa_one_phase_) {
            ret = OB_TRANS_ROLLBACKED;
          } else {
            ret = OB_TRANS_XA_PROTO;
          }
        }
        break;
      }
      case ObXATransState::NON_EXISTING:
      case ObXATransState::IDLE: {
        ret = OB_TRANS_XA_RMFAIL;
        break;
      }
      case ObXATransState::UNKNOWN: {
        if (!is_xa_one_phase) {
          ret = OB_SUCCESS;
        } else {
          ret = OB_TRANS_XA_RMFAIL;
        }
        break;
      }
      default: {
        ret = OB_TRANS_XA_RMFAIL;
        break;
      }
    }
  } else {
    switch (xa_trans_state_) {
      case ObXATransState::PREPARING:{
        if (!is_xa_one_phase) {
          ret = OB_TRANS_XA_RMFAIL;
        } else {
          ret = OB_SUCCESS;
        }
        break;
      }
      case ObXATransState::COMMITTING: {
        if (request_id_ == request_id) {
          ret = OB_EAGAIN;
        } else {
          ret = OB_TRANS_XA_PROTO;
        }
        break;
      }
      case ObXATransState::COMMITTED: {
        if (request_id_ == request_id) {
          ret = OB_TRANS_COMMITED;;
        } else {
          ret = OB_TRANS_XA_PROTO;
        }
        break;
      }
      case ObXATransState::PREPARED: {
        ret = OB_TRANS_XA_RMFAIL;
        break;
      }
      case ObXATransState::ROLLBACKED: {
        ret = OB_TRANS_XA_PROTO;
        break;
      }
      case ObXATransState::ROLLBACKING: {
        ret = OB_TRANS_XA_PROTO;
        break;
      }
      case ObXATransState::ACTIVE: {
        if (!is_xa_one_phase) {
          ret = OB_TRANS_XA_RMFAIL;
        } else {
          ret = OB_SUCCESS;
        }
        break;
      }
      case ObXATransState::NON_EXISTING:
      case ObXATransState::IDLE: {
        ret = OB_TRANS_XA_RMFAIL;
        break;
      }
      case ObXATransState::UNKNOWN: {
        if (!is_xa_one_phase) {
          ret = OB_SUCCESS;
        } else {
          ret = OB_TRANS_XA_RMFAIL;
        }
        break;
      }
      default: {
        ret = OB_TRANS_XA_RMFAIL;
        break;
      }
    }
  }

  return ret;
}

int ObXACtx::stop_check_stmt_lock(const ObXATransID &xid)
{
  int ret = OB_SUCCESS;
  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  if (!is_executing_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(INFO, "unexpected stmt lock", K(ret), K(xid), K(*this));
  } else if (!xid.all_equal_to(executing_xid_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(INFO, "unexpected stmt lock", K(ret), K(xid), K(*this));
  } else if (!need_stmt_lock_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(INFO, "unexpected stmt lock", K(ret), K(xid), K(*this));
  } else {
    need_stmt_lock_ = false;
  }
  TRANS_LOG(INFO, "stop check stmt lock", K(ret), K(xid), K_(trans_id), K_(is_terminated),
      K_(local_lock_level));
  return ret;
}

int ObXACtx::start_check_stmt_lock(const ObXATransID &xid)
{
  int ret = OB_SUCCESS;
  ObLatchWGuard guard(lock_, common::ObLatchIds::XA_CTX_LOCK);
  if (!is_executing_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(INFO, "unexpected stmt lock", K(ret), K(xid), K(*this));
  } else if (!xid.all_equal_to(executing_xid_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(INFO, "unexpected stmt lock", K(ret), K(xid), K(*this));
  } else if (need_stmt_lock_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(INFO, "unexpected stmt lock", K(ret), K(xid), K(*this));
  } else {
    need_stmt_lock_ = true;
  }
  TRANS_LOG(INFO, "start check stmt lock", K(ret), K(xid), K_(trans_id), K_(is_terminated),
      K_(local_lock_level));
  return ret;
}

}//transaction

}//oceanbase
