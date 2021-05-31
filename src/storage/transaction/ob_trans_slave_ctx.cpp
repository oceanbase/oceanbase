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

#include "storage/ob_partition_service.h"
#include "lib/profile/ob_perf_event.h"
#include "ob_trans_slave_ctx.h"
#include "ob_trans_ctx_mgr.h"
#include "ob_ts_mgr.h"
#include "storage/memtable/ob_memtable.h"
#include "ob_weak_read_util.h"

namespace oceanbase {

using namespace common;
using namespace memtable;
using namespace storage;
using namespace sql;

namespace transaction {
int ObSlaveTransCtx::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t trans_expired_time,
    const ObPartitionKey& self, ObITransCtxMgr* ctx_mgr, const ObStartTransParam& trans_param,
    const uint64_t cluster_version, ObTransService* trans_service)
{
  int ret = OB_SUCCESS;

  CtxLockGuard guard(lock_);

  if (is_inited_) {
    TRANS_LOG(WARN, "ObSlaveTransCtx inited twice");
    ret = OB_INIT_TWICE;
  } else if (!is_valid_tenant_id(tenant_id) || !trans_id.is_valid() || !self.is_valid() || trans_expired_time <= 0 ||
             OB_ISNULL(ctx_mgr) || !trans_param.is_valid() || cluster_version <= 0 || OB_ISNULL(trans_service)) {
    TRANS_LOG(WARN,
        "invalid argument",
        K(tenant_id),
        K(trans_id),
        K(self),
        "ctx_mgr",
        OB_P(ctx_mgr),
        K(trans_param),
        K(cluster_version),
        KP(trans_service));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (mt_ctx_factory_ = trans_service->get_memtable_ctx_factory()) ||
             NULL == (partition_service_ = trans_service->get_partition_service())) {
    TRANS_LOG(ERROR, "ObTransService is invalid, unexpected error", KP(mt_ctx_factory_), KP_(partition_service));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ObDistTransCtx::init(tenant_id,
                 trans_id,
                 trans_expired_time,
                 self,
                 ctx_mgr,
                 trans_param,
                 cluster_version,
                 trans_service))) {
    TRANS_LOG(WARN, "ObDistTransCtx inited error", KR(ret), K(self), K(trans_id));
  } else if (OB_FAIL(init_memtable_ctx_(tenant_id))) {
    TRANS_LOG(WARN, "ObSlaveTransCtx init memtable context error", KR(ret), K(self), K(trans_id));
  } else {
    // do nothing
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
    if (is_readonly_) {
      mt_ctx_.set_read_only();
    }
    if (for_replay_) {
      mt_ctx_.trans_replay_begin();
    } else {
      mt_ctx_.trans_begin();
    }
    mt_ctx_.set_trans_ctx(this);
    mt_ctx_.set_for_replay(for_replay_);
  }
  if (OB_FAIL(ret)) {
    set_exiting_();
  }

  return ret;
}

bool ObSlaveTransCtx::is_inited() const
{
  return ATOMIC_LOAD(&is_inited_);
}

int ObSlaveTransCtx::init_memtable_ctx_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  memtable::MemtableIDMap& id_map = static_cast<ObMemtableCtxFactory*>(mt_ctx_factory_)->get_id_map();
  common::ObIAllocator& malloc_allocator = static_cast<ObMemtableCtxFactory*>(mt_ctx_factory_)->get_malloc_allocator();
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mt_ctx_.init(tenant_id, id_map, malloc_allocator))) {
    TRANS_LOG(WARN, "memtable context init error", KR(ret));
  } else {
    uint32_t ctx_descriptor = IDMAP_INVALID_ID;
    if (OB_FAIL(id_map.assign(&mt_ctx_, ctx_descriptor))) {
      TRANS_LOG(WARN, "id map assign fail", KR(ret));
    } else {
      mt_ctx_.set_ctx_descriptor(ctx_descriptor);
    }
  }

  return ret;
}

void ObSlaveTransCtx::destroy()
{
  int ret = OB_SUCCESS;

  int64_t memstore_count = 0;
  if (is_inited_) {
    end_trans_callback_(OB_TRANS_UNKNOWN);
    if (NULL == mt_ctx_factory_) {
      TRANS_LOG(ERROR, "mt ctx is null, unexpected error", KP_(mt_ctx_factory), "context", *this);
    } else {
      if (OB_FAIL(mt_ctx_.check_memstore_count(memstore_count))) {
        TRANS_LOG(ERROR, "check memstore count error", "context", *this);
      } else if (memstore_count > 0) {
        if (NULL != trans_service_ && trans_service_->is_running()) {
          TRANS_LOG(
              ERROR, "active memstore count is not equal to 0, unexpected error", K(memstore_count), "context", *this);
          need_print_trace_log_ = true;
          (void)mt_ctx_.trans_clear();
        }
      } else {
        // do nothing
      }
      memtable::MemtableIDMap& id_map = static_cast<ObMemtableCtxFactory*>(mt_ctx_factory_)->get_id_map();
      id_map.erase(mt_ctx_.get_ctx_descriptor());
    }
    REC_TRANS_TRACE_EXT(tlog_, destroy);
    ObDistTransCtx::destroy();
    is_inited_ = false;
  }
}

void ObSlaveTransCtx::reset()
{
  // destroy();
  // partctx, must be set after ObDistTransCtx::reset
  ObDistTransCtx::reset();
  magic_number_ = PART_CTX_MAGIC_NUM;
  is_inited_ = false;
  mt_ctx_.reset();
  mt_ctx_factory_ = NULL;
  partition_service_ = NULL;
  // snapshot_version should be inited INVALID
  snapshot_version_ = ObTransVersion::INVALID_TRANS_VERSION;
  commit_task_count_ = 0;
  stmt_info_.reset();
  last_ask_scheduler_status_ts_ = 0;
  last_ask_scheduler_status_response_ts_ = 0;
}

int ObSlaveTransCtx::construct_context(const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;
  const int64_t msg_type = msg.get_msg_type();
  int64_t delay = ObServerConfig::get_instance().trx_2pc_retry_interval;

  CtxLockGuard guard(lock_);

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObSlaveTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (!msg.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(msg));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(set_scheduler_(msg.get_scheduler()))) {
    TRANS_LOG(WARN, "set scheduler error", KR(ret), "scheduler", msg.get_scheduler());
  } else if (OB_TRANS_START_STMT_REQUEST == msg_type) {
    if (0 != msg.get_session_id() && UINT32_MAX != msg.get_session_id()) {
      session_id_ = msg.get_session_id();
    }
    if (0 != msg.get_proxy_session_id() && UINT64_MAX != msg.get_proxy_session_id()) {
      proxy_session_id_ = msg.get_proxy_session_id();
    }
    delay = trans_expired_time_ - ObClockGenerator::getClock();
    if (delay > 0) {
      trans_2pc_timeout_ = delay;
    } else {
      ret = OB_TRANS_TIMEOUT;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(register_timeout_task_(trans_2pc_timeout_ + trans_id_.hash() % 1000000))) {
        TRANS_LOG(WARN, "register timeout task error", KR(ret), K_(self), K_(trans_id), K_(trans_2pc_timeout));
      }
      last_ask_scheduler_status_response_ts_ = last_ask_scheduler_status_ts_ = ObTimeUtility::current_time();
    }
  } else {
    TRANS_LOG(ERROR, "invalid message type", K(msg_type), K(msg));
    ret = OB_TRANS_INVALID_MESSAGE_TYPE;
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "construct transaction context error", KR(ret), "context", *this);
    (void)unregister_timeout_task_();
    set_exiting_();
    const bool commit = false;
    const int64_t global_trans_version = -1;
    (void)trans_end_(commit, global_trans_version);
    (void)trans_clear_();
  } else {
    TRANS_LOG(DEBUG, "construct transaction context success", "context", *this);
  }
  REC_TRANS_TRACE_EXT(tlog_, cons_context, OB_ID(ret), ret, OB_ID(read_only), is_readonly_, OB_ID(uref), get_uref());

  return ret;
}

int ObSlaveTransCtx::start_trans()
{
  int ret = OB_SUCCESS;
  const int64_t left_time = trans_expired_time_ - ObClockGenerator::getClock();
  const int64_t usec_per_sec = 1000 * 1000;

  CtxLockGuard guard(lock_);

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObSlaveTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (is_exiting_) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (for_replay_) {
    TRANS_LOG(WARN, "invalid state, transaction is replaying", "context", *this);
    ret = OB_ERR_UNEXPECTED;
  } else {
    trans_start_time_ = ObClockGenerator::getClock();
    if (OB_FAIL(register_timeout_task_(left_time + trans_id_.hash() % usec_per_sec))) {
      TRANS_LOG(WARN, "register timeout task error", KR(ret), "context", *this);
    }
    last_ask_scheduler_status_response_ts_ = last_ask_scheduler_status_ts_ = ObTimeUtility::current_time();
  }
  REC_TRANS_TRACE_EXT(tlog_, part_start_trans, OB_ID(ret), ret, OB_ID(left_time), left_time, OB_ID(uref), get_uref());

  return ret;
}

int ObSlaveTransCtx::trans_end_(const bool commit, const int64_t commit_version)
{
  int64_t start_us = ObTimeUtility::current_time();
  int ret = OB_SUCCESS;

  if (commit && commit_version <= 0) {
    TRANS_LOG(ERROR, "memtable ctx is null, unexpected error", K(commit_version), "context", *this);
    ret = OB_ERR_UNEXPECTED;
  } else {
    (void)mt_ctx_.trans_end(commit, commit_version);
  }
  int64_t end_us = ObTimeUtility::current_time();
  ObTransStatistic::get_instance().add_trans_mt_end_count(tenant_id_, 1);
  ObTransStatistic::get_instance().add_trans_mt_end_time(tenant_id_, end_us - start_us);

  return ret;
}

int ObSlaveTransCtx::trans_clear_()
{
  return mt_ctx_.trans_clear();
}

int ObSlaveTransCtx::start_stmt_(const int64_t sql_no)
{
  UNUSED(sql_no);
  return OB_NOT_SUPPORTED;
}

int ObSlaveTransCtx::start_bounded_staleness_task_(const int64_t sql_no, const int64_t snapshot_version,
    storage::ObIPartitionGroupGuard& pkey_guard, const int snapshot_gene_type)
{
  UNUSED(pkey_guard);
  int ret = OB_SUCCESS;
  bool need_check_and_set_snapshot_version = false;

  if (cluster_version_after_2200_() && OB_UNLIKELY(snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(INFO,
        "snapshot_version should be valid",
        K(snapshot_version),
        K(sql_no),
        K(snapshot_gene_type),
        "context",
        *this);
  } else if (sql_no == stmt_info_.get_sql_no()) {
    TRANS_LOG(INFO,
        "multiple start task, maybe self join scenario",
        K(sql_no),
        K(snapshot_version_),
        K(snapshot_version),
        K(snapshot_gene_type),
        "context",
        *this);

    if (ObTransSnapshotGeneType::NOTHING == snapshot_gene_type) {
      if (OB_UNLIKELY(snapshot_version_ <= 0)) {
        TRANS_LOG(WARN,
            "local snapshot_version should be valid",
            K(snapshot_version_),
            K(sql_no),
            K(snapshot_gene_type),
            "context",
            *this);
        ret = OB_ERR_UNEXPECTED;
      } else {
        need_check_and_set_snapshot_version = false;
      }
    } else {
      need_check_and_set_snapshot_version = true;
    }
  } else if (OB_FAIL(stmt_info_.start_stmt(sql_no))) {
    TRANS_LOG(WARN, "start stmt failed", KR(ret), K(sql_no), "context", *this);
  } else {
    need_check_and_set_snapshot_version = true;
  }

  if (OB_SUCC(ret) && need_check_and_set_snapshot_version) {
    if (OB_UNLIKELY(snapshot_version < 0)) {
      TRANS_LOG(ERROR, "unexpected snapshot version", K(sql_no), K(snapshot_version), "context", *this);
      ret = OB_ERR_UNEXPECTED;
    } else if (stmt_info_.is_stmt_ended()) {
      snapshot_version_ = snapshot_version;
      REC_TRANS_TRACE_EXT(tlog_,
          publish_version,
          OB_ID(sql_no),
          sql_no,
          OB_ID(publish_version),
          snapshot_version_,
          OB_ID(uref),
          get_uref());
    } else {
      // maybe self join scenario
      if (OB_UNLIKELY(snapshot_version_ != snapshot_version)) {
        TRANS_LOG(ERROR,
            "unexpected snapshot version",
            K(snapshot_version),
            K(snapshot_gene_type),
            K_(stmt_info),
            "context",
            *this);
        ret = OB_ERR_UNEXPECTED;
      }
      REC_TRANS_TRACE_EXT(tlog_,
          publish_version,
          OB_ID(sql_no),
          sql_no,
          OB_ID(publish_version),
          snapshot_version_,
          OB_ID(uref),
          get_uref());
    }
  }

  return ret;
}

int ObSlaveTransCtx::start_task(const ObTransDesc& trans_desc, const int64_t snapshot_version,
    const int snapshot_gene_type, const bool need_update_gts)
{
  UNUSED(trans_desc);
  UNUSED(snapshot_version);
  UNUSED(snapshot_gene_type);
  UNUSED(need_update_gts);
  return OB_NOT_SUPPORTED;
}

int ObSlaveTransCtx::end_task(const bool is_rollback, const sql::ObPhyPlanType plan_type, const int64_t sql_no)
{
  UNUSED(is_rollback);
  UNUSED(plan_type);
  UNUSED(sql_no);
  return OB_NOT_SUPPORTED;
  ;
}

int ObSlaveTransCtx::end_stmt_(const bool is_rollback, bool& need_response)
{
  UNUSED(is_rollback);
  UNUSED(need_response);
  return OB_NOT_SUPPORTED;
}

int ObSlaveTransCtx::handle_message(const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;

  CtxLockGuard guard(lock_);

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObSlaveTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (!msg.is_valid()) {
    TRANS_LOG(WARN, "invalid message", "context", *this, K(msg));
    ret = OB_TRANS_INVALID_MESSAGE;
  } else if (is_exiting_) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this, K(msg));
    ret = OB_TRANS_IS_EXITING;
  } else if (for_replay_) {
    TRANS_LOG(WARN, "invalid state, transaction is replaying", "context", *this);
    ret = OB_ERR_UNEXPECTED;
  } else {
    request_id_ = msg.get_request_id();
    const int64_t msg_type = msg.get_msg_type();
    switch (msg_type) {
      case OB_TRANS_START_STMT_REQUEST: {
        if (OB_FAIL(handle_start_stmt_request_(msg))) {
          TRANS_LOG(WARN, "handle start stmt request error", KR(ret), K(msg));
        }
        break;
      }
      case OB_TRANS_STMT_ROLLBACK_REQUEST: {
        if (OB_FAIL(handle_stmt_rollback_request_(msg))) {
          TRANS_LOG(WARN, "handle trans stmt rollback request error", KR(ret), K(msg));
        }
        break;
      }
      case OB_TRANS_CLEAR_REQUEST: {
        if (OB_FAIL(set_scheduler_(msg.get_scheduler()))) {
          TRANS_LOG(WARN, "set scheduler error", KR(ret), K(msg));
        } else if (OB_FAIL(set_participants_(msg.get_participants()))) {
          TRANS_LOG(WARN, "set participants error", KR(ret), K(msg));
        } else if (OB_FAIL(handle_trans_clear_request_(msg))) {
          TRANS_LOG(WARN, "handle trans clear request error", KR(ret), K(msg));
        } else {
          // do nothing
        }
        break;
      }
      case OB_TRANS_DISCARD_REQUEST: {
        if (OB_FAIL(handle_trans_discard_request_(msg))) {
          TRANS_LOG(WARN, "handle trans discard request error", KR(ret), K(msg));
        }
        break;
      }
      case OB_TRANS_ASK_SCHEDULER_STATUS_RESPONSE: {
        if (OB_FAIL(handle_trans_ask_scheduler_status_response_(msg))) {
          TRANS_LOG(WARN, "handle trans ask scheduler status response error", KR(ret), K(msg));
        }
        break;
      }
      default: {
        TRANS_LOG(ERROR, "invalid message type", "context", *this, K(msg_type));
        ret = OB_TRANS_INVALID_MESSAGE_TYPE;
        break;
      }
    }
    REC_TRANS_TRACE_EXT(tlog_, handle_message, OB_ID(ret), ret, OB_ID(msg_type), msg_type, OB_ID(uref), get_uref());
  }

  return ret;
}

int ObSlaveTransCtx::handle_timeout(const int64_t delay)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObClockGenerator::getClock();

  common::ObTimeGuard timeguard("part_handle_timeout", 10 * 1000);
  if (OB_SUCC(lock_.lock(5000000 /*5 seconds*/))) {
    CtxLockGuard guard(lock_, false);

    timeguard.click();
    TRANS_LOG(DEBUG, "handle participant transaction timeout", "context", *this);
    if (!is_inited_) {
      TRANS_LOG(WARN, "ObSlaveTransCtx not inited");
      ret = OB_NOT_INIT;
    } else if (is_exiting_) {
      TRANS_LOG(WARN, "transaction is exiting", "context", *this);
      ret = OB_TRANS_IS_EXITING;
    } else if (!timeout_task_.is_registered()) {
      // timer task is canceled, do nothing
    } else {
      timeguard.click();
      (void)unregister_timeout_task_();
      timeout_task_.set_running(true);
      if (now >= (trans_expired_time_ + OB_TRANS_WARN_USE_TIME)) {
        TRANS_LOG(ERROR, "transaction use too much time", "context", *this);
      }
      const bool commit = false;
      const int64_t global_trans_version = -1;
      if (is_trans_expired_()) {
        (void)trans_end_(commit, global_trans_version);
        (void)trans_clear_();
        set_exiting_();
        if (is_sp_trans_()) {
          if (ObPartTransAction::COMMIT == part_trans_action_) {
            TRANS_LOG(ERROR, "sp trans timeout, maybe client do not commit/rollback", "context", *this);
            FORCE_PRINT_TRACE(tlog_, "[trans error] ");
            need_print_trace_log_ = true;
          }
          ObTransStatistic::get_instance().add_trans_timeout_count(tenant_id_, 1);
          ObTransStatistic::get_instance().add_abort_trans_count(tenant_id_, 1);
        }
      }
      timeout_task_.set_running(false);
      timeguard.click();
    }
    REC_TRANS_TRACE_EXT(tlog_, handle_timeout, OB_ID(ret), ret, OB_ID(used), timeguard, OB_ID(uref), get_uref());
  } else {
    TRANS_LOG(WARN, "failed to acquire lock in specified time", K_(trans_id));
    unregister_timeout_task_();
    register_timeout_task_(delay);
  }

  return ret;
}

int ObSlaveTransCtx::commit(const bool is_rollback, sql::ObIEndTransCallback* cb, const bool is_readonly,
    const MonotonicTs commit_time, const int64_t stmt_expired_time, const ObStmtRollbackInfo& stmt_rollback_info,
    bool& need_convert_to_dist_trans)
{
  UNUSED(commit_time);
  UNUSED(stmt_expired_time);
  UNUSED(stmt_rollback_info);
  UNUSED(need_convert_to_dist_trans);
  int ret = OB_SUCCESS;
  part_trans_action_ = ObPartTransAction::COMMIT;

  CtxLockGuard guard(lock_);

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObSlaveTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(cb)) {
    TRANS_LOG(WARN, "invalid argument", KP(cb));
    ret = OB_INVALID_ARGUMENT;
  } else if (is_exiting_) {
    if (is_rollback) {
      cb->callback(OB_SUCCESS);
      cb = NULL;
    } else {
      ret = OB_TRANS_IS_EXITING;
    }
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
  } else if (for_replay_) {
    TRANS_LOG(WARN, "invalid state, transaction is replaying", "context", *this);
    ret = OB_ERR_UNEXPECTED;
  } else {
    commit_start_time_ = ObClockGenerator::getClock();
    if (is_readonly) {
      if (OB_FAIL(do_clear_())) {
        TRANS_LOG(WARN, "do clear error", KR(ret), "context", *this);
      } else if (OB_FAIL(end_trans_cb_.init(tenant_id_, cb))) {
        TRANS_LOG(WARN, "end trans callback init error", KR(ret));
      } else {
        end_trans_callback_(is_rollback, ret);
        TRANS_STAT_COMMIT_ABORT_TRANS_TIME(is_rollback, tenant_id_);
        TRANS_STAT_TOTAL_USED_TIME(tenant_id_, ctx_create_time_);
        TRANS_STAT_COMMIT_ABORT_TRANS_INC(is_rollback, tenant_id_);
      }
    }
  }

  REC_TRANS_TRACE_EXT(tlog_,
      sp_commit,
      OB_ID(ret),
      ret,
      OB_ID(is_rollback),
      is_rollback,
      OB_ID(is_readonly),
      is_readonly,
      OB_ID(uref),
      get_uref());

  return ret;
}

int ObSlaveTransCtx::post_trans_response_(const int64_t msg_type)
{
  int ret = OB_SUCCESS;
  ObTransMsg msg;

  if (!ObTransMsgTypeChecker::is_valid_msg_type(msg_type)) {
    TRANS_LOG(WARN, "invalid argument", K(msg_type), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(msg.init(tenant_id_,
                 trans_id_,
                 msg_type,
                 trans_expired_time_,
                 self_,
                 SCHE_PARTITION_ID,
                 scheduler_,
                 participants_,
                 trans_param_,
                 addr_,
                 request_id_,
                 OB_SUCCESS))) {
    TRANS_LOG(WARN, "transaction clear response message init error", KR(ret));
  } else if (OB_FAIL(post_trans_msg_(tenant_id_, scheduler_, msg, msg_type))) {
    TRANS_LOG(WARN, "post transaction message error", KR(ret), K_(scheduler), K(msg_type));
  } else {
    // do nothing
  }

  return ret;
}

int ObSlaveTransCtx::post_stmt_response_(const int64_t msg_type, int64_t sql_no)
{
  int ret = OB_SUCCESS;
  ObTransMsg msg;

  if (!ObTransMsgTypeChecker::is_valid_msg_type(msg_type) || sql_no <= 0) {
    TRANS_LOG(WARN, "invalid argument", K(msg_type), K(sql_no), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (OB_TRANS_START_STMT_RESPONSE == msg_type) {
      if (OB_FAIL(msg.init(tenant_id_,
              trans_id_,
              msg_type,
              trans_expired_time_,
              self_,
              SCHE_PARTITION_ID,
              trans_param_,
              addr_,
              sql_no,
              snapshot_version_,
              get_status_(),
              request_id_))) {
        TRANS_LOG(WARN, "message init error", KR(ret), K(scheduler_), K(msg_type));
      }
    } else {
      if (OB_FAIL(msg.init(tenant_id_,
              trans_id_,
              msg_type,
              trans_expired_time_,
              self_,
              SCHE_PARTITION_ID,
              trans_param_,
              addr_,
              sql_no,
              get_status_(),
              request_id_))) {
        TRANS_LOG(WARN, "message init error", KR(ret), K(scheduler_), K(msg_type));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(post_trans_msg_(tenant_id_, scheduler_, msg, msg_type))) {
      TRANS_LOG(WARN, "post transaction message error", KR(ret), K(scheduler_), K(msg_type));
    }
  }

  return ret;
}

int ObSlaveTransCtx::handle_start_stmt_request_(const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (!msg.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(msg), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (Ob2PCState::INIT != get_state_()) {
    need_print_trace_log_ = true;
    ret = OB_STATE_NOT_MATCH;
    TRANS_LOG(INFO, "state not match, discard message", KR(ret), "context", *this, K(msg));
  } else {
    bool need_generate_snapshot_version = false;
    if (!trans_param_.is_bounded_staleness_read()) {
      need_generate_snapshot_version = true;
    } else {
      if (ObTransSnapshotGeneType::APPOINT == msg.get_snapshot_gene_type()) {
      } else if (ObTransSnapshotGeneType::CONSULT == msg.get_snapshot_gene_type()) {
        need_generate_snapshot_version = true;
      } else {
        TRANS_LOG(WARN, "invalid snapshot generate type, unexpected error", K(msg));
        ret = OB_ERR_UNEXPECTED;
      }
    }
    if (OB_SUCC(ret) && need_generate_snapshot_version && OB_FAIL(start_stmt_(msg.get_sql_no()))) {
      TRANS_LOG(WARN, "partiticpant start stmt error", KR(ret), "context", *this, K(msg));
    }

    // also use status_ before 2PC. reply ret to scheduler via msg
    set_status_(ret);
    if (OB_SUCCESS != (tmp_ret = post_stmt_response_(OB_TRANS_START_STMT_RESPONSE, msg.get_sql_no()))) {
      TRANS_LOG(WARN, "post rpc create ctx response fail", "ret", tmp_ret, K(msg));
    }
  }

  return ret;
}

int ObSlaveTransCtx::handle_trans_discard_request_(const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;

  if (!msg.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(msg), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (Ob2PCState::INIT != get_state_()) {
    TRANS_LOG(ERROR, "protocol error", K(msg), "context", *this);
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_TRANS_DISCARD_REQUEST != msg.get_msg_type()) {
    TRANS_LOG(WARN, "unexpected msg_type", K(msg), "context", *this);
    ret = OB_ERR_UNEXPECTED;
  } else {
    set_exiting_();
    (void)unregister_timeout_task_();
    const bool commit = false;
    const int64_t global_trans_version = -1;
    (void)trans_end_(commit, global_trans_version);
    (void)trans_clear_();
  }

  return ret;
}

int ObSlaveTransCtx::handle_trans_clear_request_(const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;
  part_trans_action_ = ObPartTransAction::COMMIT;

  if (!msg.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(msg), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (Ob2PCState::INIT != get_state_()) {
    TRANS_LOG(ERROR, "protocol error", K(msg), "context", *this);
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_TRANS_CLEAR_REQUEST != msg.get_msg_type()) {
    TRANS_LOG(WARN, "unexpected msg_type", K(msg), "context", *this);
    ret = OB_ERR_UNEXPECTED;
  } else {
    set_exiting_();
    (void)unregister_timeout_task_();
    const bool commit = false;
    const int64_t global_trans_version = -1;
    (void)trans_end_(commit, global_trans_version);
    (void)trans_clear_();
    const int64_t msg_type = OB_TRANS_CLEAR_RESPONSE;
    if (OB_FAIL(post_trans_response_(msg_type))) {
      TRANS_LOG(WARN, "post transaction response error", KR(ret), K(msg_type));
    }
  }

  return ret;
}

int ObSlaveTransCtx::handle_stmt_rollback_request_(const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const bool is_rollback = true;
  bool need_response = true;

  if (!msg.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(msg), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (Ob2PCState::INIT != get_state_()) {
    // unexpect to receive a stmt rollback request in other state
    TRANS_LOG(ERROR, "protocol error", K(msg), "context", *this);
    ret = OB_ERR_UNEXPECTED;
  } else {
    if (OB_FAIL(end_stmt_(is_rollback, need_response))) {
      TRANS_LOG(WARN, "participant end stmt error", KR(ret), "context", *this, K(is_rollback));
    }
    if (need_response) {
      set_status_(ret);
      if (OB_SUCCESS != (tmp_ret = post_stmt_response_(OB_TRANS_STMT_ROLLBACK_RESPONSE, msg.get_sql_no()))) {
        TRANS_LOG(WARN, "post stmt rollback response error", "ret", tmp_ret, K(msg), "context", *this);
      }
    }
  }

  return ret;
}

int ObSlaveTransCtx::handle_trans_ask_scheduler_status_response_(const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;
  const int status = msg.get_status();
  last_ask_scheduler_status_response_ts_ = ObTimeUtility::current_time();

  if (OB_UNLIKELY(OB_TRANS_ASK_SCHEDULER_STATUS_RESPONSE != msg.get_msg_type())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected msg type", KR(ret), K(msg));
  } else if (OB_UNLIKELY(trans_id_ != msg.get_trans_id())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected trans id", KR(ret), K(msg));
  } else if (OB_UNLIKELY(scheduler_ != msg.get_scheduler())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected scheduler", KR(ret), K(msg));
  } else if (OB_UNLIKELY(is_sp_trans_())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "sp trans cannot ask scheduer", KR(ret), K(msg));
  } else if (OB_TRANS_CTX_NOT_EXIST == status || OB_PARTITION_NOT_EXIST == status) {
    TRANS_LOG(INFO, "[TRANS GC]slave scheduler is not exist", K(msg), K(status));
    need_print_trace_log_ = true;
    const bool commit = false;
    const int64_t global_trans_version = -1;
    (void)unregister_timeout_task_();
    (void)trans_end_(commit, global_trans_version);
    (void)trans_clear_();
    set_exiting_();
  } else {
    TRANS_LOG(DEBUG, "slave scheduler is OB_SUCCESS", K(msg), K(status));
  }

  return ret;
}

int ObSlaveTransCtx::do_clear_()
{
  int ret = OB_SUCCESS;

  if (is_readonly_) {
    set_state_(Ob2PCState::CLEAR);
    const bool commit = false;
    const int64_t global_trans_version = -1;
    (void)trans_end_(commit, global_trans_version);
    (void)trans_clear_();
    (void)unregister_timeout_task_();
    set_exiting_();
  }

  return ret;
}

bool ObSlaveTransCtx::need_to_ask_scheduler_status_()
{
  bool ret = false;
  if (!is_inited_) {
    ret = false;
  } else if (is_exiting_) {
    ret = false;
  } else if (for_replay_) {
    ret = false;
  } else if (is_sp_trans_() || is_mini_sp_trans_()) {
    ret = false;
  } else if (0 == last_ask_scheduler_status_ts_) {
    ret = false;
  } else if (ObTimeUtility::current_time() - last_ask_scheduler_status_ts_ < CHECK_SCHEDULER_STATUS_INTERVAL) {
    ret = false;
  } else {
    TRANS_LOG(DEBUG, "need to ask scheduler status", K(last_ask_scheduler_status_ts_));
    ret = true;
  }

  return ret;
}

int ObSlaveTransCtx::check_rs_scheduler_is_alive_(bool& is_alive)
{
  int ret = OB_SUCCESS;
  int64_t trace_time = 0;
  int64_t cur_time = ObTimeUtility::current_time();
  is_alive = true;
  share::ObAliveServerTracer* server_tracer = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (OB_UNLIKELY(for_replay_)) {
    TRANS_LOG(WARN, "invalid state, transaction is replaying", "context", *this);
    ret = OB_ERR_UNEXPECTED;
  } else if (cur_time - last_ask_scheduler_status_response_ts_ < CHECK_RS_SCHEDULER_STATUS_INTERVAL) {
    is_alive = true;
  } else if (OB_ISNULL(trans_service_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "trans service is NULL", KR(ret), K(*this));
  } else if (OB_ISNULL(server_tracer = trans_service_->get_server_tracer())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "server tracer is NULL", KR(ret), K(*this));
  } else if (OB_FAIL(server_tracer->is_alive(scheduler_, is_alive, trace_time))) {
    TRANS_LOG(WARN, "server tracer error", KR(ret), "context", *this);
    is_alive = true;
  } else {
    // do nothing
  }

  return ret;
}

int ObSlaveTransCtx::force_kill_()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (OB_UNLIKELY(for_replay_)) {
    TRANS_LOG(WARN, "invalid state, transaction is replaying", "context", *this);
    ret = OB_ERR_UNEXPECTED;
  } else {
    TRANS_LOG(INFO, "[TRANS GC]slave ctx force kill", "context", *this);
    need_print_trace_log_ = true;
    const bool commit = false;
    const int64_t global_trans_version = -1;
    (void)unregister_timeout_task_();
    (void)trans_end_(commit, global_trans_version);
    (void)trans_clear_();
    set_exiting_();
  }

  return ret;
}

int ObSlaveTransCtx::post_ask_scheduler_status_msg_()
{
  int ret = OB_SUCCESS;
  ObTransMsg msg;

  if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (OB_UNLIKELY(for_replay_)) {
    TRANS_LOG(WARN, "invalid state, transaction is replaying", "context", *this);
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(msg.init(tenant_id_,
                 trans_id_,
                 OB_TRANS_ASK_SCHEDULER_STATUS_REQUEST,
                 trans_expired_time_,
                 self_,
                 SCHE_PARTITION_ID,
                 trans_param_,
                 addr_,
                 scheduler_,
                 OB_SUCCESS))) {
    TRANS_LOG(WARN, "trans msg init error", KR(ret), K(msg));
  } else if (OB_ISNULL(rpc_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "trans rpc is NULL", KR(ret), "ctx", *this);
  } else if (OB_FAIL(rpc_->post_trans_msg(tenant_id_, scheduler_, msg, OB_TRANS_ASK_SCHEDULER_STATUS_REQUEST))) {
    TRANS_LOG(WARN, "post transaction message error", KR(ret), K(msg));
  } else {
    TRANS_LOG(DEBUG, "post OB_TRANS_ASK_SCHEDULER_STATUS_REQUEST", K_(trans_id));
  }
  last_ask_scheduler_status_ts_ = ObTimeUtility::current_time();

  return ret;
}

int ObSlaveTransCtx::check_scheduler_status()
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS == lock_.try_lock()) {
    if (!need_to_ask_scheduler_status_()) {
    } else if (OB_UNLIKELY(!is_inited_)) {
      TRANS_LOG(WARN, "ObPartTransCtx not inited");
      ret = OB_NOT_INIT;
    } else if (OB_UNLIKELY(is_exiting_)) {
      TRANS_LOG(WARN, "transaction is exiting", "context", *this);
      ret = OB_TRANS_IS_EXITING;
    } else if (OB_UNLIKELY(for_replay_)) {
      TRANS_LOG(WARN, "invalid state, transaction is replaying", "context", *this);
      ret = OB_ERR_UNEXPECTED;
    } else {
      last_ask_scheduler_status_ts_ = 0;
      bool is_alive = true;
      if (OB_FAIL(check_rs_scheduler_is_alive_(is_alive))) {
        TRANS_LOG(WARN, "check rs scheduler is alive error", KR(ret), K(is_alive), "context", *this);
      } else if (!is_alive) {
        if (OB_FAIL(force_kill_())) {
          TRANS_LOG(WARN, "force kill part_ctx error", KR(ret), "context", *this);
        }
      } else {
        // do nothing
      }
      if (is_alive && OB_FAIL(post_ask_scheduler_status_msg_())) {
        TRANS_LOG(WARN, "post ask scheduler status msg error", KR(ret), "context", *this);
      }
    }
    lock_.unlock();
  }

  return ret;
}

int ObSlaveTransCtx::kill(const KillTransArg& arg, ObEndTransCallbackArray& cb_array)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObEndTransCallbackItem cb_item;
  int cb_param = OB_TRANS_UNKNOWN;

  common::ObTimeGuard timeguard("part_kill", 10 * 1000);
  CtxLockGuard guard(lock_);

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else {
    need_print_trace_log_ = true;
    if (arg.graceful_) {
      TRANS_LOG(INFO, "transaction can not be killed", "context", *this);
      ret = OB_TRANS_CANNOT_BE_KILLED;
    } else {
      cb_param = OB_TRANS_KILLED;
      if (is_sp_trans_()) {
        TRANS_STAT_ABORT_TRANS_INC(tenant_id_);
      }
    }
    if (OB_SUCC(ret)) {
      mt_ctx_.trans_kill();
      (void)mt_ctx_.trans_clear();
      if (OB_FAIL(unregister_timeout_task_())) {
        TRANS_LOG(WARN, "unregister timer task error", KR(ret), "context", *this);
      }
      // ignore ret
      set_exiting_();
      TRANS_LOG(INFO, "transaction killed success", "context", *this, K(arg));
    }
    if (NULL != end_trans_cb_.get_cb()) {
      cb_item.cb_ = end_trans_cb_.get_cb();
      cb_item.retcode_ = cb_param;
      end_trans_cb_.reset();
      if (OB_SUCCESS != (tmp_ret = cb_array.push_back(cb_item))) {
        TRANS_LOG(WARN, "callback push back error", "ret", tmp_ret, "context", *this);
        ret = tmp_ret;
      }
    }
  }
  REC_TRANS_TRACE_EXT(tlog_,
      kill,
      OB_ID(ret),
      ret,
      OB_ID(arg1),
      arg.graceful_,
      OB_ID(used),
      timeguard.get_diff(),
      OB_ID(uref),
      get_uref());
  return ret;
}

}  // namespace transaction
}  // namespace oceanbase
