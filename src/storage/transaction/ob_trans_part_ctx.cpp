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

#define USING_LOG_PREFIX TRANS
#include "ob_trans_part_ctx.h"

#include "clog/ob_log_common.h"
#include "lib/profile/ob_perf_event.h"
#include "lib/utility/serialization.h"
#include "ob_dup_table_rpc.h"
#include "ob_trans_ctx_mgr.h"
#include "ob_trans_dependency.h"
#include "ob_trans_elr_task.h"
#include "ob_trans_status.h"
#include "ob_trans_result_info_mgr.h"
#include "ob_ts_mgr.h"
#include "share/ob_worker.h"
#include "share/rc/ob_context.h"
#include "storage/memtable/ob_lock_wait_mgr.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/ob_partition_service.h"
#include "clog/ob_log_common.h"
#include "ob_trans_elr_task.h"
#include "ob_dup_table_rpc.h"
#include "ob_trans_msg_type2.h"
#include "ob_trans_msg2.h"
#include "election/ob_election.h"
#include "ob_trans_split_adapter.h"
#include "ob_trans_coord_ctx.h"
#include "observer/omt//ob_tenant_config_mgr.h"

namespace oceanbase {

using namespace clog;
using namespace common;
using namespace memtable;
using namespace share;
using namespace sql;
using namespace storage;
using namespace election;

namespace transaction {
typedef ObRedoLogSyncResponseMsg::ObRedoLogSyncResponseStatus ObRedoLogSyncResponseStatus;

#define INC_ELR_STATISTIC(item)     \
  do {                              \
    if (can_elr_) {                 \
      partition_mgr_->inc_##item(); \
    }                               \
  } while (0)

void TransResultInfo::reset()
{
  if (!registered_) {
    if (NULL != result_info_) {
      ObTransResultInfoFactory::release(result_info_);
    }
  }
  result_info_ = NULL;
  registered_ = false;
}

int TransResultInfo::alloc_trans_result_info()
{
  int ret = OB_SUCCESS;
  if (NULL == (result_info_ = ObTransResultInfoFactory::alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "trans result info factory alloc task fail", K(ret));
  }
  return ret;
}

int ObPartTransCtx::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t trans_expired_time,
    const ObPartitionKey& self, ObITransCtxMgr* ctx_mgr, const ObStartTransParam& trans_param,
    const uint64_t cluster_version, ObTransService* trans_service, const uint64_t cluster_id,
    const int64_t leader_epoch, const bool can_elr)
{
  int ret = OB_SUCCESS;
  bool is_dup_table_partition_serving = true;

  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (OB_UNLIKELY(is_inited_)) {
    TRANS_LOG(WARN, "ObPartTransCtx inited twice");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_UNLIKELY(!trans_id.is_valid()) ||
             OB_UNLIKELY(!self.is_valid()) || OB_UNLIKELY(trans_expired_time <= 0) || OB_ISNULL(ctx_mgr) ||
             OB_UNLIKELY(!trans_param.is_valid()) || OB_UNLIKELY(cluster_version <= 0) || OB_ISNULL(trans_service)) {
    TRANS_LOG(WARN,
        "invalid argument",
        K(tenant_id),
        K(trans_id),
        K(self),
        K(leader_epoch),
        "ctx_mgr",
        OB_P(ctx_mgr),
        K(trans_param),
        K(cluster_version),
        KP(trans_service),
        K(cluster_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(clog_adapter_ = trans_service->get_clog_adapter()) ||
             OB_ISNULL(mt_ctx_factory_ = trans_service->get_memtable_ctx_factory()) ||
             OB_ISNULL(partition_service_ = trans_service->get_partition_service()) ||
             OB_ISNULL(big_trans_worker_ = trans_service->get_big_trans_worker()) ||
             OB_ISNULL(trans_status_mgr_ = trans_service->get_trans_status_mgr())) {
    TRANS_LOG(ERROR,
        "ObTransService is invalid, unexpected error",
        KP(clog_adapter_),
        KP(mt_ctx_factory_),
        KP(big_trans_worker_),
        KP(partition_service_),
        KP(trans_status_mgr_));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(submit_log_cb_.init(trans_service, self, trans_id, this))) {
    TRANS_LOG(WARN, "submit log callback inited error", KR(ret), K(self), K(trans_id));
  } else if (OB_FAIL(ObDistTransCtx::init(tenant_id,
                 trans_id,
                 trans_expired_time,
                 self,
                 ctx_mgr,
                 trans_param,
                 cluster_version,
                 trans_service))) {
    TRANS_LOG(WARN, "ObDistTransCtx inited error", KR(ret), K(self), K(trans_id));
  } else if (OB_FAIL(ctx_dependency_wrap_.init(this))) {
    TRANS_LOG(WARN, "ctx dependency wrap init error", KR(ret), K(*this));
  } else if (OB_FAIL(trans_service_->check_duplicated_partition(self, is_dup_table_trans_))) {
    TRANS_LOG(WARN, "check duplicated partition serving error", KR(ret), K(*this));
  } else if (is_dup_table_trans_ && !for_replay_ &&
             OB_FAIL(partition_mgr_->is_dup_table_partition_serving(true, is_dup_table_partition_serving))) {
    TRANS_LOG(WARN, "check dup table partition serving error", KR(ret), K(*this));
  } else if (is_dup_table_trans_ && (!is_dup_table_partition_serving)) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "duplicated partition not serving", K(ret), K(is_dup_table_partition_serving), K(*this));
  } else if (is_dup_table_trans_ && OB_ISNULL(redo_sync_task_ = ObDupTableRedoSyncTaskFactory::alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "duplicated partition alloc task fail", K(ret), K(*this));
  } else if (can_elr && OB_FAIL(result_info_.alloc_trans_result_info())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "trans result info factory alloc task fail", K(ret), K(*this));
  } else if (OB_FAIL(init_memtable_ctx_(trans_service, self.get_tenant_id()))) {
    TRANS_LOG(WARN, "ObPartTransCtx init memtable context error", KR(ret), K(self), K(trans_id));
  } else {
    // do nothing
  }
  if (OB_SUCC(ret)) {
    last_ask_scheduler_status_ts_ = ObTimeUtility::current_time();
    last_ask_scheduler_status_response_ts_ = ObTimeUtility::current_time();
    is_inited_ = true;
    if (is_readonly_) {
      mt_ctx_.set_read_only();
    }
    if (for_replay_) {
      mt_ctx_.trans_replay_begin();
    } else {
      mt_ctx_.trans_begin();
    }
    cluster_id_ = cluster_id;
    leader_epoch_ = leader_epoch;
    scheduler_ = trans_id.get_server();
    mt_ctx_.set_trans_ctx(this);
    mt_ctx_.set_for_replay(for_replay_);
    can_elr_ = can_elr;
    if (is_dup_table_trans_) {
      need_print_trace_log_ = true;
    }
    is_listener_ = false;
    listener_handler_ = NULL;
    redo_log_id_serialize_size_ = prev_redo_log_ids_.get_serialize_size();
    participants_serialize_size_ = partition_log_info_arr_.get_serialize_size();
    undo_serialize_size_ = undo_status_.get_serialize_size();
  }
  if (OB_FAIL(ret)) {
    if (NULL != redo_sync_task_) {
      ObDupTableRedoSyncTaskFactory::release(redo_sync_task_);
      redo_sync_task_ = NULL;
    }
    result_info_.destroy();
    set_exiting_();
    ObDistTransCtx::destroy();
  }

  return ret;
}

bool ObPartTransCtx::is_inited() const
{
  return ATOMIC_LOAD(&is_inited_);
}

int ObPartTransCtx::init_memtable_ctx_(ObTransService* trans_service, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  memtable::MemtableIDMap& id_map = static_cast<ObMemtableCtxFactory*>(mt_ctx_factory_)->get_id_map();
  common::ObIAllocator& malloc_allocator = static_cast<ObMemtableCtxFactory*>(mt_ctx_factory_)->get_malloc_allocator();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id));
  } else {
    if (OB_FAIL(mt_ctx_.init(tenant_id, id_map, malloc_allocator))) {
      TRANS_LOG(WARN, "memtable context init error", KR(ret));
    } else {
      uint32_t ctx_descriptor = IDMAP_INVALID_ID;
      if (OB_FAIL(id_map.assign(&mt_ctx_, ctx_descriptor))) {
        TRANS_LOG(WARN, "id map assign fail", KR(ret));
      } else if (0 == ctx_descriptor) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unexpected ctx descriptor", K(ctx_descriptor), "context", *this);
        need_print_trace_log_ = true;
        // add ref for trans_table_guard matained by memtable_ctx
      } else if (OB_FAIL(trans_service->get_part_trans_ctx_mgr().get_partition_trans_ctx_mgr_with_ref(
                     self_, *(mt_ctx_.get_trans_table_guard())))) {
        TRANS_LOG(WARN, "get trans part ctx mgr ref error", K(ret), K(*this));
      } else {
        mt_ctx_.set_ctx_descriptor(ctx_descriptor);
        // the p_mt_ctx_ is used to notify the lock_wait_mgr for early lock release txn
        p_mt_ctx_ = &mt_ctx_;
      }
    }
  }

  return ret;
}

void ObPartTransCtx::destroy()
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(is_inited_)) {
    if (OB_ISNULL(mt_ctx_factory_)) {
      TRANS_LOG(ERROR, "mt ctx is null, unexpected error", KP_(mt_ctx_factory), "context", *this);
    } else {
      int64_t memstore_count = 0;
      (void)mt_ctx_.check_memstore_count(memstore_count);
      if (memstore_count > 0) {
        if (NULL != trans_service_ && trans_service_->is_running()) {
          TRANS_LOG(
              ERROR, "active memstore count is not equal to 0, unexpected error", K(memstore_count), "context", *this);
          need_print_trace_log_ = true;
          (void)trans_clear_();
        }
      }
      memtable::MemtableIDMap& id_map = static_cast<ObMemtableCtxFactory*>(mt_ctx_factory_)->get_id_map();
      id_map.erase(mt_ctx_.get_ctx_descriptor());
    }
    if (is_exiting_ && 0 != submit_log_pending_count_) {
      TRANS_LOG(ERROR, "submit log pending count not equal to 0", "context", *this);
      need_print_trace_log_ = true;
    }
    if (is_gts_waiting_) {
      TRANS_LOG(ERROR, "transaction is waiting gts", "context", *this);
      need_print_trace_log_ = true;
    }
    if (OB_NOT_NULL(trans_audit_record_)) {
      ObElrTransArrGuard prev_trans_guard;
      ObElrTransArrGuard next_trans_guard;
      if (OB_FAIL(ctx_dependency_wrap_.get_prev_trans_arr_guard(prev_trans_guard))) {
        TRANS_LOG(WARN, "get prev trans arr guard error", KR(ret), K(*this));
      } else if (OB_FAIL(ctx_dependency_wrap_.get_next_trans_arr_guard(next_trans_guard))) {
        TRANS_LOG(WARN, "get next trans arr guard error", KR(ret), K(*this));
      } else {
        (void)trans_audit_record_->set_trans_dep_arr(
            prev_trans_guard.get_elr_trans_arr(), next_trans_guard.get_elr_trans_arr());
      }
    }
    if (NULL != redo_sync_task_) {
      ObDupTableRedoSyncTaskFactory::release(redo_sync_task_);
      redo_sync_task_ = NULL;
    }
    if (NULL != coord_ctx_) {
      trans_service_->get_coord_trans_ctx_mgr().revert_trans_ctx(coord_ctx_);
      coord_ctx_ = NULL;
    }
    REC_TRANS_TRACE_EXT(tlog_, destroy, OB_ID(arg1), (int64_t)(&submit_log_cb_));
    if (mt_ctx_.get_ref() != 0) {
      TRANS_LOG(ERROR, "memtable_ctx ref not match!!!", K(mt_ctx_.get_ref()), K(*this), K(mt_ctx_), K(&mt_ctx_));
    }
    result_info_.destroy();
    if (OB_NOT_NULL(listener_handler_)) {
      listener_handler_->destroy();
      ob_free(listener_handler_);
      listener_handler_ = NULL;
    }
    ObDistTransCtx::destroy();
    is_inited_ = false;
  }
}

void ObPartTransCtx::reset()
{
  // destroy();
  ObDistTransCtx::reset();
  // partctx, must be set after ObDistTransCtx::reset
  magic_number_ = PART_CTX_MAGIC_NUM;
  is_inited_ = false;
  clog_adapter_ = NULL;
  submit_log_cb_.reset();
  mt_ctx_.reset();
  mt_ctx_factory_ = NULL;
  big_trans_worker_ = NULL;
  redo_log_no_ = 0;
  mutator_log_no_ = 0;
  partition_service_ = NULL;
  trans_status_mgr_ = NULL;
  prev_redo_log_ids_.reset();
  partition_log_info_arr_.reset();
  // snapshot_version should be inited INVALID
  snapshot_version_ = ObTransVersion::INVALID_TRANS_VERSION;
  local_trans_version_ = ObTransVersion::INVALID_TRANS_VERSION;
  commit_task_count_ = 0;
  submit_log_pending_count_ = 0;
  submit_log_count_ = 0;
  cluster_id_ = 0;
  global_trans_version_ = ObTransVersion::INVALID_TRANS_VERSION;
  prepare_log_id_ = OB_INVALID_ID;
  prepare_log_timestamp_ = OB_INVALID_TIMESTAMP;
  leader_epoch_ = -1;
  stmt_info_.reset();
  preassigned_log_meta_.reset();
  min_log_id_ = OB_INVALID_ID;
  min_log_ts_ = INT64_MAX;
  end_log_ts_ = INT64_MAX;
  max_submitted_log_timestamp_ = 0;
  log_type_ = ObStorageLogType::OB_LOG_UNKNOWN;
  gts_request_ts_ = -1;
  stmt_expired_time_ = 0;
  quick_checkpoint_ = 0;
  commit_log_checksum_ = 0;
  proposal_leader_.reset();
  last_ask_scheduler_status_ts_ = 0;
  last_ask_scheduler_status_response_ts_ = 0;
  cur_query_start_time_ = 0;
  batch_commit_state_ = ObBatchCommitState::INIT;
  ctx_dependency_wrap_.reset();
  dup_table_msg_mask_set_.reset();
  dup_table_lease_addrs_.reset();
  sp_user_request_ = USER_REQUEST_UNKNOWN;
  same_leader_batch_partitions_count_ = 0;
  clear_log_base_ts_ = OB_INVALID_TIMESTAMP;
  need_checksum_ = false;
  is_prepared_ = false;
  is_gts_waiting_ = false;
  batch_commit_trans_ = false;
  is_trans_state_sync_finished_ = false;
  is_changing_leader_ = false;
  prepare_changing_leader_state_ = CHANGING_LEADER_STATE::NO_CHANGING_LEADER;
  can_rollback_stmt_ = true;
  has_trans_state_log_ = false;
  waiting_next_trans_rollback_ = false;
  redo_sync_task_ = NULL;
  result_info_.reset();
  is_dup_table_prepare_ = false;
  dup_table_syncing_log_id_ = UINT64_MAX;
  dup_table_syncing_log_ts_ = INT64_MAX;
  async_applying_log_ts_ = INT64_MAX;
  is_prepare_leader_revoke_ = false;
  is_local_trans_ = true;
  undo_status_.reset();
  max_durable_sql_no_ = 0;
  max_durable_log_ts_ = 0;
  is_dirty_ = false;
  forbidden_sql_no_ = -1;
  sp_user_request_ = USER_REQUEST_UNKNOWN;
  same_leader_batch_partitions_count_ = 0;
  is_hazardous_ctx_ = false;
  in_xa_prepare_state_ = false;
  end_log_ts_for_batch_commit_ = INT64_MAX;
  end_log_ts_for_elr_ = INT64_MAX;
  is_listener_ = false;
  if (OB_NOT_NULL(listener_handler_)) {
    listener_handler_->reset();
    ob_free(listener_handler_);
    listener_handler_ = NULL;
  }
  pg_ = NULL;
  enable_new_1pc_ = false;
  coord_ctx_ = NULL;
  is_redo_prepared_ = false;
  has_gen_last_redo_log_ = false;
  last_replayed_redo_log_id_ = 0;
  tmp_scheduler_.reset();
  last_redo_log_mutator_size_ = 0;
  has_write_or_replay_mutator_redo_log_ = false;
  is_in_redo_with_prepare_ = false;
  redo_log_id_serialize_size_ = 0;
  participants_serialize_size_ = 0;
  undo_serialize_size_ = 0;
  prev_checkpoint_id_ = 0;
}

int ObPartTransCtx::construct_context(const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;
  const int64_t msg_type = msg.get_msg_type();

  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!msg.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(msg));
    ret = OB_INVALID_ARGUMENT;
    // During constructing the txn context when the START_STMT request is received:
    // 1. participant need not to save the information of all participants
    // 2. participant need not to save the information of coordinator
    // coordinator and all participants can be obtained for txn prepare msg
  } else if (OB_FAIL(set_scheduler_(msg.get_scheduler()))) {
    TRANS_LOG(WARN, "set scheduler error", KR(ret), "scheduler", msg.get_scheduler());
  } else if (OB_TRANS_START_STMT_REQUEST == msg_type || OB_TRANS_STMT_ROLLBACK_REQUEST == msg_type) {
    if (0 != msg.get_session_id() && UINT32_MAX != msg.get_session_id()) {
      session_id_ = msg.get_session_id();
    }
    if (0 != msg.get_proxy_session_id() && UINT64_MAX != msg.get_proxy_session_id()) {
      proxy_session_id_ = msg.get_proxy_session_id();
    }
    const int64_t delay = trans_expired_time_ - ObClockGenerator::getClock();
    if (delay > 0) {
      trans_2pc_timeout_ = delay;
    } else {
      ret = OB_TRANS_TIMEOUT;
    }
    if (OB_SUCC(ret)) {
      (void)unregister_timeout_task_();
      if (OB_FAIL(register_timeout_task_(trans_2pc_timeout_ + trans_id_.hash() % 1000000))) {
        TRANS_LOG(WARN, "register timeout task error", KR(ret), K_(self), K_(trans_id), K_(trans_2pc_timeout));
      }
    }
  } else {
    TRANS_LOG(ERROR, "invalid message type", K(msg_type), K(msg));
    ret = OB_TRANS_INVALID_MESSAGE_TYPE;
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "construct transaction context error", KR(ret), "context", *this);
    // the unregister is feasible regardless whether the timeout task is
    // registered successfully
    (void)unregister_timeout_task_();
    set_exiting_();
    const bool commit = false;
    (void)trans_end_(commit, get_global_trans_version_());
    (void)trans_clear_();
  } else {
    TRANS_LOG(DEBUG, "construct transaction context success", "context", *this);
  }
  REC_TRANS_TRACE_EXT(tlog_, cons_context, OB_ID(ret), ret, OB_ID(read_only), is_readonly_, OB_ID(uref), get_uref());

  return ret;
}

int ObPartTransCtx::start_trans()
{
  int ret = OB_SUCCESS;
  const int64_t left_time = trans_expired_time_ - ObClockGenerator::getRealClock();
  const int64_t usec_per_sec = 1000 * 1000;

  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (OB_UNLIKELY(for_replay_)) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "invalid state, transaction is replaying", KR(ret), "context", *this);
  } else if (OB_UNLIKELY(left_time <= 0)) {
    TRANS_LOG(WARN, "transaction is timeout", K_(trans_expired_time), "context", *this);
    ret = OB_TRANS_TIMEOUT;
  } else {
    trans_start_time_ = ObClockGenerator::getClock();
    (void)unregister_timeout_task_();
    if (OB_FAIL(register_timeout_task_(left_time + trans_id_.hash() % usec_per_sec))) {
      TRANS_LOG(WARN, "register timeout task error", KR(ret), "context", *this);
    }
  }
  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
    set_exiting_();
    (void)unregister_timeout_task_();
  }
  REC_TRANS_TRACE_EXT(tlog_, start_trans, OB_ID(ret), ret, OB_ID(left_time), left_time, OB_ID(uref), get_uref());

  return ret;
}

int ObPartTransCtx::trans_end_(const bool commit, const int64_t commit_version)
{
  const int64_t start_us = ObTimeUtility::fast_current_time();
  int64_t raise_memstore = 0;
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_dirty = false;
  int64_t freeze_ts = 0;

  if (OB_ISNULL(partition_service_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObPartTransCtx not inited", KR(ret));
  } else if (OB_UNLIKELY(!commit)) {
    // Rollback transaction
    if (OB_FAIL(mt_ctx_.trans_end(commit, commit_version))) {
      TRANS_LOG(WARN, "trans end error", KR(ret), K(commit_version), "context", *this);
      need_print_trace_log_ = true;
    } else {
      if (OB_SUCCESS != (tmp_ret = ctx_dependency_wrap_.callback_next_trans_arr(ObTransResultState::ABORT))) {
        TRANS_LOG(WARN, "callback next transaciton array error", K(ret), "context", *this);
      }
      if (OB_FAIL(partition_service_->check_dirty_txn(self_, min_log_ts_, end_log_ts_, freeze_ts, is_dirty))) {
        TRANS_LOG(WARN, "check dirty txn error", KR(ret), K(min_log_ts_), K(end_log_ts_), "context", *this);
      }
    }
  } else {
    if (OB_UNLIKELY(commit_version <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "invalid commit version", KR(ret), K(commit_version), "context", *this);
      need_print_trace_log_ = true;
    } else {
      raise_memstore = ObTimeUtility::fast_current_time();
      if (OB_FAIL(mt_ctx_.trans_end(commit, commit_version))) {
        TRANS_LOG(WARN, "trans end error", KR(ret), K(commit_version), "context", *this);
        need_print_trace_log_ = true;
      } else {
        if (can_elr_) {
          if (OB_SUCCESS != (tmp_ret = register_trans_result_info_(ObTransResultState::COMMIT))) {
            TRANS_LOG(WARN, "register trans result info error", K(ret), K(*this));
          }
          if (OB_SUCCESS != (tmp_ret = ctx_dependency_wrap_.callback_next_trans_arr(ObTransResultState::COMMIT))) {
            TRANS_LOG(WARN, "callback next transaciton array error", K(ret), "context", *this);
          }
        }

        if (OB_FAIL(partition_service_->check_dirty_txn(self_, min_log_ts_, end_log_ts_, freeze_ts, is_dirty))) {
          TRANS_LOG(WARN, "check dirty txn error", KR(ret), K(min_log_ts_), K(end_log_ts_), "context", *this);
        } else {
          // reset the early lock release stat after the txn commits
          reset_elr_state_();
        }
      }
    }
  }

  if (OB_SUCC(ret) && is_dirty) {
    // mark transactions whose logs or data span multiple memtables as dirty
    // transactions
    if (mark_dirty_trans()) {
      TRANS_LOG(INFO,
          "mark dirty for leader crossing multiple memtables",
          KR(ret),
          K(min_log_ts_),
          K(end_log_ts_),
          K(freeze_ts),
          "context",
          *this);
      REC_TRANS_TRACE_EXT(tlog_,
          mark_dirty_trans_accross_memtable,
          OB_ID(ret),
          ret,
          OB_ID(arg1),
          min_log_ts_,
          OB_ID(arg2),
          end_log_ts_,
          OB_ID(arg3),
          freeze_ts,
          OB_ID(commit),
          commit);
    }
  }

  const int64_t end_us = ObTimeUtility::fast_current_time();
  // just for debug
  if (end_us - start_us > 5 * 1000 * 1000) {
    TRANS_LOG(WARN,
        "transaction end use too much time",
        KR(ret),
        K(commit),
        K(commit_version),
        "raise_memstore_used",
        raise_memstore - start_us,
        "trans_end_used",
        end_us - raise_memstore,
        "context",
        *this);
    need_print_trace_log_ = true;
  }
  return ret;
}

int ObPartTransCtx::trans_kill_()
{
  int ret = OB_SUCCESS;
  const bool for_replay = false;
  bool alloc = false;
  ObTransCtx* prev_trans_ctx = NULL;
  const bool need_completed_dirty_txn = false;

  if (ctx_dependency_wrap_.get_prev_trans_arr_count() > 0) {
    // remove yourself from the successor array of the predecessor
    ObElrPrevTransArrIterator iter(ctx_dependency_wrap_);
    ObElrTransInfo* info = NULL;
    while (OB_SUCC(ret) && NULL != (info = iter.next())) {
      const ObTransID& trans_id = info->get_trans_id();
      if (OB_FAIL(partition_mgr_->get_trans_ctx_(trans_id,
              for_replay,
              is_readonly_,
              trans_param_.is_bounded_staleness_read(),
              need_completed_dirty_txn,
              alloc,
              prev_trans_ctx))) {
        if (OB_TRANS_CTX_NOT_EXIST != ret) {
          TRANS_LOG(WARN, "get transaction context error", KR(ret), K(trans_id), "context", *this);
        }
        // No matter how the predecessor fails, you need to continue to visit
        // the next one
        ret = OB_SUCCESS;
      } else {
        if (OB_FAIL(ctx_dependency_wrap_.unregister_dependency_item(info->get_ctx_id(), prev_trans_ctx))) {
          TRANS_LOG(WARN, "unregister dependency item error", K(ret), K(trans_id));
        }
        (void)partition_mgr_->revert_trans_ctx_(prev_trans_ctx);
      }
    }
  }
  mt_ctx_.trans_kill();
  return ret;
}

int ObPartTransCtx::trans_clear_()
{
  return mt_ctx_.trans_clear();
}

int ObPartTransCtx::start_stmt_(const int64_t sql_no)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(sql_no <= 0)) {
    TRANS_LOG(WARN, "invalid argument", K(sql_no), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (stmt_info_.stmt_expired(sql_no)) {
    TRANS_LOG(WARN, "sql sequence is illegal", "context", *this, K(sql_no), K_(stmt_info));
    ret = OB_TRANS_SQL_SEQUENCE_ILLEGAL;
  } else if (stmt_info_.stmt_matched(sql_no)) {
    // do nothing
  } else {
    can_rollback_stmt_ = true;
    if (OB_FAIL(stmt_info_.start_stmt(sql_no))) {
      TRANS_LOG(WARN, "start stmt failed", KR(ret), K(sql_no), "context", *this);
      need_print_trace_log_ = true;
    } else if (OB_FAIL(generate_snapshot_(NULL))) {
      TRANS_LOG(WARN, "generate snapshot version error", KR(ret), K(sql_no), "context", *this);
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObPartTransCtx::set_cur_query_start_time(const int64_t cur_query_start_time)
{
  cur_query_start_time_ = cur_query_start_time;
  return OB_SUCCESS;
}

int ObPartTransCtx::start_task(const ObTransDesc& trans_desc, const int64_t snapshot_version,
    const bool need_update_gts, storage::ObIPartitionGroup* ob_partition)
{
  int ret = OB_SUCCESS;
  ObITsMgr* ts_mgr = NULL;
  const int64_t sql_no = trans_desc.get_sql_no();
  const int64_t expired_time = trans_desc.get_cur_stmt_expired_time();
  const int64_t trans_receive_sql_us = ObTimeUtility::fast_current_time();
  const sql::stmt::StmtType stmt_type = trans_desc.get_cur_stmt_desc().stmt_type_;
  const int64_t trx_lock_timeout = trans_desc.get_cur_stmt_desc().trx_lock_timeout_;

  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (IS_NOT_INIT || OB_ISNULL(partition_service_)) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(sql_no <= 0) || OB_UNLIKELY(expired_time <= 0) || OB_ISNULL(ob_partition)) {
    TRANS_LOG(WARN, "invalid argument", K(sql_no), K(expired_time), KP(ob_partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ts_mgr = get_ts_mgr_())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "ts mgr is null", KR(ret));
  } else if (OB_UNLIKELY(OB_SUCCESS != get_status_())) {
    ret = get_status_();
    TRANS_LOG(WARN, "start task failed", KR(ret), "context", *this);
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (OB_UNLIKELY(is_changing_leader_)) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "transaction is preparing changing leader", KR(ret), "context", *this);
  } else if (OB_UNLIKELY(for_replay_)) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "invalid state, transaction is replaying", KR(ret), "context", *this);
    need_print_trace_log_ = true;
  } else if (is_in_2pc_()) {
    TRANS_LOG(WARN, "transaction is in 2pc", "context", *this);
    ret = OB_TRANS_HAS_DECIDED;
  } else if (stmt_info_.stmt_expired(sql_no)) {
    ret = OB_TRANS_SQL_SEQUENCE_ILLEGAL;
    TRANS_LOG(WARN, "sql sequence is illegal", KR(ret), "context", *this, K(sql_no));
  } else if (OB_UNLIKELY(snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(
        WARN, "invalid snapshot version for start_task", KR(ret), K(snapshot_version), K(trans_desc), "context", *this);
  } else {
    snapshot_version_ = snapshot_version;
    REC_TRANS_TRACE_EXT(tlog_, snapshot, OB_ID(snapshot), snapshot_version_, OB_ID(uref), get_uref());
  }

  if (OB_SUCC(ret)) {
    bool updated = false;
    if (need_update_gts) {
      if (OB_FAIL(ts_mgr->update_local_trans_version(self_.get_tenant_id(), snapshot_version_, updated))) {
        TRANS_LOG(WARN, "update gts failed", KR(ret), "context", *this);
      } else {
        if (EXECUTE_COUNT_PER_SEC(16)) {
          TRANS_LOG(INFO, "update gts cache success", K(updated), "context", *this);
        }
        REC_TRANS_TRACE_EXT(tlog_,
            update_gts,
            OB_ID(ret),
            ret,
            OB_ID(sql_no),
            sql_no,
            OB_ID(uref),
            get_uref(),
            OB_ID(snapshot),
            snapshot_version_,
            OB_ID(tenant_id),
            self_.get_tenant_id());
      }
    }
    UNUSED(updated);
  }

  if (OB_SUCC(ret)) {
    ++commit_task_count_;
    pg_ = ob_partition;
    if (stmt_info_.main_stmt_change(sql_no) ||
        (cluster_version_before_2271_() && stmt_info_.main_stmt_change_compat(sql_no))) {
      set_cur_stmt_type(trans_desc.get_cur_stmt_desc().get_stmt_type(), trans_desc.get_cur_stmt_desc().is_sfu());
      if (OB_FAIL(mt_ctx_.sub_trans_begin(snapshot_version_, expired_time, false, trx_lock_timeout))) {
        TRANS_LOG(ERROR,
            "sub transaction begin should never fail",
            KR(ret),
            "context",
            *this,
            K_(snapshot_version),
            K(expired_time));
        // merge the predecessor information of previous stmt into predecessor array
      } else if (OB_FAIL(ctx_dependency_wrap_.merge_cur_stmt_prev_trans_arr())) {
        TRANS_LOG(WARN, "merge last statement prev trans arr error", K(ret), K(*this));
      } else {
        can_rollback_stmt_ = true;
      }
      //(void)audit_partition(false, stmt_type);
    } else {
      mt_ctx_.set_read_snapshot(snapshot_version_);
    }

    if (OB_SUCC(ret) && OB_FAIL(stmt_info_.start_task(sql_no, snapshot_version_))) {
      TRANS_LOG(WARN, "start task failed", K(ret), K(sql_no), K(*this));
    }
  }

  const ObStmtDesc& stmt_desc = trans_desc.get_cur_stmt_desc();
  if (OB_NOT_NULL(trans_audit_record_)) {
    const int64_t proxy_receive_sql_us = 0;  // stmt_desc.get_proxy_receive_us();
    const int64_t server_receive_sql_us = stmt_desc.cur_query_start_time_;
    (void)trans_audit_record_->set_start_stmt_info(sql_no,
        stmt_desc.phy_plan_type_,
        stmt_desc.trace_id_adaptor_,
        proxy_receive_sql_us,
        server_receive_sql_us,
        trans_receive_sql_us);
  }

  if (is_sp_trans_() || is_mini_sp_trans_()) {
    REC_TRANS_TRACE_EXT(tlog_,
        start_task,
        OB_ID(ret),
        ret,
        OB_ID(sql_no),
        sql_no,
        OB_ID(uref),
        get_uref(),
        OB_ID(need_update_gts),
        need_update_gts,
        OB_ID(trace_id),
        stmt_desc.trace_id_adaptor_,
        OB_ID(start_time),
        cur_query_start_time_);
  } else {
    REC_TRANS_TRACE_EXT(tlog_,
        start_task,
        OB_ID(ret),
        ret,
        OB_ID(sql_no),
        sql_no,
        OB_ID(uref),
        get_uref(),
        OB_ID(need_update_gts),
        need_update_gts);
  }

  return ret;
}

int ObPartTransCtx::generate_snapshot_(storage::ObIPartitionGroup* ob_partition)
{
  int ret = OB_SUCCESS;
  const bool check_election = true;
  int clog_status = OB_SUCCESS;
  int64_t snapshot_version = 0;
  int64_t leader_epoch = 0;
  ObTsWindows changing_leader_windows;
  ObITsMgr* ts_mgr = get_ts_mgr_();

  if (OB_ISNULL(ts_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "ts mgr is null", KR(ret));
  } else if (-1 == leader_epoch_) {
    if (NULL == ob_partition) {
      if (OB_FAIL(
              clog_adapter_->get_status(self_, check_election, clog_status, leader_epoch, changing_leader_windows))) {
        TRANS_LOG(WARN, "clog adapter get status error", KR(ret), "context", *this);
      } else {
        leader_epoch_ = leader_epoch;
      }
    } else if (OB_FAIL(clog_adapter_->get_status(
                   ob_partition, check_election, clog_status, leader_epoch, changing_leader_windows))) {
      TRANS_LOG(WARN, "clog adapter get status error", KR(ret), "context", *this);
    } else {
      leader_epoch_ = leader_epoch;
    }
  } else {
    // do nothing
  }
  if (OB_SUCCESS == ret) {
    if (OB_FAIL(ts_mgr->get_publish_version(self_.get_tenant_id(), snapshot_version))) {
      TRANS_LOG(WARN, "get publish version error", KR(ret), "context", *this);
    } else if (NULL == ob_partition) {
      if (OB_FAIL(
              clog_adapter_->get_status(self_, check_election, clog_status, leader_epoch, changing_leader_windows))) {
        TRANS_LOG(WARN, "clog adapter get status error", KR(ret), "context", *this);
      }
    } else if (OB_FAIL(clog_adapter_->get_status(
                   ob_partition, check_election, clog_status, leader_epoch, changing_leader_windows))) {
      TRANS_LOG(WARN, "clog adapter get status error", KR(ret), "context", *this);
    } else {
      // do nothing
    }
  }
  if (OB_SUCCESS == ret) {
    const int64_t cur_ts = ObTimeUtility::current_time();
    if (OB_SUCCESS != clog_status) {
      ret = clog_status;
    } else if (leader_epoch_ != leader_epoch) {
      TRANS_LOG(WARN,
          "leader epoch not identical when double check",
          K_(leader_epoch),
          "current_leader_epoch",
          leader_epoch,
          "context",
          *this);
      ret = OB_NOT_MASTER;
    } else if (GCONF.enable_smooth_leader_switch &&
               cur_ts > changing_leader_windows.get_start() + changing_leader_windows.get_left_size() / 3 &&
               cur_ts < changing_leader_windows.get_end()) {
      ret = OB_NOT_MASTER;
    } else {
      snapshot_version_ = snapshot_version;
    }
  }
  REC_TRANS_TRACE_EXT(tlog_, generate_snapshot_version, OB_ID(snapshot), snapshot_version_, OB_ID(uref), get_uref());

  return ret;
}

// TODO(): remove the constraint between rollback and logging
int ObPartTransCtx::end_task(
    const bool is_rollback, const ObTransDesc& trans_desc, const int64_t sql_no, const int64_t stmt_min_sql_no)
{
  int ret = OB_SUCCESS;
  bool need_retry = true;
  int64_t cnt = 0;

  while (need_retry) {
    {
      CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

      if (is_sp_trans_() && is_rollback && is_logging_()) {
        if (++cnt > MAX_END_STMT_RETRY_TIMES) {
          need_retry = false;
          ret = OB_TRANS_NEED_ROLLBACK;
          set_status_(OB_TRANS_NEED_ROLLBACK);
          TRANS_LOG(WARN, "end stmt retry too many times", K(is_rollback), K(trans_desc), K(sql_no), K(*this));
        }
      } else {
        need_retry = false;
        ret = end_task_(is_rollback, trans_desc, sql_no, stmt_min_sql_no);
      }
    }

    if (need_retry) {
      usleep(END_STMT_SLEEP_US);
    }
  }

  return ret;
}

int ObPartTransCtx::end_task_(
    const bool is_rollback, const ObTransDesc& trans_desc, const int64_t sql_no, const int64_t stmt_min_sql_no)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObPhyPlanType plan_type = trans_desc.get_cur_stmt_desc().phy_plan_type_;
  const sql::stmt::StmtType stmt_type = trans_desc.get_cur_stmt_desc().stmt_type_;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (sql_no <= 0) {
    TRANS_LOG(WARN, "invalid argument", K(sql_no));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (stmt_info_.stmt_expired(sql_no)) {
    ret = OB_TRANS_SQL_SEQUENCE_ILLEGAL;
    TRANS_LOG(WARN, "sql sequence is illegal", KR(ret), "context", *this, K(sql_no), K_(stmt_info));
  } else if (FALSE_IT(stmt_info_.end_task())) {
  } else if (OB_UNLIKELY(for_replay_)) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "invalid state, transaction is replaying", KR(ret), "context", *this);
  } else if (is_in_2pc_()) {
    TRANS_LOG(WARN, "transaction is in 2pc", "context", *this);
    ret = OB_TRANS_HAS_DECIDED;
  } else {
    if (is_sp_trans_()) {
      if (is_rollback) {
        --commit_task_count_;
        if (stmt_info_.is_task_match(stmt_min_sql_no)) {
          if (OB_FAIL(rollback_to_(stmt_min_sql_no - 1))) {
            set_status_(OB_TRANS_NEED_ROLLBACK);
            TRANS_LOG(WARN, "rollback to sql no error", K(ret), K(sql_no), K(*this));
          }
          /*
          if (0 == commit_task_count_) {
            const bool commit = false;
            (void)trans_end_(commit, get_global_trans_version_());
            (void)trans_clear_();
            (void)unregister_timeout_task_();
            set_exiting_();
          }*/
          if (sql::stmt::T_UPDATE == stmt_type) {
            // Remove the predecessor array to avoid the useless dependencies
            ctx_dependency_wrap_.clear_cur_stmt_prev_trans_item();
          }

          //(void)audit_partition(true, stmt_type);
          TRANS_LOG(
              DEBUG, "rollback stmt success", K(is_rollback), K(plan_type), K_(commit_task_count), "context", *this);
        }
      }
    } else if (OB_PHY_PLAN_REMOTE == plan_type || OB_PHY_PLAN_DISTRIBUTED == plan_type ||
               OB_PHY_PLAN_UNCERTAIN == plan_type || OB_PHY_PLAN_LOCAL == plan_type) {
      if (is_rollback) {
        --commit_task_count_;
        if (OB_PHY_PLAN_UNCERTAIN == plan_type) {
          need_print_trace_log_ = true;
        }
        /*
        if (stmt_info_.is_task_match(stmt_min_sql_no)) {
          (void)audit_partition(true, stmt_type);
        }*/
      }
    } else {
      // do nothing
    }

    try_restore_read_snapshot();
  }

  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }

  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;

    if (is_changing_leader_ && prepare_changing_leader_state_ == CHANGING_LEADER_STATE::STATEMENT_NOT_FINISH &&
        stmt_info_.is_task_match()) {
      if (0 == submit_log_count_) {
        if (OB_SUCCESS != (tmp_ret = submit_log_incrementally_(true /*need state log*/))) {
          TRANS_LOG(WARN,
              "submit_log_when_preparing_changing_leader_ failed",
              K(tmp_ret),
              K(ret),
              K(proposal_leader_),
              K(*this));
        }
      } else {
        prepare_changing_leader_state_ = CHANGING_LEADER_STATE::LOGGING_NOT_FINISH;
      }
    }
  }

  if (OB_NOT_NULL(trans_audit_record_)) {
    const int64_t trans_execute_sql_us = ObTimeUtility::fast_current_time();
    (void)trans_audit_record_->set_end_stmt_info(sql_no, trans_execute_sql_us, get_lock_for_read_retry_count());
  }

  REC_TRANS_TRACE_EXT(tlog_, end_task, OB_ID(ret), ret, OB_ID(is_rollback), is_rollback, OB_ID(uref), get_uref());

  return ret;
}

void ObPartTransCtx::try_restore_read_snapshot()
{
  int64_t snapshot_version = OB_INVALID_VERSION;
  if (OB_INVALID_VERSION != (snapshot_version = stmt_info_.get_top_snapshot_version())) {
    snapshot_version_ = snapshot_version;
    mt_ctx_.set_read_snapshot(snapshot_version_);
  }
}

bool ObPartTransCtx::is_xa_last_empty_redo_log_() const
{
  return is_xa_local_trans() && 0 == last_redo_log_mutator_size_;
}

/*
 * rollback stmt on this participant
 * @from_sql_no: max_sql_no this stmt executed
 * @to_sql_no  : stmt's start sql_no
 *
 * the state will rollbacked to : to_sql_no - 1
 *
 */
int ObPartTransCtx::rollback_stmt(const int64_t from_sql_no, const int64_t to_sql_no)
{
  int ret = OB_SUCCESS;
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);
  TRANS_LOG(DEBUG, "part_ctx rolblack_stmt", K(from_sql_no), K(to_sql_no), K(*this));
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited", K(from_sql_no), K(to_sql_no));
    ret = OB_NOT_INIT;
  } else if (for_replay_) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "transaction is replaying", KR(ret), K(from_sql_no), K(to_sql_no), K(*this));
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting, assume aborted", KR(ret), K(from_sql_no), K(to_sql_no), K(*this));
  } else if (OB_UNLIKELY(is_hazardous_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "current ctx status is hazardous", KR(ret), K(from_sql_no), K(to_sql_no), K(*this));
  } else if (OB_UNLIKELY(Ob2PCState::INIT != get_state_())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "protocol error", KR(ret), K(from_sql_no), K(to_sql_no), K(*this));
  } else {
    bool response = false;
    ret = end_stmt_(true, from_sql_no, to_sql_no, response);
  }
  return ret;
}

int ObPartTransCtx::end_stmt_(
    const bool is_rollback, const int64_t sql_no, const int64_t stmt_min_sql_no, bool& need_response)
{
  int ret = OB_SUCCESS;

  if (is_rollback) {
    if (is_logging_()) {
      TRANS_LOG(INFO, "transaction is logging", "context", *this);
      // The log is currently being written, let the scheduler retry
      need_response = false;
      ret = OB_TRANS_STMT_NEED_RETRY;
    } else if (stmt_info_.stmt_expired(sql_no, is_rollback)) {
      ret = OB_TRANS_SQL_SEQUENCE_ILLEGAL;
      TRANS_LOG(WARN, "sql sequence is illegal", "context", *this, K(sql_no), K_(stmt_info));
    } else if (stmt_info_.main_stmt_change(sql_no) ||
               (cluster_version_before_2271_() && stmt_info_.main_stmt_change_compat(sql_no))) {
      TRANS_LOG(INFO, "pre stmt rollback success", K(sql_no), "context", *this);
      need_response = true;
      can_rollback_stmt_ = true;
      stmt_info_.end_stmt(sql_no);
    } else if (is_changing_leader_) {
      need_response = false;
      ret = OB_NOT_MASTER;
      TRANS_LOG(WARN, "transaction is preparing changing leader", KR(ret), "context", *this);
    } else if (OB_FAIL(get_status_())) {
      TRANS_LOG(WARN, "transaction is not healthy when end_stmt_", KR(ret), "context", *this);
    } else if (!can_rollback_stmt_) {
      need_response = true;
      if (Ob2PCState::INIT == get_state_()) {
        if (redo_log_no_ > 0) {
          if (OB_FAIL(compensate_prepare_no_log_())) {
            TRANS_LOG(WARN, "compensate prepare no log error", KR(ret), "context", *this);
          }
        } else if (mutator_log_no_ > 0 || has_trans_state_log_) {
          if (OB_FAIL(compensate_mutator_abort_log_())) {
            TRANS_LOG(WARN, "compensate mutator abort log error", KR(ret), "context", *this);
          }
        } else {
          // do nothing
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unexpected state", KR(ret), "context", *this);
      }
      if (OB_SUCC(ret)) {
        ret = OB_TRANS_NEED_ROLLBACK;
      }
    } else if (!stmt_info_.is_task_match()) {
      // If the task is not finished, we need use the OB_TRANS_STMT_NEED_RETRY
      // to let the scheduler retry
      need_response = false;
      ret = OB_TRANS_STMT_NEED_RETRY;
      if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        TRANS_LOG(WARN, "current stmt info not match, need retry end stmt", KR(ret), "context", *this);
      }
    } else {
      need_response = true;
      stmt_info_.end_stmt(sql_no);
      if (OB_FAIL(rollback_to_(stmt_min_sql_no - 1))) {
        TRANS_LOG(WARN, "rollback to sql no error", K(ret), K(stmt_min_sql_no), K(sql_no), K(*this));
      }
    }
  }

  return ret;
}

int ObPartTransCtx::handle_message(const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;

#ifdef ERRSIM
  (void)DEBUG_SYNC_slow_txn_before_handle_message_(msg.get_msg_type());
#endif

  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!msg.is_valid())) {
    TRANS_LOG(WARN, "invalid message", "context", *this, K(msg));
    ret = OB_TRANS_INVALID_MESSAGE;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this, K(msg));
    ret = OB_TRANS_IS_EXITING;
  } else if (for_replay_ && OB_TRANS_ASK_SCHEDULER_STATUS_RESPONSE != msg.get_msg_type()) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "invalid state, transaction is replaying", KR(ret), "context", *this);
  } else if (OB_UNLIKELY(is_hazardous_ctx_)) {
    // avoids two duplicated logs
    if (EXECUTE_COUNT_PER_SEC(1)) {
      TRANS_LOG(ERROR, "current ctx status is hazardous", K(msg), K(*this));
    }
  } else {
#ifdef ERRSIM
    // Error injection test, used for scenarios where test statement rollback
    // fails
    const int64_t random = ObRandom::rand(0, 100);
    ret = E(EventTable::EN_TRANS_END_TASK_ERROR) OB_SUCCESS;
    if (OB_FAIL(ret) && OB_TRANS_STMT_ROLLBACK_REQUEST == msg.get_msg_type() && random < 80) {
      ret = OB_NOT_MASTER;
      TRANS_LOG(WARN, "[random fail] not master", KR(ret), K_(trans_id));
      return ret;
    }
#endif
    const int64_t msg_type = msg.get_msg_type();
    // we need save request id for request message
    if (ObTransMsgTypeChecker::is_request(msg_type)) {
      request_id_ = msg.get_request_id();
    }
    same_leader_batch_partitions_count_ = 0;
    switch (msg_type) {
      case OB_TRANS_START_STMT_REQUEST: {
        if (OB_FAIL(set_tmp_scheduler_(msg.get_sender_addr()))) {
          TRANS_LOG(WARN, "set xa tmp scheduler error", K(ret), K(msg));
        } else if (OB_FAIL(set_app_trace_id_(msg.get_app_trace_id_str()))) {
          TRANS_LOG(WARN, "set app trace id error", K(ret), K(msg), K(*this));
        } else if (OB_FAIL(handle_start_stmt_request_(msg))) {
          TRANS_LOG(WARN, "handle start stmt request error", KR(ret), K(msg));
        }
        break;
      }
      case OB_TRANS_STMT_ROLLBACK_REQUEST: {
        if (OB_FAIL(set_tmp_scheduler_(msg.get_sender_addr()))) {
          TRANS_LOG(WARN, "set xa tmp scheduler error", K(ret), K(msg));
        } else if (OB_FAIL(set_app_trace_id_(msg.get_app_trace_id_str()))) {
          TRANS_LOG(WARN, "set app trace id error", K(ret), K(msg), K(*this));
        } else if (OB_FAIL(handle_stmt_rollback_request_(msg))) {
          TRANS_LOG(WARN, "handle trans stmt rollback request error", KR(ret), K(msg));
        }
        break;
      }
      case OB_TRANS_CLEAR_REQUEST: {
        if (OB_FAIL(set_scheduler_(msg.get_scheduler()))) {
          TRANS_LOG(WARN, "set scheduler error", KR(ret), K(msg));
        } else if (OB_FAIL(calc_serialize_size_and_set_participants_(msg.get_participants()))) {
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
      case OB_TRANS_2PC_LOG_ID_REQUEST: {
        if (OB_FAIL(set_coordinator_(msg.get_sender()))) {
          TRANS_LOG(WARN, "set coordinator error", KR(ret), K(msg));
        } else if (OB_FAIL(handle_2pc_log_id_request_(msg))) {
          TRANS_LOG(WARN, "handle 2pc log id request error", KR(ret), "context", *this, K(msg));
        } else {
          // do nothing
        }
        break;
      }
      case OB_TRANS_2PC_PREPARE_REQUEST: {
        if (msg.get_sender_addr() == addr_ && msg.get_stc().is_valid()) {
          set_stc_(msg.get_stc());
        } else {
          set_stc_by_now_();
        }
        const int64_t tmp_config = ObServerConfig::get_instance().trx_2pc_retry_interval;
        // TODO do not set scheduler/coordinator/participants if state is init
        if (OB_FAIL(set_app_trace_info_(msg.get_app_trace_info()))) {
          TRANS_LOG(WARN, "set app trace info error", K(ret), K(msg), K(*this));
        } else if (OB_FAIL(set_scheduler_(msg.get_scheduler()))) {
          TRANS_LOG(WARN, "set scheduler error", KR(ret), K(msg));
        } else if (OB_FAIL(set_coordinator_(msg.get_coordinator()))) {
          TRANS_LOG(WARN, "set coordinator error", KR(ret), K(msg));
        } else if (OB_FAIL(calc_serialize_size_and_set_participants_(msg.get_participants()))) {
          TRANS_LOG(WARN, "set participants error", KR(ret), K(msg));
        } else if (OB_FAIL(set_xid_(msg.get_xid()))) {
          TRANS_LOG(WARN, "set xid error", KR(ret), K(msg));
        } else if (OB_FAIL(handle_2pc_prepare_request_(msg))) {
          TRANS_LOG(WARN, "handle 2pc preprare request error", KR(ret), "context", *this, K(msg));
        } else if (OB_FAIL(unregister_timeout_task_())) {
          TRANS_LOG(WARN, "unregister timeout handler error", K(ret), "context", *this);
        } else if (OB_FAIL(register_timeout_task_(std::min(tmp_config, (int64_t)MAX_TRANS_2PC_TIMEOUT_US)))) {
          TRANS_LOG(WARN, "register timeout handler error", K(ret), "context", *this);
        } else {
          // do nothing
        }
        break;
      }
      case OB_TRANS_2PC_PRE_COMMIT_REQUEST: {
        same_leader_batch_partitions_count_ = msg.get_batch_same_leader_partitions().count();
        if (OB_FAIL(handle_2pc_pre_commit_request_(msg.get_trans_version(), true))) {
          TRANS_LOG(WARN, "handle 2pc pre commit request error", KR(ret), "context", *this, K(msg));
        }
        break;
      }
      case OB_TRANS_2PC_COMMIT_REQUEST: {
        same_leader_batch_partitions_count_ = msg.get_batch_same_leader_partitions().count();
        if (OB_FAIL(handle_2pc_commit_request_(msg))) {
          TRANS_LOG(WARN, "handle 2pc commit request error", KR(ret), "context", *this, K(msg));
        }
        break;
      }
      case OB_TRANS_2PC_COMMIT_CLEAR_REQUEST: {
        if (OB_FAIL(handle_2pc_commit_clear_request_(msg.get_trans_version(), true))) {
          TRANS_LOG(WARN, "handle 2pc commit request error", KR(ret), "context", *this, K(msg));
        }
        break;
      }
      case OB_TRANS_2PC_ABORT_REQUEST: {
        same_leader_batch_partitions_count_ = msg.get_batch_same_leader_partitions().count();
        if (OB_FAIL(handle_2pc_abort_request_(msg))) {
          TRANS_LOG(WARN, "handle 2pc abort request error", KR(ret), "context", *this, K(msg));
        }
        break;
      }
      case OB_TRANS_2PC_CLEAR_REQUEST: {
        if (is_readonly_) {
          if (OB_FAIL(set_scheduler_(msg.get_scheduler()))) {
            TRANS_LOG(WARN, "set scheduler error", KR(ret), K(msg));
          } else if (OB_FAIL(set_coordinator_(msg.get_coordinator()))) {
            TRANS_LOG(WARN, "set coordinator error", KR(ret), K(msg));
          } else if (OB_FAIL(calc_serialize_size_and_set_participants_(msg.get_participants()))) {
            TRANS_LOG(WARN, "set participants error", KR(ret), K(msg));
          } else {
            // do nothing
          }
        }
        if (OB_SUCC(ret)) {
          same_leader_batch_partitions_count_ = msg.get_batch_same_leader_partitions().count();
          if (OB_FAIL(handle_2pc_clear_request_(msg))) {
            TRANS_LOG(WARN, "handle 2pc clear request error", KR(ret), "context", *this, K(msg));
          }
        }
        break;
      }
      case OB_TRANS_ASK_SCHEDULER_STATUS_RESPONSE: {
        if (OB_FAIL(handle_trans_ask_scheduler_status_response_(msg))) {
          TRANS_LOG(WARN, "handle trans ask scheduler status response error", KR(ret), K(msg));
        }
        break;
      }
      case OB_TRANS_SAVEPOINT_ROLLBACK_REQUEST: {
        if (OB_FAIL(set_tmp_scheduler_(msg.get_sender_addr()))) {
          TRANS_LOG(WARN, "set xa tmp scheduler error", KR(ret), K(msg));
        } else if (OB_FAIL(handle_savepoint_rollback_request_(msg.get_sql_no(), msg.get_additional_sql_no(), true))) {
          TRANS_LOG(WARN, "handle savepoint request error", KR(ret), K(msg));
        }
        break;
      }
      default: {
        TRANS_LOG(ERROR, "invalid message type", "context", *this, K(msg_type));
        ret = OB_TRANS_INVALID_MESSAGE_TYPE;
        break;
      }
    }
    REC_TRANS_TRACE_EXT(tlog_,
        handle_message,
        OB_ID(ret),
        ret,
        OB_ID(msg_type),
        msg_type,
        OB_ID(request_id),
        request_id_,
        OB_ID(uref),
        get_uref());
  }

  return ret;
}

int ObPartTransCtx::handle_timeout(const int64_t delay)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool has_redo_log = false;
  const int64_t now = ObClockGenerator::getClock();
  common::ObTimeGuard timeguard("part_handle_timeout", 10 * 1000);
  if (OB_SUCC(lock_.lock(5000000 /*5 seconds*/))) {
    CtxTransTableLockGuard guard(lock_, trans_table_seqlock_, false);
    timeguard.click();
    TRANS_LOG(DEBUG, "handle participant transaction timeout", "context", *this);
    if (IS_NOT_INIT) {
      TRANS_LOG(WARN, "ObPartTransCtx not inited");
      ret = OB_NOT_INIT;
    } else if (OB_UNLIKELY(is_exiting_)) {
      TRANS_LOG(WARN, "transaction is exiting", "context", *this);
      ret = OB_TRANS_IS_EXITING;
    } else if (!timeout_task_.is_registered()) {
      // timer task is canceled, do nothing
    } else {
      timeguard.click();
      (void)unregister_timeout_task_();
      update_trans_2pc_timeout_();
      timeout_task_.set_running(true);
      if (now >= (trans_expired_time_ + OB_TRANS_WARN_USE_TIME)) {
        TRANS_LOG(ERROR, "transaction use too much time", "context", *this);
      }
      if (is_sp_trans_()) {
        // Return the txn timeout when txn is both trx_timeout and stmt_timeout
        if (is_trans_expired_()) {
          // Invoke the end txn callback
          end_trans_callback_(OB_TRANS_TIMEOUT);
        } else if (stmt_expired_time_ > 0 && now > stmt_expired_time_) {
          end_trans_callback_(OB_TRANS_STMT_TIMEOUT);
        } else {
          (void)register_timeout_task_(trans_2pc_timeout_);
        }
      }
      if (!for_replay_) {
        if (is_in_2pc_()) {
          need_refresh_location_ = true;
          (void)drive_();
          need_refresh_location_ = false;
          // Flush aggregated log bugger
          if (is_logging_()) {
            common::ObVersion version(2, 0);
            if (OB_UNLIKELY(
                    OB_SUCCESS != (tmp_ret = clog_adapter_->flush_aggre_log(
                                       self_, version, pg_, partition_mgr_->get_trans_log_buffer_aggre_container())))) {
              TRANS_LOG(WARN, "flush aggre log failed", K(tmp_ret), K(*this));
            }
          }
        } else if (is_logging_()) {
          (void)drive_();
        } else if (is_trans_expired_()) {
          if (is_logging_()) {
            part_trans_action_ = ObPartTransAction::DIED;
            TRANS_LOG(WARN, "in logging when handle timeout", K(ret), "context", *this);
          } else if (has_logged_()) {
            if (OB_FAIL(submit_log_async_(OB_LOG_MUTATOR_ABORT, has_redo_log))) {
              TRANS_LOG(WARN, "submit mutator abort log failed", K(ret), "context", *this);
            } else {
              TRANS_LOG(INFO, "submit mutator abort log success", "context", *this);
            }
            (void)drive_();
          } else {
            const bool commit = false;
            (void)trans_end_(commit, get_global_trans_version_());
            (void)trans_clear_();
            set_exiting_();
            if (is_sp_trans_()) {
              // The client doesnot commit or rollback until txn timeouts. We
              // need print the ERROR to trace the problem
              if (ObPartTransAction::COMMIT != part_trans_action_ && ObPartTransAction::ABORT != part_trans_action_) {
                TRANS_LOG(WARN, "sp trans timeout, maybe client do not commit/rollback", "context", *this);
                FORCE_PRINT_TRACE(tlog_, "[trans error] ");
                need_print_trace_log_ = true;
              }
              ObTransStatistic::get_instance().add_trans_timeout_count(tenant_id_, 1);
              ObTransStatistic::get_instance().add_abort_trans_count(tenant_id_, 1);
            }
          }
        } else {
          (void)drive_();
        }
      }
      timeout_task_.set_running(false);
      timeguard.click();
    }
    REC_TRANS_TRACE_EXT(tlog_, handle_timeout, OB_ID(ret), ret, OB_ID(used), timeguard, OB_ID(uref), get_uref());
  } else {
    TRANS_LOG(WARN, "failed to acquire lock in specified time", K_(trans_id));
    (void)unregister_timeout_task_();
    (void)register_timeout_task_(delay);
  }

  return ret;
}

// The function is only invoked during partition split
int ObPartTransCtx::wait_1pc_trx_end_in_spliting(bool& trx_end)
{
  int ret = OB_SUCCESS;
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);
  trx_end = true;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (!for_replay_) {
    // The txn will not go through 1PC if the batch_commit_trans_ is false
    if (batch_commit_trans_) {
      if (!is_exiting_) {
        trx_end = false;
      }
    }
  } else {
    // On Follower, the partition has finished replaying all logs before replaying SPLIT_SOURCE_LOG
    // 1. If the txn is not in 2pc, the 1PC is not allowed for it
    // 2. If no redo is replayed, it will wait for retry
    if (batch_commit_trans_) {
      if (redo_log_no_ > 1) {
        trx_end = false;
      } else if (!is_exiting_) {
        trx_end = false;
      }
    }
  }

  return ret;
}

int ObPartTransCtx::kill(const KillTransArg& arg, ObEndTransCallbackArray& cb_array)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObEndTransCallbackItem cb_item;
  int cb_param = OB_TRANS_UNKNOWN;
  bool has_redo_log = false;

  common::ObTimeGuard timeguard("part_kill", 10 * 1000);
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (is_exiting_) {
    // TODO(): refine the semantics of the dirty txn
    if (is_dirty_) {
      if (arg.graceful_) {
        remove_trans_table_();
        TRANS_LOG(INFO, "release dirty trans context when graceful kill", K(*this), K(arg));
      } else {
        trans_kill_();
        (void)trans_clear_();
        remove_trans_table_();
        TRANS_LOG(INFO, "release dirty trans context when ungraceful kill", K(*this), K(arg));
      }
    } else {
      TRANS_LOG(INFO, "trans is existing when being killed", K(*this), K(arg));
    }
  } else {
    need_print_trace_log_ = true;
    if (arg.graceful_) {
      if (is_in_2pc_() || is_logging_() || is_readonly_ || is_pre_preparing_()) {
        ret = OB_TRANS_CANNOT_BE_KILLED;
      } else if (has_logged_()) {
        if (OB_FAIL(submit_log_async_(OB_LOG_MUTATOR_ABORT, has_redo_log))) {
          TRANS_LOG(WARN, "submit mutator abort log failed", KR(ret), "context", *this);
        } else {
          TRANS_LOG(INFO, "submit mutator abort log success", "context", *this);
        }
        if (OB_SUCC(ret)) {
          ret = OB_TRANS_CANNOT_BE_KILLED;
        }
      } else {
        cb_param = OB_TRANS_KILLED;
        if (is_sp_trans_()) {
          TRANS_STAT_ABORT_TRANS_INC(tenant_id_);
        }
      }
      if (OB_SUCCESS != ret) {
        TRANS_LOG(INFO, "transaction can not be killed", KR(ret), "context", *this);
      }
    }
    if (OB_SUCC(ret)) {
      trans_kill_();
      // Force kill cannot guarantee the consistency, so we just set end_log_ts
      // to zero
      end_log_ts_ = 0;
      (void)trans_clear_();
      if (OB_FAIL(unregister_timeout_task_())) {
        TRANS_LOG(WARN, "unregister timer task error", KR(ret), "context", *this);
      }
      // Ignore ret
      set_exiting_();
      TRANS_LOG(INFO, "transaction killed success", "context", *this, K(arg));
    }
    // We need to invoke the callback of the sql regardless of whether the
    // context can be killed graceful
    if (NULL != end_trans_cb_.get_cb()) {
      cb_item.cb_ = end_trans_cb_.get_cb();
      cb_item.retcode_ = cb_param;
      // The errcode should not be overwritten
      if (OB_SUCCESS != (tmp_ret = cb_array.push_back(cb_item))) {
        TRANS_LOG(WARN, "callback push back error", "ret", tmp_ret, "context", *this);
        ret = tmp_ret;
      } else {
        end_trans_cb_.reset();
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

int ObPartTransCtx::callback_big_trans(
    const ObPartitionKey& pkey, const int64_t log_type, const int64_t log_id, const int64_t timestamp)
{
  int ret = OB_SUCCESS;
  bool need_revert_ctx = false;
  {
    CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

    if (!pkey.is_valid() || log_id <= 0 || timestamp <= 0 ||
        (((log_type & OB_LOG_SP_TRANS_COMMIT) <= 0) && ((log_type & OB_LOG_SP_TRANS_ABORT) <= 0) &&
            ((log_type & OB_LOG_TRANS_COMMIT) <= 0) && ((log_type & OB_LOG_TRANS_ABORT) <= 0))) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey), K(log_type), K(log_id), K(timestamp), "context", *this);
    } else if (OB_ISNULL(partition_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unxpected partition mgr", KR(ret), KP_(partition_mgr), "context", *this);
    } else if (OB_FAIL(partition_mgr_->acquire_ctx_ref(trans_id_))) {
      TRANS_LOG(WARN, "get trans ctx error", KR(ret), "context", *this);
    } else {
      need_revert_ctx = true;
      // add ref
      dec_submit_log_count_();
      dec_submit_log_pending_count_();
      TRANS_LOG(INFO, "start to callback big transaction", K(log_type), K(log_id), K(timestamp), "context", *this);
      if ((log_type & OB_LOG_SP_TRANS_COMMIT) != 0 || (log_type & OB_LOG_SP_TRANS_ABORT) != 0) {
        const bool commit = (log_type == OB_LOG_SP_TRANS_COMMIT ? true : false);
        if (OB_FAIL(on_sp_commit_(commit))) {
          TRANS_LOG(WARN, "on sp commit error", KR(ret), K(log_type), K(log_id), K(timestamp), "context", *this);
        }
      } else if ((log_type & OB_LOG_TRANS_COMMIT) != 0) {
        if (OB_FAIL(on_dist_commit_())) {
          TRANS_LOG(WARN,
              "ObPartTransCtx on dist commit error",
              KR(ret),
              K(log_type),
              K(log_id),
              K(timestamp),
              "context",
              *this);
        }
      } else if ((log_type & OB_LOG_TRANS_ABORT) != 0) {
        if (OB_FAIL(on_dist_abort_())) {
          TRANS_LOG(WARN,
              "ObPartTransCtx on dist abort error",
              KR(ret),
              K(log_type),
              K(log_id),
              K(timestamp),
              "context",
              *this);
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "unexpected log type", K(log_type), K(log_id), K(timestamp), "context", *this);
      }

      async_applying_log_ts_ = INT64_MAX;
    }
  }
  if (need_revert_ctx) {
    ret = revert_self_();
  }

  return ret;
}

bool ObPartTransCtx::not_need_write_next_log_(const int64_t log_type)
{
  // In order to not stuck the progress of leader revoke, the dup table will not
  // continue to submit log. While it depends on the ABORT log to kill the dup
  // table txns.
  return is_dup_table_trans_ && is_prepare_leader_revoke_ && ObStorageLogTypeChecker::is_trans_redo_log(log_type);
}

void ObPartTransCtx::reset_prev_redo_log_ids()
{
  prev_redo_log_ids_.reset();
  redo_log_id_serialize_size_ = 0;
}

// when submit log success
int ObPartTransCtx::on_sync_log_success(
    const int64_t log_type, const int64_t log_id, const int64_t timestamp, const bool batch_committed)
{
  int ret = OB_SUCCESS;
  int64_t start_us = ObTimeUtility::fast_current_time();
  int64_t end_us = 0;
  bool has_redo_log = false;
  bool has_pending_cb = true;
  debug_slow_on_sync_log_success_(log_type);

  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!ObTransLogType::is_valid(log_type)) || OB_UNLIKELY(log_id < 0) ||
             OB_UNLIKELY(timestamp < 0)) {
    TRANS_LOG(WARN, "invalid argument", "context", *this, K(log_type), K(log_id), K(timestamp));
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (ObStorageLogTypeChecker::has_trans_mutator(log_type)) {
      mt_ctx_.sync_log_succ(log_id, timestamp, has_pending_cb);
    }

    dec_submit_log_count_();
    update_durable_log_id_ts_(log_type, log_id, timestamp);

    if ((log_type & OB_LOG_TRANS_RECORD) != 0) {
      reset_prev_redo_log_ids();
      prev_checkpoint_id_ = log_id;
      TRANS_LOG(INFO, "succeed to update prev_checkpoint_id ||| ", K(hash()), K(get_partition()), K(prev_checkpoint_id_));
      // decide and then submit the next log
      if (need_record_log()) {
        if (OB_FAIL(submit_log_task_(OB_LOG_TRANS_RECORD, has_redo_log))) {
          TRANS_LOG(WARN, "submit trans record log task error", KR(ret), "context", *this);
        }
      } else {
        ObStorageLogType next_log_type = ObStorageLogType::OB_LOG_UNKNOWN;
        if (OB_FAIL(decide_and_submit_next_log_(next_log_type, has_redo_log, has_pending_cb))) {
          TRANS_LOG(ERROR, "decide and submit next log failed", KR(ret), "context", *this);
        }
      }
    }

    if ((log_type & OB_LOG_SP_TRANS_REDO) != 0) {
      has_write_or_replay_mutator_redo_log_ = true;
      ++redo_log_no_;
      if (redo_log_no_ == 1) {
        // The log is completed, we need verify the txn checksum
        need_checksum_ = true;
      }
      if (OB_FAIL(prev_redo_log_ids_.push_back(log_id))) {
        TRANS_LOG(WARN, "sp redo log id push back error", KR(ret), "context", *this, K(log_id));
      } else if (cluster_version_after_3100_() && need_record_log()) {
        if (OB_FAIL(submit_log_task_(OB_LOG_TRANS_RECORD, has_redo_log))) {
          TRANS_LOG(WARN, "submit trans record log task error", KR(ret), "context", *this);
        }
      } else if (!not_need_write_next_log_(log_type)
                 && OB_FAIL(submit_log_task_(OB_LOG_SP_TRANS_COMMIT, has_redo_log))) {
        TRANS_LOG(WARN, "submit sp trans commit log task error", KR(ret), "context", *this);
      } else {
        // do nothing
      }
      if (OB_FAIL(ret)) {
        // Mark the txn need to rollback, and rely on the timer task of
        // coordinator to progress the txn.
        set_status_(OB_TRANS_ROLLBACKED);
      }
    }
    if ((log_type & OB_LOG_SP_ELR_TRANS_COMMIT) != 0) {
      ++redo_log_no_;
      update_last_checkpoint_(quick_checkpoint_);
      if (OB_SUCC(ret)) {
        set_state_(ObSpState::PREPARE);
        int state = ObTransResultState::INVALID;
        if (OB_FAIL(ctx_dependency_wrap_.get_prev_trans_arr_result(state))) {
          TRANS_LOG(WARN, "get prev trans arr result error", K(ret), "context", *this);
        } else if (ObTransResultState::is_commit(state)) {
          end_log_ts_ = timestamp;
          if (OB_FAIL(on_sp_commit_(true))) {
            TRANS_LOG(WARN, "ObPartTransCtx on sp commit error", KR(ret), "context", *this);
          }
        } else if (ObTransResultState::is_abort(state)) {
          end_log_ts_ = timestamp;
          set_status_(OB_TRANS_ROLLBACKED);
          if (OB_FAIL(submit_log_task_(OB_LOG_SP_TRANS_ABORT, has_redo_log))) {
            TRANS_LOG(WARN, "submit sp trans abort log task error", KR(ret), "context", *this);
          }
        } else {
          if (OB_FAIL(partition_service_->inc_pending_elr_count(self_, mt_ctx_, timestamp))) {
            TRANS_LOG(WARN, "failed to inc pending elr count", K(ret), "context", *this);
          } else {
            end_log_ts_for_elr_ = timestamp;
          }

          TRANS_LOG(DEBUG, "prev trans not commit success, current trans need to wait", "context", *this);
        }
      }

      INC_ELR_STATISTIC(with_dependency_trx_count);
      if (OB_FAIL(ret)) {
        is_hazardous_ctx_ = true;
        TRANS_LOG(ERROR, "current ctx status is hazardous", K(*this));
      }
    }
    if ((log_type & OB_LOG_SP_TRANS_COMMIT) != 0 || (log_type & OB_LOG_SP_TRANS_ABORT) != 0) {
      ++redo_log_no_;
      const bool commit = ((log_type & OB_LOG_SP_TRANS_COMMIT) != 0) ? true : false;
      if (commit) {
        // update checkpoint
        update_last_checkpoint_(quick_checkpoint_);
      } else {
        end_trans_callback_(OB_TRANS_KILLED);
      }
      if (OB_SUCCESS == ret) {
        // Handle the large txn asynchronously and the small txn synchronously.
        // We distinguish the large txn and small txn through whether the redo
        // log count is larger than 1.
        if (redo_log_no_ > 1) {
          ret = submit_big_trans_callback_task_(log_type, log_id, timestamp);
        } else {
          if (OB_FAIL(on_sp_commit_(commit, timestamp))) {
            TRANS_LOG(WARN, "ObPartTransCtx on sp commit error", KR(ret), K(commit), "context", *this);
          }
        }
      }

      INC_ELR_STATISTIC(without_dependency_trx_count);
      if (OB_FAIL(ret)) {
        is_hazardous_ctx_ = true;
        TRANS_LOG(ERROR, "current ctx status is hazardous", K(*this));
      }
    }

    if ((log_type & OB_LOG_TRANS_REDO) != 0) {
      //_FILL_TRACE_BUF(tlog_, "sync redo log success");
      // enter 2pc when the first redo log submited success
      has_write_or_replay_mutator_redo_log_ = true;
      if (0 == redo_log_no_++) {
        TRANS_LOG(DEBUG, "participant enter into 2pc", "context", *this, K(log_type), K(timestamp));
      }
      if (redo_log_no_ == 1) {
        // The log is completed, we need verify the txn checksum
        need_checksum_ = true;
      }
      // need submit redo_prepare log when log_type equal OB_LOG_TRANS_REDO
      if (OB_LOG_TRANS_REDO == log_type) {
        // record the redo log id
        if (!is_xa_last_empty_redo_log_() && OB_FAIL(prev_redo_log_ids_.push_back(log_id))) {
          TRANS_LOG(WARN, "redo log id push back error", KR(ret), "context", *this, K(log_id));
        } else if (cluster_version_after_3100_() && need_record_log()) {
          if (OB_FAIL(submit_log_task_(OB_LOG_TRANS_RECORD, has_redo_log))) {
            TRANS_LOG(WARN, "submit trans record log task error", KR(ret), "context", *this);
          }
        } else if (not_need_write_next_log_(log_type)) {
          // No need to write log for dup table in order to prevent the leader
          // revoke from getting stuck
        } else if (OB_FAIL(alloc_local_trans_version_(OB_LOG_TRANS_REDO_WITH_PREPARE))) {
          if (OB_EAGAIN != ret) {
            TRANS_LOG(WARN, "alloc log id and timestamp error", KR(ret), "context", *this);
          } else {
            ret = OB_SUCCESS;
            if (submit_log_count_ > 0) {
              TRANS_LOG(ERROR, "unexpected submit log count or pending count", "context", *this);
              need_print_trace_log_ = true;
            }
            inc_submit_log_pending_count_();
            inc_submit_log_count_();
          }
        } else if (is_xa_local_trans() && in_xa_prepare_state_) {
          // If the XA txn receives the last redo log, we need to post the
          // success of XA prepare request to the coordinator
          if (has_gen_last_redo_log_) {
            if (OB_FAIL(on_prepare_redo_())) {
              TRANS_LOG(WARN, "ObPartTransCtx on_prepare_redo error", KR(ret), "context", *this);
            }
          } else {
            if (OB_FAIL(submit_log_task_(OB_LOG_TRANS_REDO, has_redo_log))) {
              TRANS_LOG(WARN, "submit log task error", KR(ret), "context", *this);
            }
          }
        } else {
          if (OB_FAIL(submit_log_task_(OB_LOG_TRANS_REDO_WITH_PREPARE, has_redo_log))) {
            TRANS_LOG(WARN, "submit log task error", KR(ret), "context", *this);
          } else {
            // do nothing
          }
        }
        if (OB_FAIL(ret)) {
          // Mark the txn need to rollback, and rely on the timer task of
          // coordinator to progress the txn
          set_status_(OB_TRANS_ROLLBACKED);
        }
      } else {
        ret = OB_SUCCESS;
      }
    }
    if ((log_type & OB_LOG_TRANS_PREPARE) != 0) {
      // update checkpoint
      update_last_checkpoint_(quick_checkpoint_);
      prepare_log_id_ = log_id;
      prepare_log_timestamp_ = timestamp;
      // We need record the information into partition_log_info_arr for single
      // partition txn inorder to rebuild context when coordinator shut down
      if (is_single_partition_() && !is_xa_local_trans()) {
        ObPartitionLogInfo partition_log_info(self_, prepare_log_id_, prepare_log_timestamp_);
        if (OB_FAIL(partition_log_info_arr_.push_back(partition_log_info))) {
          TRANS_LOG(WARN, "partition logid array push back error", KR(ret), K(partition_log_info));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(on_prepare_(batch_committed, timestamp))) {
          TRANS_LOG(WARN, "ObPartTransCtx on_prepare error", KR(ret), "context", *this);
        }
      } else {
        is_hazardous_ctx_ = true;
        TRANS_LOG(ERROR, "current ctx status is hazardous", K(*this));
      }
    }
    // ignore ret
    if ((log_type & OB_LOG_TRANS_COMMIT) != 0) {
      if (is_listener_) {
        if (OB_ISNULL(listener_handler_)) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "listen handler is null", KR(ret), K(*this));
        } else if (OB_FAIL(listener_handler_->on_listener_commit())) {
          TRANS_LOG(WARN, "on_listener_commit failed", KR(ret));
        }
      } else {
        start_us = ObTimeUtility::fast_current_time();
        // Use commit log timestamp to update local clear_log_base_ts
        clear_log_base_ts_ = timestamp;

        // Handle the large txn asynchronously and the small txn synchronously.
        // We distinguish the large txn and small txn through whether the redo
        // log count is larger than 1
        if (redo_log_no_ > 1) {
          ret = submit_big_trans_callback_task_(log_type, log_id, timestamp);
        } else {
          if (OB_FAIL(on_dist_commit_())) {
            TRANS_LOG(WARN, "ObPartTransCtx on commit error", KR(ret), "context", *this);
          }
        }
        if (OB_FAIL(ret)) {
          is_hazardous_ctx_ = true;
          TRANS_LOG(ERROR, "current ctx status is hazardous", K(*this));
        }
      }
    }
    // ignore ret
    if ((log_type & OB_LOG_TRANS_ABORT) != 0) {
      if (is_listener_) {
        if (OB_ISNULL(listener_handler_)) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "listen handler is null", KR(ret), K(*this));
        } else if (OB_FAIL(listener_handler_->on_listener_abort())) {
          TRANS_LOG(WARN, "on_listener_abort failed", KR(ret));
        }
      } else {
        start_us = ObTimeUtility::fast_current_time();
        // Asynchronous processing of large transactions
        if (redo_log_no_ > 1) {
          ret = submit_big_trans_callback_task_(log_type, log_id, timestamp);
        } else {
          // Synchronization of small transactions
          if (OB_FAIL(on_dist_abort_())) {
            TRANS_LOG(WARN, "ObPartTransCtx on_abort error", KR(ret), "context", *this);
          }
        }
      }
    }
    // ignore ret
    if ((log_type & OB_LOG_TRANS_CLEAR) != 0) {
      // no listener or all listeners have replied
      if (OB_NOT_NULL(listener_handler_)) {
        listener_handler_->set_commit_log_synced(true);
      }
      if (OB_ISNULL(listener_handler_) || listener_handler_->is_listener_ready()) {
        if (OB_FAIL(on_clear_(true))) {
          TRANS_LOG(WARN, "ObPartTransCtx on_clear error", KR(ret), "context", *this);
        }
      }
    }
    if ((OB_LOG_MUTATOR & log_type) != 0) {
      ObStorageLogType submit_log_type = ObStorageLogType::OB_LOG_UNKNOWN;
      has_write_or_replay_mutator_redo_log_ = true;
      mutator_log_no_++;
      if (mutator_log_no_ == 1) {
        // The log is completed, we need verify the txn checksum
        need_checksum_ = true;
      }
      start_us = ObTimeUtility::fast_current_time();
      if (OB_FAIL(prev_redo_log_ids_.push_back(log_id))) {
        TRANS_LOG(WARN, "redo log id push back error", KR(ret), "context", *this, K(log_id));
      } else if ((OB_LOG_TRANS_STATE & log_type) != 0) {
        // do nothing
      } else if (cluster_version_after_3100_() && need_record_log()) {
        if (OB_FAIL(submit_log_task_(OB_LOG_TRANS_RECORD, has_redo_log))) {
          TRANS_LOG(WARN, "submit trans record log task error", KR(ret), "context", *this);
        }
      } else if (batch_commit_trans_) {
        // do nothing
      } else if (OB_FAIL(decide_and_submit_next_log_(submit_log_type, has_redo_log, has_pending_cb))) {
          TRANS_LOG(ERROR, "decide and submit next log failed", KR(ret), "context", *this);
      } else {
        // do nothing
      }
      if (OB_FAIL(ret)) {
        // Mark the txn need to rollback, and rely on the timer task of
        // coordinator to progress the txn
        set_status_(OB_TRANS_ROLLBACKED);
      }
      TRANS_LOG(DEBUG, "sync transaction mutator log", KR(ret), "context", *this, K(submit_log_type));
    }
    if ((OB_LOG_TRANS_STATE & log_type) != 0) {
      is_trans_state_sync_finished_ = true;
      has_trans_state_log_ = true;
      TRANS_LOG(INFO, "sync transaction state log success", "context", *this);
    }
    if (OB_LOG_MUTATOR_ABORT == log_type) {
      (void)unregister_timeout_task_();
      set_exiting_();
      const bool commit = false;
      set_state_(ObRunningState::ABORT);
      (void)trans_end_(commit, get_global_trans_version_());
      (void)trans_clear_();
      end_trans_callback_(OB_TRANS_KILLED);
      ObTransStatus trans_status(trans_id_, OB_TRANS_KILLED);
      (void)trans_status_mgr_->set_status(trans_id_, trans_status);
      TRANS_LOG(INFO, "sync transaction mutator abort log", KR(ret), "context", *this);
    }
  }

  dup_table_syncing_log_id_ = UINT64_MAX;
  dup_table_syncing_log_ts_ = INT64_MAX;
  end_us = ObTimeUtility::fast_current_time();

  if (end_us - start_us > 30 * 1000) {
    TRANS_LOG(WARN,
        "on_sync_log_success use too much time",
        KR(ret),
        "total_used",
        end_us - start_us,
        K(log_type),
        K(log_id),
        K(timestamp),
        "context",
        *this);
  }

  // If the callback process fails, we need revoke the partition leader except for OB_NOT_MASTER
  if (OB_FAIL(ret) && OB_NOT_MASTER != ret) {
    (void)partition_service_->async_leader_revoke(self_, ObElection::RevokeType::TRANS_CB_ERROR);
  }

  REC_TRANS_TRACE_EXT(tlog_, on_sync_log_succ, OB_ID(ret), ret, Y(log_type), Y(log_id), OB_ID(uref), get_uref());
  return ret;
}

int ObPartTransCtx::submit_big_trans_callback_task_(
    const int64_t log_type, const int64_t log_id, const int64_t timestamp)
{
  int ret = OB_SUCCESS;

  async_applying_log_ts_ = timestamp;

  if (OB_FAIL(big_trans_worker_->submit_big_trans_callback_task(self_, log_type, log_id, timestamp, this))) {
    TRANS_LOG(WARN, "submit big trans callback task error", KR(ret), K(log_id), K(timestamp), "context", *this);
  } else {
    inc_submit_log_count_();
    inc_submit_log_pending_count_();
    enable_new_1pc_ = false;
  }

  return ret;
}

int64_t ObPartTransCtx::get_applying_log_ts() const
{
  return MIN(ATOMIC_LOAD(&async_applying_log_ts_), ATOMIC_LOAD(&dup_table_syncing_log_ts_));
}

int ObPartTransCtx::revert_self_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(partition_mgr_)) {
    TRANS_LOG(ERROR, "partition mgr is null, unexpected error", "context", *this, KP_(partition_mgr));
    ret = OB_ERR_UNEXPECTED;
  } else {
    (void)partition_mgr_->release_ctx_ref(this);
  }
  return ret;
}

// The lock is acquired during invoking on_submit_log_success
int ObPartTransCtx::on_submit_log_success(
    const bool with_need_update_version, const uint64_t cur_log_id, const int64_t cur_log_timestamp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_valid_log_id(cur_log_id)) || OB_UNLIKELY(0 > cur_log_timestamp)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(cur_log_id), K(cur_log_timestamp));
  } else {
    // The max_submit_log_ts is necessary for MUTATOR_WITH_STATE to set
    // prepare_version when leader transfer
    update_max_submitted_log_timestamp_(cur_log_timestamp);
    if (with_need_update_version) {
      state_.set_prepare_version(cur_log_timestamp);
      global_trans_version_ = cur_log_timestamp;
      if (!is_prepared_ && !in_xa_prepare_state_) {
        mt_ctx_.set_prepare_version(state_.get_prepare_version());
        is_prepared_ = true;
      }
    }
    dec_submit_log_pending_count_();
    REC_TRANS_TRACE_EXT(tlog_,
        on_submit_log_succ_cb,
        OB_ID(ret),
        ret,
        OB_ID(logging),
        submit_log_count_,
        OB_ID(pending),
        submit_log_pending_count_,
        OB_ID(uref),
        get_uref());
  }
  return ret;
}

// The lock is acquired during invoking on_submit_log_fail
int ObPartTransCtx::on_submit_log_fail(const int retcode)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else {
    TRANS_LOG(WARN, "submit log fail", "context", *this, K(retcode));
    if (ObStorageLogTypeChecker::has_trans_mutator(submit_log_cb_.get_log_type())) {
      mt_ctx_.undo_fill_redo_log();
    }
    dec_submit_log_pending_count_();
    dec_submit_log_count_();

    // If the log submission fails, we need revoke the partition leader except for OB_NOT_MASTER
    if (OB_NOT_MASTER != retcode) {
      (void)partition_service_->async_leader_revoke(self_, ObElection::RevokeType::TRANS_CB_ERROR);
    }
    REC_TRANS_TRACE_EXT(tlog_,
        on_submit_log_fail_cb,
        OB_ID(ret),
        ret,
        OB_ID(logging),
        submit_log_count_,
        OB_ID(pending),
        submit_log_pending_count_,
        OB_ID(uref),
        get_uref());
  }
  return ret;
}

int ObPartTransCtx::compensate_prepare_no_log_()
{
  int ret = OB_SUCCESS;
  int64_t log_type = storage::OB_LOG_UNKNOWN;
  bool has_redo_log = false;

  common::ObTimeGuard timeguard("compensate_prepare_no_log", 10 * 1000);
  // avoid compensate log mistakenly
  if (OB_UNLIKELY(Ob2PCState::INIT != get_state_())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected state", KR(ret), "context", *this);
    need_print_trace_log_ = true;
  } else if (OB_UNLIKELY(redo_log_no_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "redo log num less or equal 0", KR(ret), "context", *this);
    need_print_trace_log_ = true;
  } else if (OB_UNLIKELY(is_hazardous_ctx_)) {
    TRANS_LOG(ERROR, "current ctx status is hazardous", K(ret), K(*this));
  } else {
    set_status_(OB_TRANS_ROLLBACKED);
    if (is_sp_trans_()) {
      log_type = OB_LOG_SP_TRANS_ABORT;
      if (OB_FAIL(submit_log_task_(log_type, has_redo_log))) {
        TRANS_LOG(WARN, "submit prepare log task error", KR(ret), K(log_type), K_(trans_id));
      }
    } else {
      log_type = OB_LOG_TRANS_PREPARE;
      if (!stc_.is_valid()) {
        // maybe crashed and recovered from trans status table, stc is not set.		
        // under such condition, stc is set to current time		
        stc_ = MonotonicTs::current_time();
      }
      if (OB_FAIL(alloc_local_trans_version_(log_type))) {
        if (OB_EAGAIN != ret) {
          TRANS_LOG(WARN, "alloc log id and timestamp error", KR(ret), "context", *this);
        } else {
          ret = OB_SUCCESS;
          inc_submit_log_pending_count_();
          inc_submit_log_count_();
        }
      } else if (OB_FAIL(submit_log_task_(log_type, has_redo_log))) {
        TRANS_LOG(WARN, "submit prepare log task error", KR(ret), K(log_type), K_(trans_id));
      } else {
        // do nothing
      }
    }
    if (OB_FAIL(ret)) {
      TRANS_LOG(WARN, "compensate log error", KR(ret), K(log_type), "context", *this);
    } else {
      TRANS_LOG(INFO, "compensate log success", K(log_type), "context", *this);
    }
  }

  REC_TRANS_TRACE_EXT(
      tlog_, compensate_prepare_no_log, OB_ID(ret), ret, OB_ID(used), timeguard.get_diff(), OB_ID(uref), get_uref());
  return ret;
}

int ObPartTransCtx::compensate_mutator_abort_log_()
{
  int ret = OB_SUCCESS;
  const int64_t log_type = OB_LOG_MUTATOR_ABORT;
  bool has_redo_log = false;

  common::ObTimeGuard timeguard("compensate_prepare_no_log", 10 * 1000);
  // avoid compensate log mistakenly
  if (OB_UNLIKELY(Ob2PCState::INIT != get_state_())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected state", KR(ret), "context", *this);
    need_print_trace_log_ = true;
  } else if (OB_UNLIKELY(mutator_log_no_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "mutator log num less 0", KR(ret), "context", *this);
    need_print_trace_log_ = true;
  } else if (OB_UNLIKELY(is_hazardous_ctx_)) {
    TRANS_LOG(ERROR, "current ctx status is hazardous", K(ret), K(*this));
  } else if (is_logging_()) {
    part_trans_action_ = ObPartTransAction::DIED;
    TRANS_LOG(WARN, "in logging when compensate mutator abort", K(ret), "context", *this);
  } else {
    set_status_(OB_TRANS_ROLLBACKED);
    if (OB_FAIL(submit_log_task_(log_type, has_redo_log))) {
      TRANS_LOG(WARN, "submit mutator abort log error", KR(ret), K(log_type), K_(trans_id));
    }
    if (OB_FAIL(ret)) {
      TRANS_LOG(WARN, "compensate mutator abort log error", KR(ret), K(log_type), "context", *this);
    } else {
      TRANS_LOG(INFO, "compensate mutator abort log success", K(log_type), "context", *this);
    }
  }

  REC_TRANS_TRACE_EXT(
      tlog_, compensate_prepare_no_log, OB_ID(ret), ret, OB_ID(used), timeguard.get_diff(), OB_ID(uref), get_uref());
  return ret;
}

bool ObPartTransCtx::in_pending_state_()
{
  bool bool_ret = false;
  const int64_t state = get_state_();
  if (is_sp_trans_()) {
    if (state < ObSpState::COMMIT) {
      bool_ret = true;
    }
  } else {
    if (state < Ob2PCState::COMMIT) {
      bool_ret = true;
    }
  }
  return bool_ret;
}

bool ObPartTransCtx::has_write_data_()
{
  return redo_log_no_ + mutator_log_no_ > 0;
}

bool ObPartTransCtx::has_trans_version_()
{
  bool bool_ret = false;
  const int64_t state = get_state_();
  if (redo_log_no_ > 0) {
    bool_ret = true;
  } else if (is_sp_trans_()) {
    bool_ret = state >= ObSpState::PREPARE;
  } else {
    bool_ret = state >= Ob2PCState::PREPARE;
  }
  return bool_ret;
}

int ObPartTransCtx::get_trans_state(int64_t& state)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(lock_.try_lock())) {
    if (OB_EAGAIN == ret) {
      ret = OB_SUCCESS;
    } else {
      TRANS_LOG(ERROR, "try lock error, unexpected error", K(ret), K(state), K(*this));
    }
  } else {
    CtxLockGuard guard(lock_, false);
    state = get_state_();
  }
  return ret;
}

// Register successor for all predecessors
int ObPartTransCtx::insert_all_prev_trans_(const bool for_replaying)
{
  int ret = OB_SUCCESS;
  const bool for_replay = false;
  const bool need_completed_dirty_txn = false;
  bool alloc = false;
  ObTransCtx* prev_trans_ctx = NULL;
  ObElrPrevTransArrIterator iterator(ctx_dependency_wrap_);
  ObElrTransInfo* info = NULL;

  while (OB_SUCC(ret) && (NULL != (info = iterator.next()))) {
    const ObTransID& trans_id = info->get_trans_id();
    // If the predecessor's state is decided, it means the predecessor has
    // invoked the callback, and no dependency register is needed.
    if (ObTransResultState::is_decided_state(info->get_result())) {
      TRANS_LOG(INFO, "cur prev transaction state decided, no need to register", K(*info), "context", *this);
    } else if (OB_FAIL(partition_mgr_->get_trans_ctx_(trans_id,
                   for_replay,
                   is_readonly_,
                   trans_param_.is_bounded_staleness_read(),
                   need_completed_dirty_txn,
                   alloc,
                   prev_trans_ctx))) {
      int state = ObTransResultState::INVALID;
      int result = ObTransResultState::INVALID;
      if (for_replaying) {
        // It is possible that the predecessor txn has not replayed
        ret = OB_SUCCESS;
        TRANS_LOG(INFO, "prev transaction not exist, no need to register", K(*info), "context", *this);
      } else if (OB_TRANS_CTX_NOT_EXIST != ret) {
        set_status_(OB_TRANS_ROLLBACKED);
        need_print_trace_log_ = true;
        ret = OB_SUCCESS;
        TRANS_LOG(WARN, "get transaction context error", KR(ret), K(trans_id), "context", *this);
        break;
        // The context doesnot exist, we need check whether the txn is finished
        // from trans_result_info_mgr
      } else if (OB_FAIL(partition_mgr_->get_state_in_TRIM(trans_id, state))) {
        TRANS_LOG(WARN, "get state in trans result info mgr error", KR(ret), K(trans_id), "context", *this);
        set_status_(OB_TRANS_ROLLBACKED);
        need_print_trace_log_ = true;
        ret = OB_SUCCESS;
        break;
        // The predecessor txn has fnished, so we need mark the predecessor txn state
      } else if (OB_FAIL(ctx_dependency_wrap_.mark_prev_trans_result(trans_id, state, result))) {
        TRANS_LOG(WARN, "mark prev trans result error", KR(ret), K(trans_id), K(state), K(*this));
      } else {
        // do nothing
      }
    } else {
      const uint32_t ctx_id = static_cast<ObPartTransCtx*>(prev_trans_ctx)->get_ctx_id();
      if (0 == ctx_id) {
        if (!for_replaying) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "unexpected ctx id", K(ctx_id), K(*this));
        } else {
          TRANS_LOG(INFO, "ctx id not ready", K(ctx_id), K(*this));
        }
      } else if (OB_FAIL(ctx_dependency_wrap_.register_dependency_item(ctx_id, prev_trans_ctx))) {
        // When registering the predecessor txn, the predecessor txn may invoke
        // the callback before. While the current txn has never registered the
        // dependency before, and therefore there is no chance to fetch the
        // predecessor information
        int64_t state = Ob2PCState::INIT;
        int result = ObTransResultState::INVALID;
        if (OB_EAGAIN != ret) {
          TRANS_LOG(WARN, "insert prev trans error", K(ret), "context", *this);
        } else if (OB_FAIL(static_cast<ObPartTransCtx*>(prev_trans_ctx)->get_trans_state(state))) {
          TRANS_LOG(WARN, "get trans state error", K(ret), "prev_trans_id", prev_trans_ctx->get_trans_id());
        } else {
          if (Ob2PCState::ABORT == state || ObSpState::ABORT == state) {
            if (OB_FAIL(ctx_dependency_wrap_.mark_prev_trans_result(trans_id, ObTransResultState::ABORT, result))) {
              TRANS_LOG(WARN, "mark prev trans result error", K(ret), K(trans_id), K(state), K(*this));
            }
          } else if (Ob2PCState::COMMIT == state || ObSpState::COMMIT == state) {
            if (OB_FAIL(ctx_dependency_wrap_.mark_prev_trans_result(trans_id, ObTransResultState::COMMIT, result))) {
              TRANS_LOG(WARN, "mark prev trans result error", K(ret), K(trans_id), K(state), K(*this));
            }
          } else {
            // do nothing
          }
        }
      } else {
        // do nothing
      }
      (void)partition_mgr_->revert_trans_ctx_(prev_trans_ctx);
    }
  }

  return ret;
}

// 1.Register subsequent information for the forward transaction
// 2.Refresh the status of the predecessor transaction
int ObPartTransCtx::leader_takeover(const int64_t checkpoint)
{
  int ret = OB_SUCCESS;
  need_print_trace_log_ = true;
  common::ObTimeGuard timeguard("leader_takeover", 10 * 1000);
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (is_readonly_ || is_exiting_) {
    // do nothing
  } else if (OB_ISNULL(partition_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "partition mgr is null, unexpected error", KR(ret), KP_(partition_mgr), "context", *this);
  } else if (OB_UNLIKELY(0 > redo_log_no_) || OB_UNLIKELY(0 > mutator_log_no_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected log no", KR(ret), K_(redo_log_no), K_(mutator_log_no));
  } else {
    timeguard.click();
    // NB: To guarantee the checkpoint_ function is invoked on follower, the
    // checkpoint_ function should be placed before replay_to_commit
    if (OB_SUCC(ret) && checkpoint > 0) {
      bool checkpoint_succ = false;
      if (OB_FAIL(checkpoint_(checkpoint, 0, checkpoint_succ))) {
        TRANS_LOG(WARN, "checkpoint transaction error", KR(ret), K(checkpoint), "context", *this);
        need_print_trace_log_ = true;
      } else {
        TRANS_LOG(INFO, "checkpoint transaction when leader takeover", KR(ret), K(checkpoint), "context", *this);
      }
    }
    timeguard.click();
    // Bypass the following steps when checkpoint has been invoked
    if (!is_exiting_ && OB_SUCC(ret)) {
      if (!is_in_2pc_() || in_xa_prepare_state_) {
        // The trans nodes of txn that has not in 2pc should be backfill with
        // INT64_MAX. Otherwise following read will be blocked by the trans
        // version on the trans_node(the logici of lock_for_read)
        mt_ctx_.set_prepare_version(INT64_MAX);
      }
      if (OB_FAIL(mt_ctx_.replay_to_commit())) {
        TRANS_LOG(WARN, "replay to commit error", KR(ret), "context", *this);
      } else if (OB_FAIL(insert_all_prev_trans_(false))) {
        TRANS_LOG(WARN, "insert all prev trans error", KR(ret), "context", *this);
        // If the register fails, we can reply on trans_result_info to obtain the txn state later
        ret = OB_SUCCESS;
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObPartTransCtx::leader_active(const LeaderActiveArg& arg)
{
  int ret = OB_SUCCESS;

  need_print_trace_log_ = true;
  common::ObTimeGuard timeguard("leader_active", 10 * 1000);
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);
  timeguard.click();
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(DEBUG, "transaction is exiting", "context", *this);
  } else if (is_readonly_) {
    TRANS_LOG(INFO, "transaction is readonly", "context", *this);
  } else if (OB_UNLIKELY(0 > redo_log_no_) || OB_UNLIKELY(0 > mutator_log_no_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected log no", K(ret), K_(redo_log_no), K_(mutator_log_no));
  } else {
    timeguard.click();
    bool need_register_timer_task = true;
    int state = ObTransResultState::INVALID;
    if (OB_SUCC(ret) && OB_SUCCESS == get_status_()) {
      if (OB_FAIL(ctx_dependency_wrap_.get_prev_trans_arr_result(state))) {
        TRANS_LOG(WARN, "get prev trans arr result error", KR(ret), "context", *this);
      } else if (ObTransResultState::is_abort(state)) {
        set_status_(OB_TRANS_ROLLBACKED);
        need_print_trace_log_ = true;
        TRANS_LOG(WARN, "current transaction need to rollback because of prev abort trans", "context", *this);
      } else {
        // do nothing
      }
    }
    timeguard.click();
    if (OB_SUCC(ret)) {
      if (Ob2PCState::INIT == get_state_()) {
        if (redo_log_no_ > 0) {
          if (!(is_xa_local_trans() && is_redo_prepared_) && OB_FAIL(compensate_prepare_no_log_())) {
            TRANS_LOG(WARN, "compensate prepare no log error", KR(ret), "context", *this);
          }
        } else if (mutator_log_no_ > 0 || has_trans_state_log_) {
          if (!arg.is_elected_by_changing_leader_ || !is_trans_state_sync_finished_) {
            set_status_(OB_TRANS_NEED_ROLLBACK);
          }
        } else {
          set_exiting_();
          TRANS_LOG(INFO, "no redo log and mutator log", KR(ret), "context", *this);
          need_print_trace_log_ = true;
        }
      } else if (ObSpState::PREPARE == get_state_()) {
        // The single-partitioned txn is in Sp::PREPARE, while status is not
        // OB_SUCCESS, which means the predecessors have aborted txns
        bool UNUSED = false;
        if (ObTransResultState::is_commit(state)) {
          if (OB_FAIL(on_sp_commit_(true))) {
            TRANS_LOG(WARN, "sp trans on commit error", KR(ret), "context", *this);
          }
          // The predecessor has rollbacked, and ths status has been set up,
          // which means an ABORT log is necessary
        } else if (ObTransResultState::is_abort(state) || OB_SUCCESS != get_status_()) {
          if (OB_FAIL(submit_log_task_(OB_LOG_SP_TRANS_ABORT, UNUSED))) {
            TRANS_LOG(WARN, "submit sp trans abort log task error", KR(ret), "context", *this);
          }
        } else {
          // Predecessors have undecided txns, so cannot decide here. We reply
          // on the timer to drive the state
        }
      } else {
        // do nothing
      }
    }
    timeguard.click();

    // The txn with is_exiting_ = true only need to maintain the state
    if (OB_SUCC(ret) && !is_exiting_) {
      for_replay_ = false;
      is_trans_state_sync_finished_ = false;
      is_changing_leader_ = false;
      prepare_changing_leader_state_ = CHANGING_LEADER_STATE::NO_CHANGING_LEADER;
      update_max_submitted_log_timestamp_(max_durable_log_ts_);
      if (need_register_timer_task) {
        // The request_id_ should be initialized to prevent the 2pc cannot be
        // driven if all participants transferring the leader
        generate_request_id_();
        trans_2pc_timeout_ = ObServerConfig::get_instance().trx_2pc_retry_interval;
        if (Ob2PCState::INIT == get_state_()) {
          const int64_t left_time = trans_expired_time_ - ObClockGenerator::getRealClock();
          if (left_time > 0) {
            trans_2pc_timeout_ = left_time;
          } else {
            trans_2pc_timeout_ = std::min(trans_2pc_timeout_, (int64_t)MAX_TRANS_2PC_TIMEOUT_US);
          }
          // The XA txn has replayed the last redo log
          if (is_xa_local_trans() && is_redo_prepared_) {
            trans_2pc_timeout_ = std::min(trans_2pc_timeout_, (int64_t)MAX_TRANS_2PC_TIMEOUT_US);
          }
        } else {
          trans_2pc_timeout_ = std::min(trans_2pc_timeout_, (int64_t)MAX_TRANS_2PC_TIMEOUT_US);
        }
        // do not post transaction message to avoid deadlock, bug#8257026
        // just register timeout task
        (void)unregister_timeout_task_();
        if (OB_FAIL(register_timeout_task_(trans_2pc_timeout_))) {
          TRANS_LOG(WARN, "register timeout handler error", KR(ret), "context", *this);
        }
      }
    }
    timeguard.click();
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "participant leader active error", KR(ret), "context", *this);
  } else {
    TRANS_LOG(INFO, "participant leader active success", "context", *this);
  }
  REC_TRANS_TRACE_EXT(
      tlog_, leader_active, OB_ID(ret), ret, OB_ID(used), timeguard.get_diff(), OB_ID(uref), get_uref());
  return ret;
}

bool ObPartTransCtx::can_be_freezed() const
{
  CtxLockGuard guard(lock_);
  return (is_readonly_ || ((!has_logged_()) && is_exiting_));
}

int ObPartTransCtx::do_sp_trans_rollback_()
{
  int ret = OB_SUCCESS;

  if (is_logging_()) {
    TRANS_LOG(INFO, "transaction is logging when sp rollback", "context", *this);
  } else if (has_logged_()) {
    bool has_redo_log = false;
    if (OB_FAIL(submit_log_task_(OB_LOG_SP_TRANS_ABORT, has_redo_log))) {
      TRANS_LOG(WARN, "submit mutator abort log error", KR(ret), K_(trans_id));
    } else {
      TRANS_LOG(INFO, "transaction has logged when sp rollback", "context", *this);
    }
  } else if (OB_FAIL(do_prepare_(OB_TRANS_ROLLBACKED))) {
    TRANS_LOG(WARN, "do prepare error", KR(ret), "context", *this);
  } else if (OB_FAIL(do_abort_())) {
    TRANS_LOG(WARN, "do abort error", KR(ret), "context", *this);
  } else if (OB_FAIL(do_clear_())) {
    TRANS_LOG(WARN, "do clear error", KR(ret), "context", *this);
  } else {
    set_state_(ObSpState::ABORT);
    (void)unregister_timeout_task_();
    TRANS_STAT_ABORT_TRANS_TIME(tenant_id_);
    TRANS_STAT_TOTAL_USED_TIME(tenant_id_, ctx_create_time_);
    set_exiting_();
  }

  return ret;
}

int ObPartTransCtx::kill_trans(bool& need_convert_to_dist_trans)
{
  TRANS_LOG(DEBUG, "part_ctx kill_trans");
  int ret = OB_SUCCESS;
  need_convert_to_dist_trans = false;
  bool is_split_partition = false;
  if (OB_FAIL(lock_.try_lock())) {
    TRANS_LOG(WARN, "try lock error", K(ret), K(*this));
  } else {
    CtxTransTableLockGuard guard(lock_, trans_table_seqlock_, false);
    part_trans_action_ = ObPartTransAction::ABORT;
    if (IS_NOT_INIT) {
      TRANS_LOG(WARN, "ObPartTransCtx not inited");
      ret = OB_NOT_INIT;
    } else if (OB_UNLIKELY(is_changing_leader_) || OB_UNLIKELY(has_trans_state_log_)) {
      ret = OB_NOT_MASTER;
      TRANS_LOG(WARN, "transaction is preparing changing leader", KR(ret), "context", *this);
      need_convert_to_dist_trans = true;
    } else if (OB_UNLIKELY(is_exiting_)) {
      TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    } else if (OB_UNLIKELY(for_replay_)) {
      ret = OB_NOT_MASTER;
      TRANS_LOG(WARN, "transaction is replaying", KR(ret), "context", *this);
      need_print_trace_log_ = true;
      need_convert_to_dist_trans = true;
    } else if (OB_FAIL(check_cur_partition_split_(is_split_partition))) {
      TRANS_LOG(WARN, "check current partition split error", K(ret), K(*this));
    } else if (OB_UNLIKELY(is_split_partition)) {
      ret = OB_EAGAIN;
      TRANS_LOG(WARN, "current partition is split partition, need to convert distributed trx", K(ret), K(*this));
      need_convert_to_dist_trans = true;
    } else if (OB_FAIL(do_sp_trans_rollback_())) {
      TRANS_LOG(WARN, "do sp trans rollback error", K(ret), K(*this));
    }
  }
  return ret;
}

int ObPartTransCtx::commit(const bool is_rollback, sql::ObIEndTransCallback* cb, const bool is_readonly,
    const MonotonicTs commit_time, const int64_t stmt_expired_time, const ObStmtRollbackInfo& stmt_rollback_info,
    const common::ObString& app_trace_info, bool& need_convert_to_dist_trans)
{
  UNUSED(stmt_rollback_info);
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t usec_per_sec = 1000 * 1000;
  need_convert_to_dist_trans = false;
  bool is_split_partition = false;

  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  part_trans_action_ = is_rollback ? ObPartTransAction::ABORT : ObPartTransAction::COMMIT;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(cb) || OB_UNLIKELY(!commit_time.is_valid()) || OB_UNLIKELY(0 >= stmt_expired_time)) {
    TRANS_LOG(WARN, "invalid argument", KP(cb), K(commit_time), K(stmt_expired_time));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(is_changing_leader_) || OB_UNLIKELY(has_trans_state_log_)) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "transaction is preparing changing leader", KR(ret), "context", *this);
    need_print_trace_log_ = true;
    need_convert_to_dist_trans = true;
  } else if (OB_UNLIKELY(is_exiting_)) {
    if (is_rollback) {
      cb->callback(OB_SUCCESS);
      cb = NULL;
    } else {
      ret = OB_TRANS_IS_EXITING;
    }
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
  } else if (OB_UNLIKELY(for_replay_)) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "transaction is replaying", KR(ret), "context", *this);
    need_print_trace_log_ = true;
    need_convert_to_dist_trans = true;
  } else if (OB_FAIL(check_cur_partition_split_(is_split_partition))) {
    TRANS_LOG(WARN, "check current partition split error", K(ret), K(*this));
  } else if (OB_UNLIKELY(is_split_partition)) {
    ret = OB_EAGAIN;
    TRANS_LOG(WARN, "current partition is split partition, need to convert distributed trx", K(ret), K(*this));
    need_convert_to_dist_trans = true;
  } else if (OB_UNLIKELY(!stmt_info_.is_task_match() && !is_rollback)) {
    sp_user_request_ = (is_rollback ? USER_ABORT : USER_COMMIT);
    tmp_ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "commit transaction but task not match", K(tmp_ret), K_(stmt_info), K(*this));
    if (OB_FAIL(do_sp_trans_rollback_())) {
      TRANS_LOG(WARN, "do sp trans rollback error", K(ret), K(is_rollback), K(*this));
    } else {
      cb->callback(OB_TRANS_ROLLBACKED);
      cb = NULL;
    }
  } else if (OB_SUCCESS != get_status_() && !is_rollback) {
    sp_user_request_ = (is_rollback ? USER_ABORT : USER_COMMIT);
    if (OB_FAIL(do_sp_trans_rollback_())) {
      TRANS_LOG(WARN, "do sp trans rollback error", K(ret), K(is_rollback), K(*this));
    } else {
      TRANS_LOG(INFO, "rollback sp trans when commit", K(ret), K(get_status_()), K(*this));
      cb->callback(get_status_());
      cb = NULL;
    }
  } else {
    sp_user_request_ = (is_rollback ? USER_ABORT : USER_COMMIT);
    commit_start_time_ = ObClockGenerator::getClock();
    if (is_rollback) {
      if (OB_FAIL(do_sp_trans_rollback_())) {
        TRANS_LOG(WARN, "do sp trans rollback error", K(ret), K(is_rollback), K(*this));
      } else {
        cb->callback(OB_SUCCESS);
        cb = NULL;
        TRANS_STAT_COMMIT_ABORT_TRANS_TIME(is_rollback, tenant_id_);
        TRANS_STAT_TOTAL_USED_TIME(tenant_id_, ctx_create_time_);
      }
    } else if (is_readonly_ && ctx_dependency_wrap_.get_prev_trans_arr_count() <= 0) {
      // The readonly txn can reply to the client with commit ok and clear the
      // memtable context due to no predecessors. Otherwise, we need wait for
      // the predecessors
      if (OB_FAIL(do_clear_())) {
        TRANS_LOG(WARN, "do clear error", K(ret), "context", *this);
      } else {
        cb->callback(OB_SUCCESS);
        cb = NULL;
        TRANS_STAT_COMMIT_ABORT_TRANS_TIME(is_rollback, tenant_id_);
        TRANS_STAT_TOTAL_USED_TIME(tenant_id_, ctx_create_time_);
      }
    } else {
      bool has_redo_log = false;
      set_stc_(commit_time);
      stmt_expired_time_ = stmt_expired_time;
      ObStorageLogType log_type = ObStorageLogType::OB_LOG_UNKNOWN;
      // Check the predecessors state, generate the necessary log_type.
      // 1. If log_type = OB_LOG_SP_TRANS_ABORT, it means the predecessors have aborted txns.
      // 2. If log_type = OB_LOG_SP_TRANS_COMMIT, it means all predecessors are decided and committed or there exists no
      // predecessors
      // 3. If log_type = OB_LOG_SP_ELR_TRANS_COMMIT, it means some predecessors' states havenot been decided
      //   - If the txn shouldnot keep the dependency with the predecessors, rollback the txn immediately
      //   - If the dependecy is necessary, write the OB_LOG_SP_ELR_TRANS_COMMIT
      //   - Current implementation write the OB_LOG_SP_ELR_TRANS_COMMIT if the predecessors exit
      const int64_t tmp_config = ObServerConfig::get_instance().trx_2pc_retry_interval;
      if (OB_FAIL(set_app_trace_info_(app_trace_info))) {
        TRANS_LOG(WARN, "set app trace info error", K(ret), K(app_trace_info), K(*this));
      } else if (OB_FAIL(generate_sp_commit_log_type_(log_type))) {
        TRANS_LOG(WARN, "generate sp commit log type error", K(ret), "context", *this);
      } else if (OB_FAIL(unregister_timeout_task_())) {
        TRANS_LOG(WARN, "unregister timeout handler error", K(ret), "context", *this);
      } else if (OB_FAIL(register_timeout_task_(
                     std::min(tmp_config, (int64_t)MAX_TRANS_2PC_TIMEOUT_US) + trans_id_.hash() % usec_per_sec))) {
        TRANS_LOG(WARN, "register timeout handler error", KR(ret), "context", *this);
      } else if (OB_FAIL(submit_log_async_(log_type, has_redo_log))) {
        TRANS_LOG(WARN, "submit sp log error", KR(ret), "context", *this);
      } else if (OB_FAIL(end_trans_cb_.init(tenant_id_, cb))) {
        TRANS_LOG(WARN, "end trans callback init error", KR(ret));
      } else if (!has_redo_log && 0 == submit_log_count_) {
        // If there exits the predecessors and the txn is not decided, the
        // single-partitioned txn should wait for the predecessors before reply
        if (OB_LOG_SP_ELR_TRANS_COMMIT == log_type) {
          TRANS_LOG(INFO, "no log, but need to wait prev trans result", "context", *this);
          ObITsMgr* ts_mgr = get_ts_mgr_();
          int64_t tmp_gts = 0;
          if (OB_FAIL(ts_mgr->get_gts(self_.get_tenant_id(), NULL, tmp_gts))) {
            if (OB_EAGAIN != ret) {
              TRANS_LOG(WARN, "get gts error", KR(ret), "context", *this);
            } else {
              ret = OB_TRANS_ROLLBACKED;
            }
          } else {
            global_trans_version_ = tmp_gts;
          }
          set_state_(ObSpState::PREPARE);
          if (OB_SUCCESS != ret) {
            end_trans_cb_.reset();
          }
        } else {
          const bool commit = false;
          (void)trans_end_(commit, get_global_trans_version_());
          (void)trans_clear_();
          end_trans_callback_(is_rollback, ret);
          (void)unregister_timeout_task_();
          set_exiting_();
          if (EXECUTE_COUNT_PER_SEC(1)) {
            TRANS_LOG(INFO, "ObPartTransCtx no redo log, maybe select for update statement", "context", *this);
          }
        }
      } else {
        // do nothing
      }
      if (OB_FAIL(ret)) {
        need_convert_to_dist_trans = true;
      }
    }
    if (OB_SUCCESS != ret) {
      need_print_trace_log_ = true;
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

int ObPartTransCtx::generate_sp_commit_log_type_(ObStorageLogType& log_type)
{
  int ret = OB_SUCCESS;
  ObStorageLogType tmp_log_type = ObStorageLogType::OB_LOG_UNKNOWN;
  int state = ObTransResultState::INVALID;

  if (OB_FAIL(ctx_dependency_wrap_.merge_cur_stmt_prev_trans_arr())) {
    TRANS_LOG(WARN, "merge last statement prev trans arr error", K(ret), K(*this));
  } else if (ctx_dependency_wrap_.get_prev_trans_arr_count() <= 0) {
    tmp_log_type = OB_LOG_SP_TRANS_COMMIT;
  } else if (OB_FAIL(ctx_dependency_wrap_.get_prev_trans_arr_result(state))) {
    TRANS_LOG(WARN, "get prev trans arr result error", K(ret), "context", *this);
  } else if (ObTransResultState::is_commit(state)) {
    tmp_log_type = OB_LOG_SP_TRANS_COMMIT;
    // The predecessors shouldnot be made durable to the log
    ctx_dependency_wrap_.reset_prev_trans_arr();
  } else if (ObTransResultState::is_abort(state)) {
    tmp_log_type = OB_LOG_SP_TRANS_ABORT;
    set_status_(OB_TRANS_ROLLBACKED);
    need_print_trace_log_ = true;
    TRANS_LOG(WARN, "current transaction need to rollback because of prev abort trans", "context", *this);
  } else {
    // If there exits the predecessors txns whose state is not decided, we write
    // the OB_LOG_SP_ELR_TRANS_COMMIT log
    tmp_log_type = OB_LOG_SP_ELR_TRANS_COMMIT;
  }
  if (OB_SUCC(ret)) {
    log_type = tmp_log_type;
  }

  return ret;
}

// After the schema split, there has no cluster level schema version.
// - The schema version of system table and user table is not comparable.
// - The schema version between tenants is not comparable.
// The upper layer need guarantee the schema version provided is comparable.
int ObPartTransCtx::check_schema_version_elapsed(const int64_t schema_version, const int64_t refreshed_schema_ts)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(lock_.lock(100000 /*100 ms*/))) {
    CtxLockGuard guard(lock_, false);

    if (IS_NOT_INIT) {
      TRANS_LOG(WARN, "ObPartTransCtx not inited");
      ret = OB_NOT_INIT;
    } else if (OB_UNLIKELY(schema_version <= 0) || OB_UNLIKELY(refreshed_schema_ts < 0)) {
      TRANS_LOG(WARN, "invalid argument", K(schema_version), "context", *this);
      ret = OB_INVALID_ARGUMENT;
    } else if (is_exiting_) {
      // do nothing
    } else if (for_replay_) {
      ret = OB_NOT_MASTER;
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
        TRANS_LOG(WARN, "current participant not master, need retry", K(ret), K(*this));
      }
    } else if (is_readonly_) {
      // do nothing
    } else if (ctx_create_time_ <= refreshed_schema_ts) {
      ret = OB_EAGAIN;
      TRANS_LOG(
          INFO, "current transaction not end, need retry", K(schema_version), K(refreshed_schema_ts), "context", *this);
    } else {
      // do nothing
    }
  } else {
    TRANS_LOG(WARN, "spin lock time out after 100 ms", K(ret));
    ret = OB_EAGAIN;
  }

  return ret;
}

int ObPartTransCtx::check_ctx_create_timestamp_elapsed(const int64_t ts)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(lock_.lock(100000 /*100 ms*/))) {
    CtxLockGuard guard(lock_, false);

    if (IS_NOT_INIT) {
      TRANS_LOG(WARN, "ObPartTransCtx not inited");
      ret = OB_NOT_INIT;
    } else if (OB_UNLIKELY(ts <= 0)) {
      TRANS_LOG(WARN, "invalid argument", K(ts), "context", *this);
      ret = OB_INVALID_ARGUMENT;
      // Either leder or not, the function may be invoked.(Discussed with @)
      //} else if (for_replay_) {
      //  ret = OB_NOT_MASTER;
      //  if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      //    TRANS_LOG(WARN, "current participant not master, need retry", K(ret), K(*this));
      //  }
    } else if (is_exiting_ || for_replay_ || is_readonly_) {
      // do nothing
    } else if (ts > ctx_create_time_) {
      ret = OB_EAGAIN;
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
        TRANS_LOG(
            INFO, "current transaction not end, need retry", KR(ret), K(ts), K_(ctx_create_time), "context", *this);
      }
    } else {
      // do nothing
    }
  } else {
    TRANS_LOG(WARN, "spin lock time out after 100 ms", K(ret));
    ret = OB_EAGAIN;
  }

  return ret;
}

// Use try lock to avoid lock conflicts during checkpointing
int ObPartTransCtx::checkpoint(const int64_t checkpoint, const int64_t safe_slave_read_timestamp, bool& checkpoint_succ)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(lock_.try_lock())) {
    if (OB_EAGAIN == ret) {
      ret = OB_SUCCESS;
    } else {
      TRANS_LOG(ERROR, "try lock error, unexpected error", K(ret), K(checkpoint), K_(trans_id));
    }
  } else {
    CtxTransTableLockGuard guard(lock_, trans_table_seqlock_, false);
    if (is_exiting_ || !for_replay_ || is_readonly_) {
      checkpoint_succ = true;
    } else if (OB_FAIL(checkpoint_(checkpoint, safe_slave_read_timestamp, checkpoint_succ))) {
      TRANS_LOG(WARN, "checkpoint error", KR(ret), K(checkpoint), "context", *this);
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObPartTransCtx::checkpoint_(
    const int64_t checkpoint, const int64_t safe_slave_read_timestamp, bool& checkpoint_succ)
{
  int ret = OB_SUCCESS;
  int64_t commit_version = 0;
  ObTimeGuard timeguard("checkpoint ctx", 50 * 1000);
  bool can_checkpoint = false;
  UNUSED(safe_slave_read_timestamp);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited, ignore it", "context", *this);
  } else if (is_exiting_ || !for_replay_ || is_readonly_) {
    // do nothing
  } else if (is_sp_trans_()) {
    // During the checkpoint process of the standby machine,
    // the state of the precursor does not need to be considered,
    // and the correctness can be guaranteed

    if (ObSpState::PREPARE == get_state_()) {
      can_checkpoint = true;
    }
  } else if (batch_commit_trans_ && is_prepared()) {
    // Batch commit transactions that have entered prepare can be checkedpoint off
    can_checkpoint = true;
  } else {
    // do nothing
  }
  if (OB_SUCC(ret) && can_checkpoint) {
    timeguard.click();
    // Single partition transactions do not need to calculate the commit version,
    //  and the log has been generated during the playback process
    if (is_sp_trans_()) {
      if (global_trans_version_ <= 0) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unexpected global transaction version", "context", *this);
      } else {
        commit_version = global_trans_version_;
      }
    } else if (OB_FAIL(calc_batch_commit_version_(commit_version))) {
      TRANS_LOG(WARN, "calc batch commit version failed", KR(ret));
    } else {
      // do nothing
    }
    timeguard.click();
    if (OB_SUCC(ret) && commit_version < checkpoint) {
      if (OB_FAIL(commit_by_checkpoint_(commit_version))) {
        TRANS_LOG(WARN, "commit by checkpoint failed", K(ret));
      } else {
        checkpoint_succ = true;
        if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
          TRANS_LOG(INFO,
              "checkpoint successfully on trans part ctx",
              K(checkpoint_succ),
              K(safe_slave_read_timestamp),
              K(commit_version),
              K(*this));
        }
      }
    }
    timeguard.click();
    REC_TRANS_TRACE_EXT(tlog_, checkpoint, Y(ret), Y(checkpoint), Y(commit_version));
    timeguard.click();
  }
  if (OB_SUCCESS != ret) {
    if (OB_NOT_RUNNING == ret) {
      TRANS_LOG(WARN, "checkpoint transaction failed", KR(ret), "context", *this, K(checkpoint));
    } else {
      TRANS_LOG(ERROR, "checkpoint transaction failed", KR(ret), "context", *this, K(checkpoint));
    }
    need_print_trace_log_ = true;
  }

  return ret;
}

int ObPartTransCtx::leader_revoke(const bool first_check, bool& need_release, ObEndTransCallbackArray& cb_array)
{
  UNUSED(first_check);
  UNUSED(cb_array);
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  need_release = false;

  common::ObTimeGuard timeguard("part_leader_revoke", 10 * 1000);
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);
  is_prepare_leader_revoke_ = true;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (is_exiting_ || for_replay_) {
    // do nothing
  } else if (is_readonly_ && ctx_dependency_wrap_.get_prev_trans_arr_count() <= 0) {
    for_replay_ = true;
    TRANS_LOG(WARN, "readonly transaction, need retry", KR(ret), K_(trans_param), "context", *this);
    // If the commit log of the current transaction is in the thread pool of the clog adapter,
    // you need to wait for the commit log to submit successfully or fail
    // Otherwise, if leader_takeover occurs after leader_revoke,
    // the commit log may be flushed to clog again
  } else if (submit_log_pending_count_ > 0 || is_gts_waiting_ || UINT64_MAX != dup_table_syncing_log_id_) {
    ret = OB_EAGAIN;
    TRANS_LOG(WARN, "submit log pending or gts waiting, need retry", KR(ret), "context", *this);
    // Update the location cache information of the local partition leader
  } else {
    // after 3.1, preapre version in mt_ctx should be set for xa trans when leader revoke
    if (ObTransVersion::INVALID_TRANS_VERSION != state_.get_prepare_version()) {
      mt_ctx_.set_prepare_version(state_.get_prepare_version());
    } else if (ObTransVersion::INVALID_TRANS_VERSION != global_trans_version_) {
      mt_ctx_.set_prepare_version(global_trans_version_);
    } else if (max_submitted_log_timestamp_ > 0) {
      mt_ctx_.set_prepare_version(max_submitted_log_timestamp_);
    } else {
      // do nothing
    }
    leader_epoch_ = -1;
    (void)unregister_timeout_task_();
    if (!has_logged_() && !is_in_2pc_() && 0 == submit_log_count_) {
      trans_kill_();
      // Because of  no logs, we can free the dirty trans instantly
      end_log_ts_ = 0;
      (void)trans_clear_();
      set_exiting_();
      if (!is_logging_()) {
        if (NULL != end_trans_cb_.get_cb()) {
          ObEndTransCallbackItem cb_item;
          cb_item.cb_ = end_trans_cb_.get_cb();
          cb_item.retcode_ = OB_TRANS_KILLED;
          if (OB_FAIL(cb_array.push_back(cb_item))) {
            TRANS_LOG(WARN, "push back callback failed", K(ret), "context", *this);
          } else {
            end_trans_cb_.reset();
          }
        }
      } else {
        TRANS_STAT_ABORT_TRANS_INC(tenant_id_);
      }
      if (!is_trans_state_sync_finished_) {
        TRANS_LOG(INFO, "transaction is killed", "context", *this);
      }
    } else if (has_logged_() && !is_in_2pc_() && !is_trans_state_sync_finished_ && 0 == submit_log_count_ &&
               FALSE_IT(mt_ctx_.clean_dirty_callbacks())) {
      // - When leader is revoking and some non-2pc logs of txn has already been
      //   submitted to sliding window:
      //   - Case 2.1: We only solve the case with no on-the-fly logs(because we have no idea
      //     whether the on-the-fly log is paxos-choosen or not)
      //   - If the state is not synced successfully(txn need abort), so we remove all
      //     marked trans node
    } else if (OB_FAIL(mt_ctx_.commit_to_replay())) {
      TRANS_LOG(WARN, "commit to replay error", KR(ret), "context", *this);
    } else {
      TRANS_LOG(DEBUG, "commit to replay success", "context", *this);
      if (can_elr_) {
        if (batch_commit_trans_ || is_sp_trans_()) {
          // Register the trans_result_info for txns who have submitted the
          // MUTATOR or REDO and current is in INIT state
          if (Ob2PCState::ABORT == state_.get_state() || ObSpState::ABORT == state_.get_state()) {
            // do nothing
          } else if (Ob2PCState::COMMIT == state_.get_state() || ObSpState::COMMIT == state_.get_state()) {
            if (OB_SUCCESS != (tmp_ret = register_trans_result_info_(ObTransResultState::COMMIT))) {
              TRANS_LOG(WARN, "register trans result info error", KR(ret), K(*this));
            }
          } else {
            // do nothing
          }
        }
      }
      // Single-partitioned txn need register timeout task in order to reply
      // client after txn timeout.
      if (is_sp_trans_()) {
        (void)register_timeout_task_(trans_2pc_timeout_);
      }
    }
    if (OB_SUCC(ret)) {
      is_trans_state_sync_finished_ = false;
      proposal_leader_.reset();
      dup_table_msg_mask_set_.reset();
      dup_table_lease_addrs_.reset();
      for_replay_ = true;
      is_prepare_leader_revoke_ = false;
      enable_new_1pc_ = false;
      if (submit_log_count_ > 0) {
        TRANS_LOG(INFO, "has uncomfirmed log", K(*this));
      }
    }
  }
  need_print_trace_log_ = true;
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "participant leader revoke error", KR(ret), "context", *this);
  } else {
    TRANS_LOG(INFO, "participant leader revoke success", "context", *this);
  }
  REC_TRANS_TRACE_EXT(tlog_,
      leader_revoke,
      OB_ID(ret),
      ret,
      OB_ID(need_release),
      need_release,
      OB_ID(used),
      timeguard.get_diff(),
      OB_ID(uref),
      get_uref());
  return ret;
}

void ObPartTransCtx::check_prev_trans_state_()
{
  int ret = OB_SUCCESS;
  ObElrPrevTransArrIterator iterator(ctx_dependency_wrap_);
  ObElrTransInfo* info = NULL;

  // Iterate all predecessor and ignore the error
  while ((NULL != (info = iterator.next()))) {
    const ObTransID& trans_id = info->get_trans_id();
    int state = ObTransResultState::INVALID;
    int result = ObTransResultState::INVALID;
    if (OB_FAIL(partition_mgr_->get_state_in_TRIM(trans_id, state))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        TRANS_LOG(WARN, "get state in trans result info mgr error", K(ret), K(trans_id), "context", *this);
      }
      // The predecessors has committed, we need mark the predecessor txns' state
    } else if (OB_FAIL(ctx_dependency_wrap_.mark_prev_trans_result(trans_id, state, result))) {
      TRANS_LOG(WARN, "mark prev trans result error", K(ret), K(trans_id), K(state), K(*this));
    }
  }
  UNUSED(ret);
}

// Drive 2pc protocol
int ObPartTransCtx::drive_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t state = get_state_();

  if (OB_UNLIKELY(is_hazardous_ctx_)) {
    // Avoid two duplicated logs
    if (EXECUTE_COUNT_PER_SEC(1)) {
      TRANS_LOG(ERROR, "current ctx status is hazardous", K(*this));
    }
  } else if (is_logging_()) {
    if (Ob2PCState::COMMIT == state) {
      if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
        TRANS_LOG(INFO, "current trans is logging, no need to drive state", "context", *this);
      }
    } else {
      TRANS_LOG(INFO, "current trans is logging, no need to drive state", "context", *this);
    }
  } else {
    switch (state) {
      case Ob2PCState::INIT: {
        if (is_xa_local_trans() && is_redo_prepared_) {
          int state = ObTransResultState::INVALID;
          if (OB_FAIL(ctx_dependency_wrap_.get_prev_trans_arr_result(state))) {
            TRANS_LOG(WARN, "get prev trans arr result error", K(ret), "context", *this);
          } else if (ObTransResultState::is_commit(state)) {
            if (OB_FAIL(post_2pc_response_(coordinator_, OB_TRANS_2PC_PREPARE_RESPONSE))) {
              TRANS_LOG(WARN, "post 2pc prepare response error", KR(ret), "context", *this);
            }
          } else if (ObTransResultState::is_abort(state) || OB_SUCCESS != get_status_()) {
            TRANS_LOG(WARN, "current transaction need to rollback because of prev abort trans", "context", *this);
            set_status_(OB_TRANS_ROLLBACKED);
            need_print_trace_log_ = true;
            if (OB_FAIL(post_2pc_response_(coordinator_, OB_TRANS_2PC_PREPARE_RESPONSE))) {
              TRANS_LOG(WARN, "post 2pc prepare response error", KR(ret), "context", *this);
            }
          } else {
            // There exits predecessors whose state is not decided, the leader
            // can reply on the callback of predecessors without looking up for
            // the trans_result_info.
            check_prev_trans_state_();
          }
        } else {
          // participant timeout in INIT state is allowed. just print a warn log.
          TRANS_LOG(DEBUG, "drive participant state", K(state), "context", *this, K(lbt()));
        }
        break;
      }
      case Ob2PCState::PRE_PREPARE: {
        // participant timeout in INIT state is allowed. just print a warn log.
        TRANS_LOG(DEBUG, "drive participant state", K(state), "context", *this, K(lbt()));
        break;
      }
      case Ob2PCState::PREPARE: {
        int state = ObTransResultState::INVALID;
        if (OB_FAIL(ctx_dependency_wrap_.get_prev_trans_arr_result(state))) {
          TRANS_LOG(WARN, "get prev trans arr result error", K(ret), "context", *this);
        } else if (ObTransResultState::is_commit(state)) {
          if (OB_FAIL(post_2pc_response_(coordinator_, OB_TRANS_2PC_PREPARE_RESPONSE))) {
            TRANS_LOG(WARN, "post 2pc prepare response error", KR(ret), "context", *this);
          }
        } else if (ObTransResultState::is_abort(state) || OB_SUCCESS != get_status_()) {
          TRANS_LOG(WARN, "current transaction need to rollback because of prev abort trans", "context", *this);
          set_status_(OB_TRANS_ROLLBACKED);
          need_print_trace_log_ = true;
          if (OB_FAIL(post_2pc_response_(coordinator_, OB_TRANS_2PC_PREPARE_RESPONSE))) {
            TRANS_LOG(WARN, "post 2pc prepare response error", KR(ret), "context", *this);
          }
        } else {
          // There exits predecessors whose state is not decided, the leader
          // can reply on the callback of predecessors without looking up for
          // the trans_result_info.
          check_prev_trans_state_();
        }
        break;
      }
      case ObSpState::PREPARE: {
        bool UNUSED = false;
        // For single-partitioned txn, if the ELR_SP_COMMIT_LOG is not in
        // majoriy, we shou check for predecessors.
        int state = ObTransResultState::INVALID;
        if (OB_FAIL(ctx_dependency_wrap_.get_prev_trans_arr_result(state))) {
          TRANS_LOG(WARN, "get prev trans arr result error", K(ret), "context", *this);
        } else if (ObTransResultState::is_commit(state)) {
          if (OB_SUCCESS != get_status_()) {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(ERROR, "unexpected status", KR(ret), "context", *this);
          } else if (OB_FAIL(on_sp_commit_(true))) {
            TRANS_LOG(WARN, "sp trans on commit error", KR(ret), "context", *this);
          } else {
            // do nothing
          }
        } else if (ObTransResultState::is_abort(state) || OB_SUCCESS != get_status_()) {
          TRANS_LOG(WARN, "current transaction need to rollback because of prev abort trans", "context", *this);
          set_status_(OB_TRANS_ROLLBACKED);
          need_print_trace_log_ = true;
          if (has_logged_()) {
            if (OB_FAIL(submit_log_task_(OB_LOG_SP_TRANS_ABORT, UNUSED))) {
              TRANS_LOG(WARN, "submit sp trans abort log task error", KR(ret), "context", *this);
            }
          } else {
            if (OB_FAIL(on_sp_commit_(false))) {
              TRANS_LOG(WARN, "sp trans on commit error", KR(ret), "context", *this);
            }
          }
        } else {
          // NB: For single-partitioned txn with no mutator, if the txn's
          // statement execution is timeout and the predecessors has not
          // committed. We can rollback it immediately to prevent successor txn
          if (stmt_expired_time_ > 0 && ObTimeUtility::current_time() > stmt_expired_time_ && !has_logged_()) {
            end_trans_callback_(OB_TRANS_STMT_TIMEOUT);
            if (OB_FAIL(on_sp_commit_(false))) {
              TRANS_LOG(WARN, "sp trans on commit error", KR(ret), "context", *this);
            }
          }
          // There exits predecessors whose state is not decided, the leader
          // can reply on the callback of predecessors without looking up for
          // the trans_result_info.
          check_prev_trans_state_();
        }
        break;
      }
      case Ob2PCState::COMMIT: {
        if (OB_FAIL(post_2pc_response_(coordinator_, OB_TRANS_2PC_COMMIT_RESPONSE))) {
          TRANS_LOG(WARN, "post 2pc commit response error", KR(ret), "context", *this);
        }
        break;
      }
      case Ob2PCState::ABORT: {
        if (OB_FAIL(post_2pc_response_(coordinator_, OB_TRANS_2PC_ABORT_RESPONSE))) {
          TRANS_LOG(WARN, "post 2pc abort response error", KR(ret), "context", *this);
        }
        break;
      }
      case Ob2PCState::CLEAR: {
        if (OB_FAIL(post_2pc_response_(coordinator_, OB_TRANS_2PC_CLEAR_RESPONSE))) {
          TRANS_LOG(WARN, "post 2pc clear response error", KR(ret), "context", *this);
        }
        break;
      }
      default: {
        TRANS_LOG(WARN, "invalid 2pc state", "context", *this, K(state));
        ret = OB_TRANS_INVALID_STATE;
        break;
      }
    }
  }

  if (!is_exiting_) {
    (void)unregister_timeout_task_();
    if (OB_SUCCESS != (tmp_ret = register_timeout_task_(trans_2pc_timeout_))) {
      TRANS_LOG(WARN, "register timeout handler error", "ret", tmp_ret, "context", *this);
    }
  }

  return ret;
}

int ObPartTransCtx::replay_sp_redo_log(
    const ObSpTransRedoLog& log, const int64_t timestamp, const uint64_t log_id, int64_t& log_table_version)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t start = ObTimeUtility::fast_current_time();
  ObWorker::CompatMode mode = ObWorker::CompatMode::INVALID;

  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!for_replay_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "invalid state, transaction is not replaying", KR(ret), "context", *this);
    need_print_trace_log_ = true;
  } else if (OB_UNLIKELY(!is_trans_valid_for_replay_(OB_LOG_SP_TRANS_REDO, timestamp))) {
    TRANS_LOG(WARN, "trans is not valid", K(*this), K(log_id), K(timestamp), K(log));
    ret = OB_TRANS_INVALID_STATE;
    need_print_trace_log_ = true;
  } else if (OB_UNLIKELY(!log.is_valid()) || OB_UNLIKELY(timestamp < 0)) {
    TRANS_LOG(WARN, "invalid argument", K(log), K(timestamp), K(log_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(is_hazardous_ctx_)) {
    ret = OB_EAGAIN;
    TRANS_LOG(ERROR, "current ctx status is hazardous", K(ret), K(*this));
  } else if (OB_LOG_SP_TRANS_REDO != log.get_log_type() && OB_LOG_SP_TRANS_COMMIT != log.get_log_type() &&
             OB_LOG_SP_ELR_TRANS_COMMIT != log.get_log_type()) {
    TRANS_LOG(WARN, "invalid log type", "log_type", log.get_log_type(), K(log));
    ret = OB_TRANS_INVALID_LOG_TYPE;
  } else if (OB_ISNULL(partition_service_)) {
    TRANS_LOG(ERROR, "partition service is null", K(log));
    ret = OB_ERR_UNEXPECTED;
  } else if (log_id <= last_replayed_redo_log_id_) {
    TRANS_LOG(INFO, "redo log has replayed", K(log_id), K(log), K(*this));
  } else if (OB_FAIL(ctx_dependency_wrap_.prev_trans_arr_assign(log.get_prev_trans_arr()))) {
    TRANS_LOG(WARN, "prev transaction array assign error", KR(ret), K(log));
  } else if (submit_log_count_ > 0) {
    // The log is written by myself
    mt_ctx_.sync_log_succ(log_id, timestamp);
    if (timestamp > max_durable_log_ts_) {
      redo_log_no_++;
    }
    submit_log_count_ = 0;
    has_write_or_replay_mutator_redo_log_ = true;
    TRANS_LOG(INFO, "replay self log", K(log_id), K(*this));
  } else if (OB_FAIL(get_compat_mode_(mode))) {
    TRANS_LOG(WARN, "check and update compact mode error", KR(ret), "context", *this);
  } else {
    share::CompatModeGuard compat_guard(mode);
    set_stc_(MonotonicTs(timestamp));
    if (can_elr_ != log.is_can_elr()) {
      TRANS_LOG(WARN, "different can elr state", K(log), K(*this));
    }
    can_elr_ = log.is_can_elr();
    if (OB_FAIL(set_trans_param_(log.get_trans_param()))) {
      TRANS_LOG(WARN, "set transaction param error", KR(ret), K(log));
    } else {
      if (OB_SUCC(ret)) {
        state_.set_prepare_version(timestamp);
        trans_type_ = TransType::SP_TRANS;
        log_type_ = log.get_log_type();
        ObStoreCtx store_ctx;
        store_ctx.mem_ctx_ = &mt_ctx_;
        store_ctx.tenant_id_ = self_.get_tenant_id();
        store_ctx.log_ts_ = timestamp;
        store_ctx.cur_pkey_ = self_;
        cluster_id_ = log.get_cluster_id();
        const ObTransMutator &mutator = log.get_mutator();
        bool replayed = true;
        if (OB_FAIL(partition_service_->replay_redo_log(
                self_, store_ctx, timestamp, log_id, mutator.get_data(), mutator.get_position(), replayed))) {
          if (OB_ALLOCATE_MEMORY_FAILED != ret || EXECUTE_COUNT_PER_SEC(100)) {
            TRANS_LOG(WARN, "replay sp redo log error", KR(ret), "partition", self_, K(log));
          }
          if (OB_TRANS_WAIT_SCHEMA_REFRESH == ret) {
            log_table_version = store_ctx.mem_ctx_->get_max_table_version();
          }
        } else if (replayed) {
          if (timestamp > max_durable_log_ts_) {
            ++mutator_log_no_;
          }
          need_checksum_ = need_checksum_ || (0 == log.get_log_no());
          has_write_or_replay_mutator_redo_log_ = true;
        } else {
          TRANS_LOG(INFO, "no need to replay, maybe partition already removed", K(log), K(*this));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (timestamp > max_durable_log_ts_) {
        redo_log_no_++;
      }
    }

    REC_TRANS_TRACE_EXT(tlog_,
        replay_sp_redo,
        OB_ID(ret),
        ret,
        OB_ID(used),
        ObTimeUtility::fast_current_time() - start,
        OB_ID(id),
        log_id,
        OB_ID(t),
        timestamp,
        OB_ID(uref),
        get_uref());
  }
  if (OB_SUCC(ret)) {
    last_replayed_redo_log_id_ = log_id;
    if (timestamp > max_durable_log_ts_ && OB_FAIL(prev_redo_log_ids_.push_back(log_id))) {
      TRANS_LOG(WARN, "redo log id push back error", KR(ret), K(timestamp));
    } else {
      if (is_dup_table_trans_ && OB_UNLIKELY(OB_SUCCESS != (tmp_ret = post_redo_log_sync_response_(log_id)))) {
        TRANS_LOG(WARN, "post redo log sync response error", K(tmp_ret), K(*this));
      }
    }
  }

  if (OB_SUCC(ret)) {
    update_durable_log_id_ts_(OB_LOG_SP_TRANS_REDO, log_id, timestamp);
  }

  const int64_t end = ObTimeUtility::fast_current_time();
  if (OB_UNLIKELY(end - start > REPLAY_PRINT_TRACE_THRESHOLD) && REACH_TIME_INTERVAL(1L * 1000 * 1000)) {
    need_print_trace_log_ = true;
  }

  return ret;
}

int ObPartTransCtx::replay_sp_commit_log(const ObSpTransCommitLog& log, const int64_t timestamp, const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  const int64_t start = ObTimeUtility::fast_current_time();

  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!for_replay_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "invalid state, transaction is not replaying", KR(ret), "context", *this);
    need_print_trace_log_ = true;
  } else if (OB_UNLIKELY(!log.is_valid()) || OB_UNLIKELY(timestamp < 0)) {
    TRANS_LOG(WARN, "invalid argument", K(log), K(timestamp), K(log_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(is_hazardous_ctx_)) {
    ret = OB_EAGAIN;
    TRANS_LOG(ERROR, "current ctx status is hazardous", K(ret), K(*this));
  } else if (OB_LOG_SP_TRANS_COMMIT != log.get_log_type() && OB_LOG_SP_ELR_TRANS_COMMIT != log.get_log_type()) {
    TRANS_LOG(WARN, "invalid log type", "log_type", log.get_log_type());
    ret = OB_TRANS_INVALID_LOG_TYPE;
  } else if (OB_UNLIKELY(!is_trans_valid_for_replay_(OB_LOG_SP_TRANS_COMMIT, timestamp))) {
    TRANS_LOG(WARN, "trans is not valid", K(*this), K(log_id), K(timestamp), K(log));
    ret = OB_TRANS_INVALID_STATE;
    need_print_trace_log_ = true;
  } else if (OB_ISNULL(partition_service_)) {
    TRANS_LOG(ERROR, "partition service is null", K(log));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(set_app_trace_info_(log.get_app_trace_info()))) {
    TRANS_LOG(WARN, "set app trace info error", K(ret), K(log), K(*this));
  } else if (OB_FAIL(set_app_trace_id_(log.get_app_trace_id_str()))) {
    TRANS_LOG(WARN, "set app trace id error", K(ret), K(log), K(*this));
  } else {
    bool need_inc_pending_elr = false;
    update_durable_log_id_ts_(OB_LOG_SP_TRANS_COMMIT, log_id, timestamp);
    submit_log_count_ = 0;
    trans_type_ = TransType::SP_TRANS;
    log_type_ = log.get_log_type();
    // Recover the predecessors' information
    if (OB_LOG_SP_ELR_TRANS_COMMIT == log.get_log_type()) {
      global_trans_version_ = timestamp;
      mt_ctx_.set_prepare_version(timestamp);
      int state = ObTransResultState::INVALID;
      if (log.get_prev_trans_arr().count() <= 0) {
        TRANS_LOG(ERROR, "unexpected prev trans arr count", K(log), "context", *this);
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(ctx_dependency_wrap_.prev_trans_arr_assign(log.get_prev_trans_arr()))) {
        TRANS_LOG(WARN, "prev transaction array assign error", KR(ret), K(log));
      } else if (OB_FAIL(ctx_dependency_wrap_.get_prev_trans_arr_result(state))) {
        TRANS_LOG(WARN, "get prev trans arr result error", K(ret), "context", *this);
      } else if (ObTransResultState::is_abort(state)) {
        // Wait for replaying abort log
      } else if (ObTransResultState::is_commit(state)) {
        const uint64_t checksum = (need_checksum_ ? log.get_checksum() : 0);
        if (OB_FAIL(trans_replay_commit_(global_trans_version_, checksum))) {
          TRANS_LOG(WARN, "trans replay commit failed", KR(ret), "context", *this);
        } else if (OB_FAIL(trans_clear_())) {
          TRANS_LOG(WARN, "transaction clear error", KR(ret), "context", *this);
        } else {
          set_state_(ObSpState::COMMIT);
          set_exiting_();
        }
      } else if (OB_FAIL(insert_all_prev_trans_(true))) {
        if (OB_EAGAIN != ret) {
          TRANS_LOG(WARN, "insert all prev trans error", KR(ret), "context", *this);
        } else {
          set_state_(ObSpState::PREPARE);
        }
        // If register fails, we can reply on later checkpoint
        ret = OB_SUCCESS;
        need_inc_pending_elr = true;
      } else {
        set_state_(ObSpState::PREPARE);
        need_inc_pending_elr = true;
      }
      if (need_inc_pending_elr) {
        if (OB_FAIL(partition_service_->inc_pending_elr_count(self_, mt_ctx_, timestamp))) {
          TRANS_LOG(WARN, "failed to inc pending elr count", K(ret), "context", *this);
        } else {
          end_log_ts_for_elr_ = timestamp;
        }
      } else {
        end_log_ts_ = timestamp;
      }
      INC_ELR_STATISTIC(with_dependency_trx_count);
    } else if (log.get_prev_trans_arr().count() > 0) {
      TRANS_LOG(ERROR, "unexpected prev trans arr count", K(log), "context", *this);
      ret = OB_ERR_UNEXPECTED;
    } else {
      const uint64_t checksum = (need_checksum_ ? log.get_checksum() : 0);
      global_trans_version_ = timestamp;
      if (OB_FAIL(trans_replay_commit_(global_trans_version_, checksum))) {
        TRANS_LOG(WARN, "trans replay commit failed", KR(ret), "context", *this);
      } else if (OB_FAIL(trans_clear_())) {
        TRANS_LOG(WARN, "transaction clear error", KR(ret), "context", *this);
      } else {
        set_state_(ObSpState::COMMIT);
        set_exiting_();
        INC_ELR_STATISTIC(without_dependency_trx_count);
        INC_ELR_STATISTIC(end_trans_by_self_count);
      }
    }
  }

  // Single-partitioned txn should not reply OB_TRANS_UNKNOWN after leader transfer for clearity,
  // we can wait for the reply of SP_COMMIT or ABORT
  end_trans_callback_(OB_SUCCESS);
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "replay sp commit log error", KR(ret), "context", *this);
  } else {
    TRANS_LOG(DEBUG, "replay sp commit log success", "context", *this);
  }
  REC_TRANS_TRACE_EXT(tlog_,
      replay_sp_commit,
      OB_ID(ret),
      ret,
      OB_ID(used),
      ObTimeUtility::fast_current_time() - start,
      OB_ID(id),
      log_id,
      OB_ID(t),
      timestamp,
      OB_ID(uref),
      get_uref());
  return ret;
}

int ObPartTransCtx::replay_sp_abort_log(const ObSpTransAbortLog& log, const int64_t timestamp, const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const int64_t start = ObTimeUtility::fast_current_time();
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!for_replay_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "invalid state, transaction is not replaying", KR(ret), "context", *this);
    need_print_trace_log_ = true;
  } else if (OB_UNLIKELY(!log.is_valid()) || OB_UNLIKELY(timestamp < 0)) {
    TRANS_LOG(WARN, "invalid argument", K(log), K(timestamp), K(log_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(OB_LOG_SP_TRANS_ABORT != log.get_log_type())) {
    TRANS_LOG(WARN, "invalid log type", "log_type", log.get_log_type());
    ret = OB_TRANS_INVALID_LOG_TYPE;
  } else if (OB_UNLIKELY(!is_trans_valid_for_replay_(OB_LOG_SP_TRANS_ABORT, timestamp))) {
    TRANS_LOG(WARN, "trans is not valid", K(*this), K(log_id), K(timestamp), K(log));
    ret = OB_TRANS_INVALID_STATE;
    need_print_trace_log_ = true;
  } else if (OB_FAIL(mt_ctx_.trans_replay_end(false, global_trans_version_))) {
    TRANS_LOG(WARN, "transaction replay end error", KR(ret), "context", *this);
  } else if (OB_FAIL(trans_clear_())) {
    TRANS_LOG(WARN, "transaction clear error", KR(ret), "context", *this);
  } else {
    if (OB_SUCCESS != (tmp_ret = ctx_dependency_wrap_.callback_next_trans_arr(ObTransResultState::ABORT))) {
      TRANS_LOG(WARN, "callback next trans array error", K(tmp_ret), "context", *this);
    }
    update_durable_log_id_ts_(OB_LOG_SP_TRANS_ABORT, log_id, timestamp);
    submit_log_count_ = 0;
    trans_type_ = TransType::SP_TRANS;
    log_type_ = log.get_log_type();
    set_status_(OB_TRANS_ROLLBACKED);
    set_state_(ObSpState::ABORT);
    set_exiting_();
    if (cluster_id_ != log.get_cluster_id()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR,
          "unexpected error, cluster id not match",
          KR(ret),
          "cluster_id_in_log",
          log.get_cluster_id(),
          K_(cluster_id),
          "context",
          *this);
      need_print_trace_log_ = true;
    }
  }

  // Single-partitioned txn should not reply OB_TRANS_UNKNOWN after leader transfer for clearity,
  // we can wait for the reply of SP_COMMIT or ABORT
  end_trans_callback_(OB_TRANS_KILLED);
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "replay sp abort log error", KR(ret), "context", *this, K(log));
  } else {
    TRANS_LOG(DEBUG, "replay sp abort log success", "context", *this, K(log));
  }
  REC_TRANS_TRACE_EXT(tlog_,
      replay_sp_abort,
      OB_ID(ret),
      ret,
      OB_ID(used),
      ObTimeUtility::fast_current_time() - start,
      OB_ID(id),
      log_id,
      OB_ID(t),
      timestamp,
      OB_ID(uref),
      get_uref());
  return ret;
}

// NB: Replay redo log should be reentrant
int ObPartTransCtx::replay_redo_log(const ObTransRedoLog& log, const int64_t timestamp, const uint64_t log_id,
    const bool with_prepare, int64_t& log_table_version)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObWorker::CompatMode mode = ObWorker::CompatMode::INVALID;
  const int64_t start = ObTimeUtility::fast_current_time();

  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!for_replay_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "invalid state, transaction is not replaying", KR(ret), "context", *this);
    need_print_trace_log_ = true;
  } else if (OB_UNLIKELY(!log.is_valid()) || OB_UNLIKELY(timestamp < 0)) {
    TRANS_LOG(WARN, "invalid argument", K(log), K(timestamp), K(log_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(is_hazardous_ctx_)) {
    ret = OB_EAGAIN;
    TRANS_LOG(ERROR, "current ctx status is hazardous", K(ret), K(*this));
  } else if (OB_UNLIKELY(OB_LOG_TRANS_REDO != log.get_log_type())) {
    TRANS_LOG(WARN, "invalid log type", "log_type", log.get_log_type(), K(log));
    ret = OB_TRANS_INVALID_LOG_TYPE;
  } else if (OB_UNLIKELY(!is_trans_valid_for_replay_(OB_LOG_TRANS_REDO, timestamp))) {
    TRANS_LOG(WARN, "trans is not valid", K(*this), K(log_id), K(timestamp), K(log));
    ret = OB_TRANS_INVALID_STATE;
    need_print_trace_log_ = true;
  } else if (OB_ISNULL(partition_service_)) {
    TRANS_LOG(ERROR, "partition service is null", K(log));
    ret = OB_ERR_UNEXPECTED;
  } else if (log_id <= last_replayed_redo_log_id_) {
    TRANS_LOG(INFO, "redo log has replayed", K(log_id), K(log), K(*this));
  } else if (OB_FAIL(ctx_dependency_wrap_.prev_trans_arr_assign(log.get_prev_trans_arr()))) {
    TRANS_LOG(WARN, "prev transaction array assign error", KR(ret), K(log));
  } else if (submit_log_count_ > 0) {
    // The log is written by myself
    mt_ctx_.sync_log_succ(log_id, timestamp);
    submit_log_count_ = 0;
    has_write_or_replay_mutator_redo_log_ = true;
    TRANS_LOG(INFO, "replay self log", K(log_id), K(*this));
  } else if (OB_FAIL(get_compat_mode_(mode))) {
    TRANS_LOG(WARN, "check and update compact mode error", KR(ret), "context", *this);
  } else {
    share::CompatModeGuard compat_guard(mode);
    set_stc_(MonotonicTs(timestamp));
    if (OB_FAIL(set_scheduler_(log.get_scheduler()))) {
      TRANS_LOG(WARN, "set scheduler error", KR(ret), K(log));
    } else if (OB_FAIL(set_coordinator_(log.get_coordinator()))) {
      TRANS_LOG(WARN, "set coordinator error", KR(ret), K(log));
    } else if (OB_FAIL(set_participants_(log.get_participants()))) {
      TRANS_LOG(WARN, "set participants error", KR(ret), K(log));
    } else if (OB_FAIL(set_trans_param_(log.get_trans_param()))) {
      TRANS_LOG(WARN, "set transaction param error", KR(ret), K(log));
    } else if (OB_FAIL(set_xid_(log.get_xid()))) {
      TRANS_LOG(WARN, "set xid error", KR(ret), K(log));
    } else {
      if (can_elr_ != log.is_can_elr()) {
        TRANS_LOG(WARN, "different can elr state", K(log), K(*this));
      }
      // Set up the batch_commit_trans_ in advance and wait for replaying
      // prepare log. The resion is as following: The txn split need wait for
      // all 1PC txn to be finished: for the scenerio of the REDO_WITH_PREPARE,
      // replay redo and prepare are different calls, and the invokation is not
      // atomic, so if there exits wait_1pc_trx_end between the two calls, we
      // may lost count the 1pc txn
      batch_commit_trans_ = with_prepare;
      can_elr_ = log.is_can_elr();
      log_type_ = log.get_log_type();
      state_.set_prepare_version(timestamp);
      ObStoreCtx store_ctx;
      store_ctx.mem_ctx_ = &mt_ctx_;
      store_ctx.tenant_id_ = self_.get_tenant_id();
      store_ctx.log_ts_ = timestamp;
      cluster_id_ = log.get_cluster_id();
      // Set up the xid first before checking for XA trans
      if (is_xa_local_trans()) {
        in_xa_prepare_state_ = true;
      }
      if (log.is_last()) {
        is_redo_prepared_ = true;
      }
      if (OB_SUCC(ret)) {
        const ObTransMutator &mutator = log.get_mutator();
        bool replayed = true;
        if (0 == mutator.get_position() && is_xa_local_trans()) {
          replayed = false;
          TRANS_LOG(INFO, "replay empty xa redo log", K_(xid), K(log_id));
        } else if (OB_FAIL(partition_service_->replay_redo_log(
                       self_, store_ctx, timestamp, log_id, mutator.get_data(), mutator.get_position(), replayed))) {
          if (OB_ALLOCATE_MEMORY_FAILED != ret || EXECUTE_COUNT_PER_SEC(100)) {
            TRANS_LOG(WARN, "replay redo log error", KR(ret), "partition", self_, K(log));
          }
          if (OB_TRANS_WAIT_SCHEMA_REFRESH == ret) {
            log_table_version = store_ctx.mem_ctx_->get_max_table_version();
          }
        }
        if (OB_SUCC(ret)) {
          if (replayed) {
            need_checksum_ = need_checksum_ || (0 == log.get_log_no());
            has_write_or_replay_mutator_redo_log_ = true;
          } else {
            TRANS_LOG(INFO, "no need to replay, maybe partition already removed", K(log), K(*this));
          }
        }
      }
    }
    REC_TRANS_TRACE_EXT(tlog_,
        replay_redo,
        OB_ID(ret),
        ret,
        OB_ID(used),
        ObTimeUtility::fast_current_time() - start,
        OB_ID(id),
        log_id,
        OB_ID(t),
        timestamp,
        OB_ID(arg1),
        with_prepare,
        OB_ID(uref),
        get_uref());
  }
  if (OB_SUCC(ret)) {
    last_replayed_redo_log_id_ = log_id;
    last_redo_log_mutator_size_ = log.get_mutator().get_position();
    if (timestamp > max_durable_log_ts_) {
      redo_log_no_++;
    }
    if (!with_prepare && timestamp > max_durable_log_ts_ && !is_xa_last_empty_redo_log_() &&
        OB_FAIL(prev_redo_log_ids_.push_back(log_id))) {
      TRANS_LOG(WARN, "redo log id push back error", KR(ret), K(timestamp));
    } else if (is_dup_table_trans_ && OB_UNLIKELY(OB_SUCCESS != (tmp_ret = post_redo_log_sync_response_(log_id)))) {
      TRANS_LOG(WARN, "post redo log sync response error", K(tmp_ret), K(*this));
    } else {
      // do nothing
    }
  }
  if (OB_SUCC(ret)) {
    if (with_prepare) {
      is_in_redo_with_prepare_ = true;
    } else {
      update_durable_log_id_ts_(OB_LOG_TRANS_REDO, log_id, timestamp);
    }
  }
  if (OB_UNLIKELY(ObTimeUtility::fast_current_time() - start > REPLAY_PRINT_TRACE_THRESHOLD) &&
      REACH_TIME_INTERVAL(1L * 1000 * 1000)) {
    need_print_trace_log_ = true;
  }

  return ret;
}

int ObPartTransCtx::replay_record_log(const ObTransRecordLog &log,
    const int64_t timestamp, const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  const int64_t start = ObTimeUtility::fast_current_time();
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);
  TRANS_LOG(INFO, "before replaying record log: ", K(log_id), K(prev_redo_log_ids_), K(prev_checkpoint_id_));

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!for_replay_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "invalid state, transaction is not replaying", KR(ret), "context", *this);
    need_print_trace_log_ = true;
  } else if (OB_UNLIKELY(!log.is_valid()) || OB_UNLIKELY(timestamp < 0)) {
    TRANS_LOG(WARN, "invalid argument", K(log), K(timestamp), K(log_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(!is_trans_valid_for_replay_(OB_LOG_TRANS_RECORD, timestamp))) {
    TRANS_LOG(WARN, "trans is not valid", K(*this), K(log_id), K(timestamp), K(log));
    ret = OB_TRANS_INVALID_STATE;
    need_print_trace_log_ = true;
  } else if (OB_UNLIKELY(is_hazardous_ctx_)) {
    ret = OB_EAGAIN;
    TRANS_LOG(ERROR, "current ctx status is hazardous", K(ret), K(*this));
  } else {
    submit_log_count_ = 0;
    update_durable_log_id_ts_(OB_LOG_TRANS_RECORD, log_id, timestamp);
    reset_prev_redo_log_ids();
    prev_checkpoint_id_ = log_id;
    TRANS_LOG(INFO, "replay record log success", K(prev_redo_log_ids_), K(prev_checkpoint_id_), "context", *this);
    REC_TRANS_TRACE_EXT(tlog_, replay_record, OB_ID(ret), ret, OB_ID(used), ObTimeUtility::fast_current_time() - start,
        OB_ID(id), log_id, OB_ID(t), timestamp, OB_ID(uref), get_uref());
  }

  return ret;
}

int ObPartTransCtx::replay_prepare_log(const ObTransPrepareLog &log,
    const int64_t timestamp, const uint64_t log_id, const bool batch_committed,
    const int64_t checkpoint)
{
  UNUSED(checkpoint);
  int ret = OB_SUCCESS;
  int64_t commit_version = 0;
  bool can_checkpoint = false;
  const int64_t start = ObTimeUtility::fast_current_time();
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!for_replay_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "invalid state, transaction is not replaying", KR(ret), "context", *this);
    need_print_trace_log_ = true;
  } else if (OB_UNLIKELY(!log.is_valid()) || OB_UNLIKELY(timestamp < 0)) {
    TRANS_LOG(WARN, "invalid argument", K(log), K(timestamp), K(log_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(!is_trans_valid_for_replay_(OB_LOG_TRANS_PREPARE, timestamp))) {
    TRANS_LOG(WARN, "trans is not valid", K(*this), K(log_id), K(timestamp), K(log));
    ret = OB_TRANS_INVALID_STATE;
    need_print_trace_log_ = true;
  } else if (OB_UNLIKELY(is_hazardous_ctx_)) {
    ret = OB_EAGAIN;
    TRANS_LOG(ERROR, "current ctx status is hazardous", K(ret), K(*this));
  } else if (batch_committed && log.get_partition_log_info_arr().count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpectd batch commit status", KR(ret), K(log), K(batch_committed));
    need_print_trace_log_ = true;
  } else if (OB_UNLIKELY(0 == redo_log_no_) && OB_FAIL(set_scheduler_(log.get_scheduler()))) {
    TRANS_LOG(WARN, "set scheduler error", K(ret), K(log));
  } else if (OB_UNLIKELY(0 == redo_log_no_) && OB_FAIL(set_coordinator_(log.get_coordinator()))) {
    TRANS_LOG(WARN, "set coordinator error", K(ret), K(log));
  } else if (OB_UNLIKELY(0 == redo_log_no_) && OB_FAIL(set_participants_(log.get_participants()))) {
    TRANS_LOG(WARN, "set participants error", K(ret), K(log));
  } else if (OB_UNLIKELY(0 == redo_log_no_) && OB_FAIL(set_trans_param_(log.get_trans_param()))) {
    TRANS_LOG(WARN, "set transaction param error", K(ret), K(log));
  } else if (OB_FAIL(partition_log_info_arr_.assign(log.get_partition_log_info_arr()))) {
    TRANS_LOG(WARN, "assign partition log info array failed", KR(ret), K(log));
  } else if (OB_FAIL(ctx_dependency_wrap_.prev_trans_arr_assign(log.get_prev_trans_arr()))) {
    TRANS_LOG(WARN, "prev transaction array assign error", KR(ret), K(log));
  } else if (OB_FAIL(set_app_trace_info_(log.get_app_trace_info()))) {
    TRANS_LOG(WARN, "set app trace info error", K(ret), K(log), K(*this));
  } else if (OB_FAIL(set_app_trace_id_(log.get_app_trace_id_str()))) {
    TRANS_LOG(WARN, "set app trace id error", K(ret), K(log), K(*this));
  } else if (OB_FAIL(set_xid_(log.get_xid()))) {
    TRANS_LOG(WARN, "set xid error", K(ret), K(log));
  } else {
    bool is_state_durable = false;
    if (max_durable_log_ts_ >= timestamp) {
      is_state_durable = true;
    }
    if (can_elr_ != log.is_can_elr()) {
      TRANS_LOG(WARN, "different can elr state", K(log), K(*this));
    }
    submit_log_count_ = 0;
    can_elr_ = log.is_can_elr();
    update_durable_log_id_ts_(OB_LOG_TRANS_PREPARE, log_id, timestamp);
    log_type_ = log.get_log_type();
    // xid_ = log.get_xid();
    if (log.is_batch_commit_trans()) {
      batch_commit_trans_ = true;
    } else {
      batch_commit_trans_ = false;
    }
    if (0 == log.get_redo_log_ids().count() && 0 == redo_log_no_) {
      // We only enable the checksum check if prev_redo_log_ids' count is zero
      // and redo_log_no is zero. The later check is used to filter the txn
      // REDO_WITH_PREPARE log which donot include itself inth prev_redo_log_id.
      need_checksum_ = true;
    }
    /*
    if (can_elr_ && redo_log_no_ + mutator_log_no_ <= 0
        && OB_FAIL(register_trans_result_info_(ObTransResultState::UNKNOWN))) {
      TRANS_LOG(WARN, "early release lock error", KR(ret), "context", *this);
    } else {*/
    set_stc_(MonotonicTs(timestamp));
    set_status_(log.get_prepare_status());
    state_.set_prepare_version(timestamp);
    mt_ctx_.set_prepare_version(timestamp);
    is_prepared_ = true;
    in_xa_prepare_state_ = false;
    is_redo_prepared_ = true;
    prepare_log_id_ = log_id;
    prepare_log_timestamp_ = timestamp;
    cluster_id_ = log.get_cluster_id();
    // During the replay of prepare log, we need gurantee the
    // local_trans_version is assigned before prepare state, otherwise the slave
    // read timestamp may be miscompyed
    if (!is_state_durable) {
      set_state_(Ob2PCState::PREPARE);
    }
    is_xa_trans_prepared_ = true;
    if (batch_commit_trans_) {
      if (OB_ISNULL(partition_mgr_)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "partition mgr is null", KR(ret));
      } else if (OB_FAIL(calc_batch_commit_version_(commit_version))) {
        TRANS_LOG(WARN, "calc batch commit version failed", KR(ret));
      } else if (OB_FAIL(partition_mgr_->update_max_replay_batch_commit_version(commit_version))) {
        TRANS_LOG(WARN, "update max replay batch commit version fail", KR(ret), K(commit_version));
      } else {
        // do nothing
      }
    }
    if (OB_SUCC(ret) && batch_committed) {
      batch_commit_state_ = ObBatchCommitState::BATCH_COMMITTED;
      int state = ObTransResultState::INVALID;
      if (ctx_dependency_wrap_.get_prev_trans_arr_count() <= 0) {
        can_checkpoint = true;
      } else if (OB_FAIL(ctx_dependency_wrap_.get_prev_trans_arr_result(state))) {
        TRANS_LOG(WARN, "get prev trans arr result error", K(ret), "context", *this);
      } else if (ObTransResultState::is_commit(state)) {
        can_checkpoint = true;
      } else if (ObTransResultState::is_abort(state)) {
        // The txn will be rollbacked finally through ABORT log
        need_print_trace_log_ = true;
        TRANS_LOG(WARN, "current transaction need to rollback because of prev abort trans", "context", *this);
        // The predecessor txns may not decide, we register to predecessors and
        // wait for callback inorder accelerate the progress of txn
      } else if (OB_FAIL(insert_all_prev_trans_(true))) {
        if (OB_EAGAIN != ret) {
          TRANS_LOG(WARN, "insert all prev trans error", KR(ret), "context", *this);
        }
        // If register fails, we can reply on later checkpoint
        ret = OB_SUCCESS;
      } else {
        need_print_trace_log_ = true;
        TRANS_LOG(DEBUG, "unknown transaction resulst state", KR(ret), "context", *this);
      }
      if (can_checkpoint && OB_FAIL(commit_by_checkpoint_(commit_version))) {
        TRANS_LOG(WARN, "commit by checkpoint failed", K(commit_version));
      }
    }
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "replay prepare log error", KR(ret), "context", *this, K(log));
  } else {
    is_in_redo_with_prepare_ = false;
    // When 1pc txns replay the prepare log, we need fill in the end_log_ts_ for
    // trans table garbage colloection. While the minor merge cannot be
    // permitted until checkpoint confirms the 1pc txn's state
    if (batch_commit_trans_) {
      if (!batch_committed || !can_checkpoint) {
        end_log_ts_for_batch_commit_ = timestamp;
        if (OB_FAIL(partition_service_->inc_pending_batch_commit_count(self_, mt_ctx_, timestamp))) {
          TRANS_LOG(WARN, "failed to inc batch commit count", K(ret), "context", *this);
        }
      } else {
        end_log_ts_ = timestamp;
      }
    }
  }
  REC_TRANS_TRACE_EXT(tlog_,
      replay_prepare,
      OB_ID(ret),
      ret,
      OB_ID(used),
      ObTimeUtility::fast_current_time() - start,
      OB_ID(id),
      log_id,
      OB_ID(t),
      timestamp,
      OB_ID(arg1),
      batch_commit_trans_,
      OB_ID(arg2),
      batch_committed,
      OB_ID(arg3),
      can_checkpoint,
      OB_ID(uref),
      get_uref());
  return ret;
}

int ObPartTransCtx::replay_commit_log(const ObTransCommitLog& log, const int64_t timestamp, const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const int64_t start = ObTimeUtility::fast_current_time();
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!for_replay_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "invalid state, transaction is not replaying", KR(ret), "context", *this);
    need_print_trace_log_ = true;
  } else if (OB_UNLIKELY(!log.is_valid()) || OB_UNLIKELY(timestamp < 0)) {
    TRANS_LOG(WARN, "invalid argument", K(log), K(timestamp), K(log_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(is_hazardous_ctx_)) {
    ret = OB_EAGAIN;
    TRANS_LOG(ERROR, "current ctx status is hazardous", K(ret), K(*this));
  } else if (OB_UNLIKELY(OB_LOG_TRANS_COMMIT != log.get_log_type())) {
    TRANS_LOG(WARN, "invalid log type", "log_type", log.get_log_type());
    ret = OB_TRANS_INVALID_LOG_TYPE;
  } else if (log.get_split_info().is_valid()) {
    if (OB_FAIL(replay_listener_commit_log(log, timestamp, log_id))) {
      TRANS_LOG(WARN, "failed to replay listener commit log", KR(ret), K(log));
    }
  } else if (OB_UNLIKELY(!is_trans_valid_for_replay_(OB_LOG_TRANS_COMMIT, timestamp))) {
    TRANS_LOG(WARN, "trans is not valid", K(*this), K(log_id), K(timestamp), K(log));
    ret = OB_TRANS_INVALID_STATE;
    need_print_trace_log_ = true;
  } else if (OB_FAIL(partition_log_info_arr_.assign(log.get_partition_log_info_array()))) {
    TRANS_LOG(WARN, "partition log array assign error", KR(ret), K(log));
  } else {
    bool is_state_durable = false;
    if (max_durable_log_ts_ > timestamp) {
      is_state_durable = true;
    }
    submit_log_count_ = 0;
    update_durable_log_id_ts_(OB_LOG_TRANS_COMMIT, log_id, timestamp);
    log_type_ = log.get_log_type();
    const uint64_t checksum = ((need_checksum_ && !is_state_durable) ? log.get_checksum() : 0);
    global_trans_version_ = log.get_global_trans_version();
    clear_log_base_ts_ = timestamp;
    if (OB_FAIL(trans_replay_commit_(global_trans_version_, checksum))) {
      TRANS_LOG(WARN, "transaction replay commit error", K(ret), K(log), "context", *this);
    } else if (OB_FAIL(trans_clear_())) {
      TRANS_LOG(WARN, "transaction clear error", K(ret), "context", *this);
    } else {
      set_state_(Ob2PCState::COMMIT);
      set_status_(OB_SUCCESS);
      if (coordinator_ == self_) {
        ObTransStatus trans_status(trans_id_, OB_SUCCESS);
        (void)trans_status_mgr_->set_status(trans_id_, trans_status);
      }

      if (cluster_id_ != log.get_cluster_id()) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR,
            "unexpected error, cluster id not match",
            KR(ret),
            "cluster_id_in_log",
            log.get_cluster_id(),
            K_(cluster_id),
            "context",
            *this);
        need_print_trace_log_ = true;
      }
    }
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "replay commit log error", KR(ret), "context", *this, K(log));
  } else {
    TRANS_LOG(DEBUG, "replay commit log success", "context", *this, K(log));
  }
  REC_TRANS_TRACE_EXT(tlog_,
      replay_commit,
      OB_ID(ret),
      ret,
      OB_ID(used),
      ObTimeUtility::fast_current_time() - start,
      OB_ID(id),
      log_id,
      OB_ID(t),
      timestamp,
      OB_ID(uref),
      get_uref());
  return ret;
}

// For pg restored in 3.x, restore_snapshot_version and last_restore_log_ts is used to
// rollback trans which is restored and superfluous
//
// For pg restored in 2.x, last_restore_log_id is used instead of last_restore_log_ts,
// as last_restore_log_ts is not maintained in pg restored from 2.x
bool ObPartTransCtx::need_rollback_when_restore_(const int64_t commit_version)
{
  bool bret = false;
  const int64_t restore_snapshot_version = partition_mgr_->get_restore_snapshot_version();
  const uint64_t last_restore_log_id = partition_mgr_->get_last_restore_log_id();
  const int64_t last_restore_log_ts = partition_mgr_->get_last_restore_log_ts();
  // restore_snapshot_version is invalid, all trans need not rollback
  if (OB_INVALID_TIMESTAMP == restore_snapshot_version) {
    bret = false;
  } else if (OB_INVALID_TIMESTAMP != last_restore_log_ts) {
    // last_restore_log_ts is valid, pg is restored in 3.x
    bret = min_log_ts_ <= last_restore_log_ts && commit_version > restore_snapshot_version;
  } else if (OB_INVALID_ID != last_restore_log_id) {
    // last_restore_log_ts is invalid and last_restore_log_id is valid, pg is restored in 2.x
    bret = min_log_id_ <= last_restore_log_id && commit_version > restore_snapshot_version;
  } else {
    // last_restore_log_ts and last_restore_log_id are invalid, pg is in restoring
    bret = commit_version > restore_snapshot_version;
  }
  return bret;
}

bool ObPartTransCtx::need_update_schema_version(const uint64_t log_id, const int64_t log_ts)
{
  const int64_t restore_snapshot_version = partition_mgr_->get_restore_snapshot_version();
  const int64_t last_restore_log_id = partition_mgr_->get_last_restore_log_id();
  bool need_update = true;
  if (restore_snapshot_version > 0 && (last_restore_log_id == OB_INVALID_ID || log_id <= last_restore_log_id) &&
      (log_ts > restore_snapshot_version)) {
    need_update = false;
  }
  return need_update;
}

int ObPartTransCtx::trans_replay_commit_(const int64_t commit_version, const int64_t checksum)
{
  ObTimeGuard tg("trans_replay_commit", 50 * 1000);
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_FAIL(update_publish_version_(commit_version, true))) {
    TRANS_LOG(WARN, "update publish version error", K(ret), K(commit_version), "context", *this);
  } else if (need_rollback_when_restore_(commit_version)) {
    // We need rollback unneeded txn during restore
    TRANS_LOG(INFO, "transaction rollback when restore", K(*this), K(commit_version), K(*partition_mgr_));
    if (OB_FAIL(mt_ctx_.trans_replay_end(false, commit_version, checksum))) {
      TRANS_LOG(WARN, "transaction replay end error", K(ret), K(commit_version), K(checksum), "context", *this);
    } else {
      tg.click();
      if (can_elr_ && OB_SUCCESS != (tmp_ret = register_trans_result_info_(ObTransResultState::COMMIT))) {
        TRANS_LOG(WARN, "update trans result info error", K(tmp_ret), "context", *this);
      }
      tg.click();
      if (OB_SUCCESS != (tmp_ret = ctx_dependency_wrap_.callback_next_trans_arr(ObTransResultState::COMMIT))) {
        TRANS_LOG(WARN, "callback next trans array error", K(tmp_ret), "context", *this);
      }
      tg.click();
    }
  } else {
    bool is_dirty = false;
    int64_t freeze_ts = 0;
    if (OB_FAIL(mt_ctx_.trans_replay_end(true, commit_version, checksum))) {
      TRANS_LOG(WARN, "transaction replay end error", KR(ret), K(commit_version), K(checksum), "context", *this);
    } else if (OB_FAIL(mt_ctx_.trans_publish())) {
      TRANS_LOG(WARN, "transaction publish error", KR(ret), "context", *this);
    } else {
      tg.click();
      if (can_elr_ && OB_SUCCESS != (tmp_ret = register_trans_result_info_(ObTransResultState::COMMIT))) {
        TRANS_LOG(WARN, "update trans result info error", K(tmp_ret), "context", *this);
      }
      tg.click();
      if (OB_SUCCESS != (tmp_ret = ctx_dependency_wrap_.callback_next_trans_arr(ObTransResultState::COMMIT))) {
        TRANS_LOG(WARN, "callback next trans array error", K(tmp_ret), "context", *this);
      }
      tg.click();
      if (OB_FAIL(partition_service_->check_dirty_txn(self_, min_log_ts_, end_log_ts_, freeze_ts, is_dirty))) {
        TRANS_LOG(WARN, "check dirty txn error", KR(ret), K(min_log_ts_), K(end_log_ts_), "context", *this);
      } else if (is_dirty) {
        if (mark_dirty_trans()) {
          TRANS_LOG(INFO,
              "mark dirty for follower crossing multiple memtables",
              KR(ret),
              K(min_log_ts_),
              K(end_log_ts_),
              K(freeze_ts),
              "context",
              *this);
          REC_TRANS_TRACE_EXT(tlog_,
              mark_dirty_trans_accross_memtable,
              OB_ID(ret),
              ret,
              OB_ID(arg1),
              min_log_ts_,
              OB_ID(arg2),
              end_log_ts_,
              OB_ID(arg3),
              freeze_ts,
              OB_ID(commit),
              true);
        }
      }
      tg.click();
    }
  }

  return ret;
}

int ObPartTransCtx::update_publish_version_(const int64_t publish_version, const bool for_replay)
{
  int ret = OB_SUCCESS;
  ObITsMgr* ts_mgr = get_ts_mgr_();
  if (OB_UNLIKELY(0 >= publish_version)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(publish_version));
  } else if (OB_ISNULL(ts_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "ts mgr is null", KR(ret));
  } else if (OB_FAIL(ts_mgr->update_publish_version(self_.get_tenant_id(), publish_version, for_replay))) {
    TRANS_LOG(WARN, "update publish version failed", KR(ret), K(publish_version), "context", *this);
  } else if (OB_FAIL(update_global_trans_version_(publish_version))) {
    TRANS_LOG(WARN, "update global trans version fail", KR(ret), K(global_trans_version_), K(publish_version));
  } else if (OB_FAIL(partition_mgr_->update_max_trans_version(publish_version))) {
    TRANS_LOG(WARN, "update partition publish version failed", KR(ret));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartTransCtx::replay_abort_log(const ObTransAbortLog& log, const int64_t timestamp, const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const int64_t start = ObTimeUtility::fast_current_time();
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!for_replay_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "invalid state, transaction is not replaying", KR(ret), "context", *this);
    need_print_trace_log_ = true;
  } else if (OB_UNLIKELY(!log.is_valid()) || OB_UNLIKELY(timestamp < 0)) {
    TRANS_LOG(WARN, "invalid argument", K(log), K(timestamp), K(log_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(OB_LOG_TRANS_ABORT != log.get_log_type())) {
    TRANS_LOG(WARN, "invalid log type", "log_type", log.get_log_type());
    ret = OB_TRANS_INVALID_LOG_TYPE;
  } else if (log.get_split_info().is_valid()) {
    if (OB_FAIL(replay_listener_abort_log(log, timestamp, log_id))) {
      TRANS_LOG(WARN, "failed to replay listener abort log", KR(ret), K(log));
    }
  } else if (OB_UNLIKELY(!is_trans_valid_for_replay_(OB_LOG_TRANS_ABORT, timestamp))) {
    TRANS_LOG(WARN, "trans is not valid", K(*this), K(log_id), K(timestamp), K(log));
    ret = OB_TRANS_INVALID_STATE;
    need_print_trace_log_ = true;
  } else if (OB_FAIL(mt_ctx_.trans_replay_end(false, global_trans_version_))) {
    TRANS_LOG(WARN, "transaction replay end error", KR(ret), "context", *this);
  } else {
    if (OB_SUCCESS != (tmp_ret = ctx_dependency_wrap_.callback_next_trans_arr(ObTransResultState::ABORT))) {
      TRANS_LOG(WARN, "callback next trans array error", K(tmp_ret), "context", *this);
    }
    submit_log_count_ = 0;
    update_durable_log_id_ts_(OB_LOG_TRANS_ABORT, log_id, timestamp);
    log_type_ = log.get_log_type();
    set_status_(OB_TRANS_ROLLBACKED);
    if (coordinator_ == self_) {
      ObTransStatus trans_status(trans_id_, OB_TRANS_ROLLBACKED);
      (void)trans_status_mgr_->set_status(trans_id_, trans_status);
    }

    set_state_(Ob2PCState::ABORT);
    if (cluster_id_ != log.get_cluster_id()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR,
          "unexpected error, cluster id not match",
          KR(ret),
          "cluster_id_in_log",
          log.get_cluster_id(),
          K_(cluster_id),
          "context",
          *this);
      need_print_trace_log_ = true;
    }
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "replay abort log error", KR(ret), "context", *this, K(log));
  } else {
    TRANS_LOG(DEBUG, "replay abort log success", "context", *this, K(log));
  }
  REC_TRANS_TRACE_EXT(tlog_,
      replay_abort,
      OB_ID(ret),
      ret,
      OB_ID(used),
      ObTimeUtility::fast_current_time() - start,
      OB_ID(id),
      log_id,
      OB_ID(t),
      timestamp,
      OB_ID(uref),
      get_uref());
  return ret;
}

int ObPartTransCtx::replay_clear_log(const ObTransClearLog& log, const int64_t timestamp, const uint64_t log_id)
{
  int ret = OB_SUCCESS;

  const int64_t start = ObTimeUtility::fast_current_time();
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!for_replay_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "invalid state, transaction is not replaying", KR(ret), "context", *this);
    need_print_trace_log_ = true;
  } else if (OB_UNLIKELY(!log.is_valid()) || OB_UNLIKELY(timestamp < 0)) {
    TRANS_LOG(WARN, "invalid argument", K(log), K(timestamp), K(log_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(OB_LOG_TRANS_CLEAR != log.get_log_type())) {
    TRANS_LOG(WARN, "invalid log type", "log_type", log.get_log_type());
    ret = OB_TRANS_INVALID_LOG_TYPE;
  } else if (OB_UNLIKELY(!is_trans_valid_for_replay_(OB_LOG_TRANS_CLEAR, timestamp))) {
    if (OB_ISNULL(partition_service_)) {
      TRANS_LOG(ERROR, "unexpected partition service ptr", KP_(partition_service), "context", *this);
      ret = OB_ERR_UNEXPECTED;
    } else if (static_cast<storage::ObPartitionService*>(partition_service_)->is_running()) {
      TRANS_LOG(WARN, "trans is not valid", K(*this), K(log_id), K(log));
      ret = OB_TRANS_INVALID_STATE;
      need_print_trace_log_ = true;
    } else {
      ret = OB_NOT_RUNNING;
    }
  } else if (OB_FAIL(trans_clear_())) {
    TRANS_LOG(WARN, "transaction clear error", KR(ret), "context", *this);
  } else {
    submit_log_count_ = 0;
    update_durable_log_id_ts_(OB_LOG_TRANS_CLEAR, log_id, timestamp);
    log_type_ = log.get_log_type();
    set_state_(Ob2PCState::CLEAR);
    set_exiting_();
    if (cluster_id_ != log.get_cluster_id()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR,
          "unexpected error, cluster id not match",
          KR(ret),
          "cluster_id_in_log",
          log.get_cluster_id(),
          K_(cluster_id),
          "context",
          *this);
      need_print_trace_log_ = true;
    }
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "replay clear log error", KR(ret), "context", *this, K(log));
  } else {
    TRANS_LOG(DEBUG, "replay clear log success", "context", *this, K(log));
  }
  REC_TRANS_TRACE_EXT(tlog_,
      replay_clear,
      OB_ID(ret),
      ret,
      OB_ID(used),
      ObTimeUtility::fast_current_time() - start,
      OB_ID(id),
      log_id,
      OB_ID(t),
      timestamp,
      OB_ID(uref),
      get_uref());
  return ret;
}

int ObPartTransCtx::replay_trans_state_log(const ObTransStateLog& log, const int64_t timestamp, const uint64_t log_id)
{
  int ret = OB_SUCCESS;

  const int64_t start = ObTimeUtility::fast_current_time();
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!for_replay_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "invalid state, transaction is not replaying", KR(ret), "context", *this);
    need_print_trace_log_ = true;
  } else if (OB_UNLIKELY(!log.is_valid()) || OB_UNLIKELY(timestamp < 0)) {
    TRANS_LOG(WARN, "invalid argument", K(log), K(timestamp), K(log_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(is_hazardous_ctx_)) {
    ret = OB_EAGAIN;
    TRANS_LOG(ERROR, "current ctx status is hazardous", K(ret), K(*this));
  } else if (OB_UNLIKELY(OB_LOG_TRANS_STATE != log.get_log_type())) {
    ret = OB_TRANS_INVALID_LOG_TYPE;
    TRANS_LOG(WARN, "invalid log type", KR(ret), "log_type", log.get_log_type());
  } else if (log.get_proposal_leader() != addr_ || (start - log.get_create_ts() > 3000000) ||
             cluster_version_before_2271_()) {
    submit_log_count_ = 0;
    has_trans_state_log_ = true;
    is_trans_state_sync_finished_ = false;
    TRANS_LOG(INFO, "discard trans state log", "context", *this, K(log));
  } else if (OB_UNLIKELY(!is_trans_valid_for_replay_(OB_LOG_TRANS_STATE, timestamp))) {
    TRANS_LOG(WARN, "trans is not valid", K(*this), K(log_id), K(timestamp), K(log));
    ret = OB_TRANS_INVALID_STATE;
    need_print_trace_log_ = true;
  } else if (OB_FAIL(ctx_dependency_wrap_.prev_trans_arr_assign(log.get_prev_trans_arr()))) {
    TRANS_LOG(WARN, "prev transaction array assign error", KR(ret), K(log));
  } else if (OB_FAIL(set_app_trace_id_(log.get_app_trace_id_str()))) {
    TRANS_LOG(WARN, "set app trace id error", K(ret), K(log), K(*this));
  } else if (OB_FAIL(set_xid_(log.get_xid()))) {
    TRANS_LOG(WARN, "set xid error", K(ret), K(log));
  } else {
    submit_log_count_ = 0;
    if (can_elr_ != log.is_can_elr()) {
      TRANS_LOG(WARN, "different can elr state", K(log), K(*this));
    }
    can_elr_ = log.is_can_elr();
    log_type_ = log.get_log_type();
    scheduler_ = log.get_scheduler();
    is_readonly_ = log.is_readonly();
    // Single-partitioned txn need be recovered to a distributed txn after
    // leader transferred
    trans_type_ = TransType::DIST_TRANS;
    session_id_ = log.get_session_id();
    proxy_session_id_ = log.get_proxy_session_id();
    cluster_id_ = log.get_cluster_id();
    commit_task_count_ = log.get_commit_task_count();
    stmt_info_ = log.get_stmt_info();
    if (INT64_MAX == log.get_schema_version()) {
      TRANS_LOG(WARN, "current table version is invalid, no need to update", K(log), K(*this));
    } else {
      mt_ctx_.set_table_version(log.get_schema_version());
    }
    mt_ctx_.set_prepare_version(timestamp);
    snapshot_version_ = log.get_snapshot_version();
    mt_ctx_.set_read_snapshot(snapshot_version_);
    cur_query_start_time_ = log.get_cur_query_start_time();
    stmt_expired_time_ = log.get_stmt_expired_time();
    is_trans_state_sync_finished_ = true;
    if (!stmt_info_.is_stmt_ended()) {
      can_rollback_stmt_ = false;
    }
    has_trans_state_log_ = true;
    TRANS_LOG(INFO, "replay trans state log success", "context", *this, K(log), K(log_id));
  }
  if (OB_SUCC(ret)) {
    update_durable_log_id_ts_(OB_LOG_TRANS_STATE, log_id, timestamp);
  } else {
    TRANS_LOG(WARN, "replay trans state log error", KR(ret), "context", *this, K(log), K(log_id));
  }

  REC_TRANS_TRACE_EXT(tlog_,
      replay_trans_state,
      Y(ret),
      OB_ID(used),
      ObTimeUtility::fast_current_time() - start,
      Y(log_id),
      OB_ID(t),
      timestamp,
      OB_ID(uref),
      get_uref());
  return ret;
}

int ObPartTransCtx::get_compat_mode_(ObWorker::CompatMode& mode)
{
  int ret = OB_SUCCESS;

  // If the compact mode is not generated, we drive to generate it. Otherwise we
  // update the thread-local compact mode
  if (partition_mgr_->has_valid_compact_mode_()) {
    mode = static_cast<ObWorker::CompatMode>(partition_mgr_->get_compact_mode_());
  } else {
    CREATE_WITH_TEMP_ENTITY(TABLE_SPACE, self_.get_table_id())
    {
      mode = THIS_WORKER.get_compatibility_mode();
      if (OB_FAIL(partition_mgr_->set_compact_mode_((int)mode))) {
        TRANS_LOG(WARN, "set compact mode error", KR(ret), K(mode), "context", *this);
      }
    }
    else
    {
      mode = ObWorker::CompatMode::MYSQL;
      // TODO: The redo log need be retried until tenant persistence has been permitted
      // ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "compact mode not ready", KR(ret), K(mode), "context", *this);
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObPartTransCtx::replay_trans_mutator_log(
    const ObTransMutatorLog& log, const int64_t timestamp, const uint64_t log_id, int64_t& log_table_version)
{
  int ret = OB_SUCCESS;
  ObWorker::CompatMode mode = ObWorker::CompatMode::INVALID;

  const int64_t start = ObTimeUtility::fast_current_time();
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!for_replay_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "invalid state, transaction is not replaying", KR(ret), "context", *this);
    need_print_trace_log_ = true;
  } else if (OB_UNLIKELY(!log.is_valid()) || OB_UNLIKELY(timestamp < 0)) {
    TRANS_LOG(WARN, "invalid argument", K(log), K(timestamp), K(log_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(is_hazardous_ctx_)) {
    ret = OB_EAGAIN;
    TRANS_LOG(ERROR, "current ctx status is hazardous", K(ret), K(*this));
  } else if (OB_UNLIKELY(OB_LOG_MUTATOR != log.get_log_type())) {
    ret = OB_TRANS_INVALID_LOG_TYPE;
    TRANS_LOG(WARN, "invalid log type", K(ret), "log_type", log.get_log_type());
  } else if (OB_UNLIKELY(!is_trans_valid_for_replay_(OB_LOG_MUTATOR, timestamp))) {
    TRANS_LOG(WARN, "trans is not valid", K(*this), K(log_id), K(timestamp), K(log));
    ret = OB_TRANS_INVALID_STATE;
    need_print_trace_log_ = true;
  } else if (log_id <= last_replayed_redo_log_id_) {
    TRANS_LOG(INFO, "redo log has replayed", K(log_id), K(log), K(*this));
  } else if (OB_FAIL(ctx_dependency_wrap_.prev_trans_arr_assign(log.get_prev_trans_arr()))) {
    TRANS_LOG(WARN, "prev transaction array assign error", KR(ret), K(log));
  } else if (submit_log_count_ > 0) {
    mt_ctx_.sync_log_succ(log_id, timestamp);
    submit_log_count_ = 0;
    if (timestamp > max_durable_log_ts_ && OB_FAIL(prev_redo_log_ids_.push_back(log_id))) {
      TRANS_LOG(WARN, "redo log id push back error", KR(ret), K(log_id));
    } else {
      if (timestamp > max_durable_log_ts_) {
        ++mutator_log_no_;
      }
      need_checksum_ = need_checksum_ || (0 == log.get_log_no());
    }
    update_durable_log_id_ts_(OB_LOG_MUTATOR, log_id, timestamp);
    has_write_or_replay_mutator_redo_log_ = true;
    TRANS_LOG(INFO, "replay self log", K(log_id), K(*this));
  } else if (OB_FAIL(get_compat_mode_(mode))) {
    TRANS_LOG(WARN, "check and update compact mode error", KR(ret), "context", *this);
  } else {
    share::CompatModeGuard compat_guard(mode);
    if (can_elr_ != log.is_can_elr()) {
      TRANS_LOG(WARN, "different can elr state", K(log), K(*this));
    }
    can_elr_ = log.is_can_elr();
    log_type_ = log.get_log_type();
    ObStoreCtx store_ctx;
    store_ctx.mem_ctx_ = &mt_ctx_;
    store_ctx.tenant_id_ = self_.get_tenant_id();
    store_ctx.log_ts_ = timestamp;
    const ObTransMutator &mutator = log.get_mutator();
    bool replayed = true;
    if (OB_FAIL(partition_service_->replay_redo_log(
            self_, store_ctx, timestamp, log_id, mutator.get_data(), mutator.get_position(), replayed))) {
      if (OB_ALLOCATE_MEMORY_FAILED != ret || EXECUTE_COUNT_PER_SEC(100)) {
        TRANS_LOG(WARN, "replay redo log error", KR(ret), "partition", self_, K(log));
      }

      if (OB_TRANS_WAIT_SCHEMA_REFRESH == ret) {
        log_table_version = store_ctx.mem_ctx_->get_max_table_version();
      }
    } else if (timestamp > max_durable_log_ts_ && OB_FAIL(prev_redo_log_ids_.push_back(log_id))) {
      TRANS_LOG(WARN, "redo log id push back error", KR(ret), K(timestamp));
    } else if (replayed) {
      if (timestamp > max_durable_log_ts_) {
        ++mutator_log_no_;
      }
      need_checksum_ = need_checksum_ || (0 == log.get_log_no());
      has_write_or_replay_mutator_redo_log_ = true;
    } else {
      TRANS_LOG(INFO, "no need to replay, maybe partition already removed", K(log), K(*this));
    }
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "replay trans mutator log error", KR(ret), "context", *this, K(log), K(log_id));
  } else {
    last_replayed_redo_log_id_ = log_id;
    update_durable_log_id_ts_(OB_LOG_MUTATOR, log_id, timestamp);
    TRANS_LOG(INFO, "replay trans mutator log success", "context", *this, K(log), K(log_id));
  }
  REC_TRANS_TRACE_EXT(tlog_,
      replay_trans_mutator,
      Y(ret),
      OB_ID(used),
      ObTimeUtility::fast_current_time() - start,
      Y(log_id),
      OB_ID(t),
      timestamp,
      OB_ID(uref),
      get_uref());
  return ret;
}

int ObPartTransCtx::replay_mutator_abort_log(
    const ObTransMutatorAbortLog& log, const int64_t timestamp, const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const int64_t start = ObClockGenerator::getClock();
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!for_replay_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "invalid state, transaction is not replaying", KR(ret), "context", *this);
    need_print_trace_log_ = true;
  } else if (OB_UNLIKELY(!log.is_valid()) || OB_UNLIKELY(timestamp < 0)) {
    TRANS_LOG(WARN, "invalid argument", K(log), K(timestamp), K(log_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(OB_LOG_MUTATOR_ABORT != log.get_log_type())) {
    TRANS_LOG(WARN, "invalid log type", "log_type", log.get_log_type());
    ret = OB_TRANS_INVALID_LOG_TYPE;
  } else if (OB_UNLIKELY(!is_trans_valid_for_replay_(OB_LOG_MUTATOR_ABORT, timestamp))) {
    TRANS_LOG(WARN, "trans is not valid", K(*this), K(log_id), K(timestamp), K(log));
    ret = OB_TRANS_INVALID_STATE;
    need_print_trace_log_ = true;
  } else if (OB_FAIL(mt_ctx_.trans_replay_end(false, global_trans_version_))) {
    TRANS_LOG(WARN, "transaction replay end error", KR(ret), "context", *this);
  } else if (OB_FAIL(trans_clear_())) {
    TRANS_LOG(WARN, "transaction clear error", KR(ret), "context", *this);
  } else {
    submit_log_count_ = 0;
    update_durable_log_id_ts_(OB_LOG_MUTATOR_ABORT, log_id, timestamp);
    log_type_ = log.get_log_type();
    set_state_(ObRunningState::ABORT);
    set_status_(OB_TRANS_ROLLBACKED);
    end_trans_callback_(OB_TRANS_KILLED);
    set_exiting_();
    if (cluster_id_ != log.get_cluster_id()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR,
          "unexpected error, cluster id not match",
          KR(ret),
          "cluster_id_in_log",
          log.get_cluster_id(),
          K_(cluster_id),
          "context",
          *this);
      need_print_trace_log_ = true;
    }
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "replay mutator abort log error", KR(ret), "context", *this, K(log));
  } else {
    TRANS_LOG(INFO, "replay mutator abort log success", "context", *this, K(log));
  }
  REC_TRANS_TRACE_EXT(tlog_,
      replay_abort,
      OB_ID(ret),
      ret,
      OB_ID(used),
      ObTimeUtility::fast_current_time() - start,
      OB_ID(id),
      log_id,
      OB_ID(t),
      timestamp,
      OB_ID(uref),
      get_uref());
  return ret;
}

int ObPartTransCtx::replay_start_working_log(const int64_t timestamp, const uint64_t log_id)
{
  int ret = OB_SUCCESS;

  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (is_exiting_) {
    // do nothing
  } else if (OB_UNLIKELY(!for_replay_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "invalid state, transaction is not replaying", KR(ret), "context", *this);
    need_print_trace_log_ = true;
  } else if (OB_UNLIKELY(!is_trans_valid_for_replay_(OB_LOG_START_MEMBERSHIP_STORAGE, timestamp))) {
    TRANS_LOG(WARN, "trans is not valid", K(*this), K(log_id), K(timestamp), K(timestamp));
    ret = OB_TRANS_INVALID_STATE;
    need_print_trace_log_ = true;
  } else if (0 == submit_log_count_) {
    if (has_logged_() && !is_in_2pc_() && !is_trans_state_sync_finished_ && is_changing_leader_) {
      // - When replaying start working:
      //   - Case 3.2: If txn has no on-the-fly log and no trans state is synced by the leader
      //     transfer(txn may need abort, while we donot have the information whether the
      //     original leader successfully synced the log), and we also remove all marked trans node.
      (void)mt_ctx_.clean_dirty_callbacks();

      TRANS_LOG(INFO, "clean dirty callbacks when replay start working", K(*this));
    }
  } else {
    need_print_trace_log_ = true;
    if (!has_logged_() && !is_in_2pc_() && !is_hazardous_ctx_) {
      // We need kill all txn which has not subbmitted log or not in 2pc before
      // leader transfer
      TRANS_LOG(INFO, "kill trans when replay start working log", K(*this));
      end_trans_callback_(OB_TRANS_KILLED);
      trans_kill_();
      (void)trans_clear_();
      // We can also clean dirty txn who has never logged before because the
      // data can only be mini merged after clog is saved to
      // majority(TODO: need provement)
      is_dirty_ = false;
      set_exiting_();
    } else if (has_logged_() && !is_in_2pc_() && submit_log_count_ > 0) {
      // - When replaying start working:
      //   - Case 3.1: If txn has a on-the-fly log, it means some logs are not paxos-choosen
      //     successfully(txn need abort), so we remove all marked trans node
      (void)mt_ctx_.clean_dirty_callbacks();

      // Because current log is not majoritied, and some logs have been
      // majoritied, we need wait for abort log by new leader
      TRANS_LOG(INFO, "no need to kill trans when replay start working log", K(*this));
    }

    submit_log_count_ = 0;
    TRANS_STAT_ABORT_TRANS_INC(tenant_id_);
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(update_max_majority_log(log_id, timestamp))) {
      TRANS_LOG(ERROR, "update max majority log failed", K(ret), K(*this));
    }
  }

  REC_TRANS_TRACE_EXT(tlog_, replay_start_working_log, OB_ID(ret), ret, OB_ID(uref), get_uref());

  return ret;
}

int ObPartTransCtx::update_max_majority_log(const uint64_t log_id, const int64_t log_ts)
{
  int ret = OB_SUCCESS;
  storage::ObIPartitionGroupGuard pg_guard;

  if (OB_NOT_NULL(pg_)) {
    if (OB_FAIL(pg_->update_max_majority_log(log_id, log_ts))) {
      TRANS_LOG(WARN, "update max majority log error", K(*this));
    }
  } else if (OB_FAIL(partition_service_->get_partition(self_, pg_guard))) {
    TRANS_LOG(WARN, "get partition error", KR(ret), "context", *this);
  } else if (NULL == pg_guard.get_partition_group()) {
    TRANS_LOG(ERROR, "partition is null, unexpected error", KP(pg_guard.get_partition_group()), "context", *this);
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(pg_guard.get_partition_group()->update_max_majority_log(log_id, log_ts))) {
    TRANS_LOG(WARN, "update max majority log error", K(*this));
  }

  return ret;
}

// The txn that have been prepared but not yet committed
bool ObPartTransCtx::is_prepared() const
{
  const int64_t state = get_state_();

  return ((state >= Ob2PCState::PREPARE && state < Ob2PCState::COMMIT) || (redo_log_no_ > 0 && is_sp_trans_()));
}

// The txn has written redo or in the state between prepare and clear
bool ObPartTransCtx::is_in_2pc_() const
{
  bool bool_ret = false;
  const int64_t state = get_state_();
  switch (state) {
    case Ob2PCState::UNKNOWN: {
      bool_ret = false;
      break;
    }
    case Ob2PCState::INIT:
    // go through
    case Ob2PCState::PRE_PREPARE: {
      if (0 < redo_log_no_) {
        bool_ret = true;
      }
      break;
    }
    case ObRunningState::ABORT: {
      bool_ret = false;
      break;
    }
    case Ob2PCState::PREPARE:
    // go through
    case ObSpState::PREPARE:
    // go through
    case Ob2PCState::COMMIT:
    // go through
    case ObSpState::COMMIT:
    // go through
    case Ob2PCState::ABORT:
    // go through
    case ObSpState::ABORT:
    // go through
    case Ob2PCState::CLEAR: {
      bool_ret = true;
      break;
    }
    default: {
      TRANS_LOG(WARN, "invalid 2pc state", "context", *this);
    }
  }
  return bool_ret;
}

bool ObPartTransCtx::is_pre_preparing_() const
{
  return (state_.get_state() == Ob2PCState::PRE_PREPARE);
}

bool ObPartTransCtx::is_logging_() const
{
  return submit_log_count_ > 0 || is_gts_waiting_;
}

bool ObPartTransCtx::has_logged_() const
{
  return (mutator_log_no_ > 0 || has_trans_state_log_ || redo_log_no_ > 0);
}

bool ObPartTransCtx::need_record_log() const
{
  // There are three variables can be very large : participants array, redo log array and undo
  // actions array. In order to avoiding the serialize size of ObPartTransCtx is too large, which
  // may larger than 1MB, we have to limit the size of these three variables.  We set the
  // following principles:
  //
  // 1. The redo_log can use at least 128KB
  // 2. All the three variables can use 1014KB(MAX_VARCHAR_LENGTH-10KB)
  //
  // In this function, we sum the serialize size of these three fileds to decide if this transaction
  // need write record log. So this function return true implicates the serialize size of these
  // three variables is larger than 1014KB and we need write record log to make sure the trans state
  // table can be dumped successfully.
  bool bool_ret = false;
  int64_t participants_serialize_size = participants_.get_serialize_size();
  int64_t undo_serialize_size = undo_status_.get_serialize_size();
  int64_t redo_log_id_serialize_size = prev_redo_log_ids_.get_serialize_size();
  int total_size = participants_serialize_size + undo_serialize_size + redo_log_id_serialize_size;
  if (total_size > OB_MAX_TRANS_SERIALIZE_SIZE) {
    bool_ret = true;
    TRANS_LOG(INFO,
        "need flush record log.",
        K(participants_serialize_size),
        K(undo_serialize_size),
        K(redo_log_id_serialize_size),
        K(total_size),
        K(OB_MAX_TRANS_SERIALIZE_SIZE),
        KPC(this));
  }

  return bool_ret;
}

int ObPartTransCtx::reserve_log_header_(char *buf, const int64_t size, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  const int64_t log_type = storage::OB_LOG_UNKNOWN;
  const int64_t idx = 0;

  if (OB_ISNULL(buf) || OB_UNLIKELY(size < 0)) {
    TRANS_LOG(WARN, "invalid argument", KP(buf), K(size), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::encode_i64(buf, size, tmp_pos, log_type))) {
    TRANS_LOG(WARN, "serialize log type error", KR(ret), K(log_type));
  } else if (OB_FAIL(serialization::encode_i64(buf, size, tmp_pos, idx))) {
    TRANS_LOG(WARN, "serialize idx error", KR(ret), K(log_type), K(idx));
  } else {
    pos = tmp_pos;
  }

  return ret;
}

int ObPartTransCtx::fill_redo_log_(char* buf, const int64_t size, int64_t& pos, int64_t& mutator_size)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  int64_t available_capacity = 0;
  ObTimeGuard timeguard("fill_redo_log", 10 * 1000);
  ObElrTransInfoArray trans_array;

  {
    ObElrTransArrGuard guard;
    if (OB_FAIL(ctx_dependency_wrap_.get_prev_trans_arr_guard(guard))) {
      TRANS_LOG(WARN, "get prev trans arr guard error", K(ret), K(*this));
    } else if (OB_FAIL(trans_array.assign(guard.get_elr_trans_arr()))) {
      TRANS_LOG(WARN, "assign trans array failed", K(ret));
    } else {
      // do nothing
    }
  }
  if (OB_SUCC(ret)) {
    ObTransRedoLog log(OB_LOG_TRANS_REDO,
        self_,
        trans_id_,
        tenant_id_,
        redo_log_no_ + mutator_log_no_,
        scheduler_,
        coordinator_,
        participants_,
        trans_param_,
        cluster_id_,
        trans_array,
        can_elr_,
        xid_,
        false /*is_last*/);
    if (OB_FAIL(log.init())) {
      TRANS_LOG(WARN, "init redo log error", K(ret));
    } else {
      // Besides the reserved size and redo log header, the rest is used to fill
      // the mutator
      available_capacity = size - OB_TRANS_REDO_LOG_RESERVE_SIZE - log.get_serialize_size();
      ObTransMutator& mutator = log.get_mutator();
      if (available_capacity > mutator.get_capacity()) {
        available_capacity = mutator.get_capacity();
      }
      if (available_capacity <= 0) {
        TRANS_LOG(WARN, "size overflow", K(size), K(available_capacity));
        ret = OB_ERR_UNEXPECTED;
      } else {
        timeguard.click();
        ret = mt_ctx_.fill_redo_log(mutator.get_data(), available_capacity, mutator.get_position());
        timeguard.click();
        if (OB_SUCCESS == ret) {
            log.set_last();
          has_gen_last_redo_log_ = true;
        }
        // For XA txn, if the partition has no mutator, we also need append a
        // new empty redo log. If we receive the XA commit directly, the
        // partition with no mutator neednot append the empty redo log
        if (in_xa_prepare_state_ && 0 == redo_log_no_ && OB_ENTRY_NOT_EXIST == ret) {
          last_redo_log_mutator_size_ = 0;
          int tmp_ret = OB_SUCCESS;
          ret = OB_SUCCESS;
          log.set_last();
          has_gen_last_redo_log_ = true;
          if (OB_SUCCESS != (tmp_ret = log.serialize(buf, size, tmp_pos))) {
            TRANS_LOG(WARN, "serialize redo log error", "ret", tmp_ret, K(log));
            mt_ctx_.undo_fill_redo_log();
            ret = tmp_ret;
          } else {
            pos = tmp_pos;
            mutator_size = mutator.get_position();
          }
        } else if (OB_SUCCESS == ret || OB_EAGAIN == ret) {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = log.serialize(buf, size, tmp_pos))) {
            TRANS_LOG(WARN, "serialize redo log error", "ret", tmp_ret, K(log));
            mt_ctx_.undo_fill_redo_log();
            ret = tmp_ret;
          } else {
            pos = tmp_pos;
            mutator_size = mutator.get_position();
            last_redo_log_mutator_size_ = mutator.get_position();
          }
        } else if (OB_ENTRY_NOT_EXIST == ret) {
          last_redo_log_mutator_size_ = 0;
        } else {
          TRANS_LOG(WARN, "fill redo log error", K(ret));
        }
        timeguard.click();
      }
    }
  }
  if (timeguard.get_diff() > 5 * 1000) {
    TRANS_LOG(WARN, "fill redo log cost too much time", K_(trans_id), K_(self), K(timeguard));
  }

  return ret;
}

int ObPartTransCtx::fill_prepare_log_(char* buf, const int64_t size, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  int64_t checkpoint = 0;

  if (OB_ISNULL(buf) || OB_UNLIKELY(size < 0)) {
    TRANS_LOG(WARN, "invalid argument", KP(buf), K(size), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(get_checkpoint_(checkpoint))) {
    TRANS_LOG(WARN, "get checkpoint failed", KR(ret), "context", *this);
  } else {
    ObElrTransArrGuard guard;
    if (OB_FAIL(ctx_dependency_wrap_.get_prev_trans_arr_guard(guard))) {
      TRANS_LOG(WARN, "get prev trans arr guard error", K(ret), K(*this));
    } else {
      ObTransPrepareLog log(OB_LOG_TRANS_PREPARE,
          self_,
          trans_id_,
          tenant_id_,
          scheduler_,
          coordinator_,
          participants_,
          trans_param_,
          get_status_(),
          prev_redo_log_ids_,
          cluster_id_,
          trace_info_.get_app_trace_id(),
          partition_log_info_arr_,
          checkpoint,
          guard.get_elr_trans_arr(),
          can_elr_,
          trace_info_.get_app_trace_info(),
          xid_,
          prev_checkpoint_id_);
      if (OB_UNLIKELY(!log.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "invalid prepare log", K(ret), K(log));
      } else if (OB_FAIL(log.serialize(buf, size, tmp_pos))) {
        TRANS_LOG(WARN, "serialize prepare log error", K(ret), K(log));
      } else {
        pos = tmp_pos;
      }
    }
  }

  return ret;
}

int ObPartTransCtx::fill_commit_log_(char* buf, const int64_t size, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;

  if (OB_ISNULL(buf) || OB_UNLIKELY(size < 0)) {
    TRANS_LOG(WARN, "invalid argument", KP(buf), K(size), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(global_trans_version_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected global trans version", KR(ret), "context", *this);
    need_print_trace_log_ = true;
  } else {
    if (0 == commit_log_checksum_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "commit log checksum is invalid", KR(ret), "context", *this);
      need_print_trace_log_ = true;
    } else {
      ObTransCommitLog log(OB_LOG_TRANS_COMMIT,
          self_,
          trans_id_,
          partition_log_info_arr_,
          global_trans_version_,
          commit_log_checksum_,
          cluster_id_);
      if (OB_UNLIKELY(!log.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "invalid commit log", K(ret), K(log));
      } else if (OB_FAIL(log.serialize(buf, size, tmp_pos))) {
        TRANS_LOG(WARN, "serialize commit log error", K(ret), K(log));
      } else {
        pos = tmp_pos;
      }
    }
  }

  return ret;
}

int ObPartTransCtx::fill_record_log_(char *buf, const int64_t size, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  int64_t prev_log_ids_count = (int64_t)prev_redo_log_ids_.count();

  // Create a ObTransRecordLog and pass the log_id of prev checkpoint to it
  ObTransRecordLog log(OB_LOG_TRANS_RECORD, self_, trans_id_, cluster_id_, prev_checkpoint_id_);
  TRANS_LOG(INFO, "succeed to create record log", K(trans_id_), K(prev_checkpoint_id_), K(log.get_prev_record_log_id()));
  #ifdef ERRSIM
  // Error injection test, used for changing prev_log_ids_count for test
  int tmp_ret = E(EventTable::EN_LOG_IDS_COUNT_ERROR) OB_SUCCESS;
  if (tmp_ret != OB_SUCCESS) {
    prev_log_ids_count = 2;
    TRANS_LOG(INFO, "set_prev_log_ids_count: ", K(prev_log_ids_count));
  }
  #endif
  // Add current all log_ids to the record log
  for (int i = 0; i < prev_log_ids_count; ++i) {
    log.add_log_id(prev_redo_log_ids_.at(i));
  }

  if (OB_FAIL(log.serialize(buf, size, tmp_pos))) {
    TRANS_LOG(WARN, "serialize commit log error", K(ret), K(log));
  } else {
    pos = tmp_pos;
  }

  return ret;
}

int ObPartTransCtx::fill_pre_commit_log_(char *buf, const int64_t size, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  ObTransPreCommitLog log;

  if (OB_ISNULL(buf) || OB_UNLIKELY(size < 0)) {
    TRANS_LOG(WARN, "invalid argument", KP(buf), K(size), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(global_trans_version_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected publish version", KR(ret), "context", *this);
    need_print_trace_log_ = true;
  } else if (OB_FAIL(log.init(OB_LOG_TRANS_PRE_COMMIT, self_, trans_id_, cluster_id_, global_trans_version_))) {
    TRANS_LOG(WARN, "init pre commit log error", KR(ret), K(*this));
  } else if (OB_FAIL(log.serialize(buf, size, tmp_pos))) {
    TRANS_LOG(WARN, "serialize pre commit log error", KR(ret), K(log));
  } else {
    pos = tmp_pos;
  }

  return ret;
}

int ObPartTransCtx::fill_abort_log_(char* buf, const int64_t size, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  ObTransAbortLog log;

  if (OB_ISNULL(buf) || OB_UNLIKELY(size < 0)) {
    TRANS_LOG(WARN, "invalid argument", KP(buf), K(size), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(log.init(OB_LOG_TRANS_ABORT, self_, trans_id_, partition_log_info_arr_, cluster_id_))) {
    TRANS_LOG(WARN, "init abort log error", KR(ret), K_(trans_id));
  } else if (OB_FAIL(log.serialize(buf, size, tmp_pos))) {
    TRANS_LOG(WARN, "serialize abort log error", KR(ret), K(log));
  } else {
    pos = tmp_pos;
  }

  return ret;
}

int ObPartTransCtx::fill_clear_log_(char* buf, const int64_t size, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  ObTransClearLog log;

  if (OB_ISNULL(buf) || OB_UNLIKELY(size < 0)) {
    TRANS_LOG(WARN, "invalid argument", KP(buf), K(size), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(log.init(OB_LOG_TRANS_CLEAR, self_, trans_id_, cluster_id_))) {
    TRANS_LOG(WARN, "init clear log error", KR(ret), K_(trans_id));
  } else if (OB_FAIL(log.serialize(buf, size, tmp_pos))) {
    TRANS_LOG(WARN, "serialize clear log error", KR(ret), K(log));
  } else {
    pos = tmp_pos;
  }

  return ret;
}

int ObPartTransCtx::fill_log_header_(
    char* buf, const int64_t size, int64_t& pos, const int64_t log_type, const int64_t idx)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;

  if (OB_ISNULL(buf) || OB_UNLIKELY(size < 0) || OB_UNLIKELY(!ObTransLogType::is_valid(log_type))) {
    TRANS_LOG(WARN, "invalid argument", KP(buf), K(size), K(log_type), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::encode_i64(buf, size, tmp_pos, log_type))) {
    TRANS_LOG(WARN, "serialize log type error", KR(ret), K(log_type));
  } else if (OB_FAIL(serialization::encode_i64(buf, size, tmp_pos, idx))) {
    TRANS_LOG(WARN, "serialize idx error", KR(ret), K(log_type), K(idx));
  } else {
    pos = tmp_pos;
  }

  return ret;
}

int ObPartTransCtx::fill_redo_prepare_log_(
    char* buf, const int64_t size, int64_t& pos, int64_t& redo_log_pos, int64_t& log_type, int64_t& mutator_size)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  int64_t tmp_redo_log_pos = 0;
  int64_t tmp_log_type = 0;
  int64_t tmp_mutator_size = 0;

  if (OB_ISNULL(buf) || OB_UNLIKELY(size < 0)) {
    TRANS_LOG(WARN, "invalid argument", KP(buf), K(size), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS == (ret = fill_redo_log_(buf, size, tmp_pos, tmp_mutator_size))) {
    tmp_redo_log_pos = tmp_pos;
    tmp_log_type = tmp_log_type | OB_LOG_TRANS_REDO;
    if (is_dup_table_trans_) {
      // We need seperate the redo and prepare in order to not take all leased
      // followers' max read version into consideration
      is_dup_table_prepare_ = true;
    } else if (OB_FAIL(fill_prepare_log_(buf, size, tmp_pos))) {
      if (OB_SIZE_OVERFLOW != ret) {
        TRANS_LOG(WARN, "fill prepare log failed", K(ret), "context", *this);
      }
      if (tmp_mutator_size > 0) {
        // rewrite ret code
        ret = OB_SUCCESS;
      }
    } else {
      tmp_log_type = tmp_log_type | OB_LOG_TRANS_PREPARE;
    }
  } else if (OB_EAGAIN == ret) {
    tmp_redo_log_pos = tmp_pos;
    tmp_log_type = tmp_log_type | OB_LOG_TRANS_REDO;
    ret = OB_SUCCESS;
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    tmp_redo_log_pos = tmp_pos;
    ret = OB_SUCCESS;
    if (OB_SUCCESS != (ret = fill_prepare_log_(buf, size, tmp_pos))) {
      TRANS_LOG(WARN, "fill prepare log error", KR(ret), "context", *this);
    } else {
      tmp_log_type = tmp_log_type | OB_LOG_TRANS_PREPARE;
    }
  } else {
    TRANS_LOG(WARN, "fill redo log error", KR(ret), "context", *this);
  }
  if (OB_SUCC(ret)) {
    pos = tmp_pos;
    redo_log_pos = tmp_redo_log_pos;
    log_type = tmp_log_type;
    mutator_size = tmp_mutator_size;
  }

  return ret;
}

int ObPartTransCtx::fill_redo_prepare_commit_log_(
    char* buf, const int64_t size, int64_t& pos, int64_t& redo_log_pos, int64_t& log_type, int64_t& mutator_size)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  int64_t tmp_redo_log_pos = 0;
  int64_t tmp_log_type = 0;
  int64_t tmp_mutator_size = 0;

  if (OB_ISNULL(buf) || OB_UNLIKELY(size < 0)) {
    TRANS_LOG(WARN, "invalid argument", KP(buf), K(size), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(fill_redo_prepare_log_(buf, size, tmp_pos, tmp_redo_log_pos, tmp_log_type, tmp_mutator_size))) {
    TRANS_LOG(WARN, "fill redo_prepare log error", KR(ret), "context", *this);
  } else if ((tmp_log_type & OB_LOG_TRANS_PREPARE) != 0) {
    if (OB_FAIL(fill_commit_log_(buf, size, tmp_pos))) {
      if (OB_SIZE_OVERFLOW != ret) {
        TRANS_LOG(WARN, "fill commit log failed", K(ret), "context", *this);
      }
      ret = OB_SUCCESS;
    } else {
      tmp_log_type = tmp_log_type | OB_LOG_TRANS_COMMIT;
    }
  } else {
    // do nothing
  }
  if (OB_SUCC(ret)) {
    pos = tmp_pos;
    redo_log_pos = tmp_redo_log_pos;
    log_type = tmp_log_type;
    mutator_size = tmp_mutator_size;
  }

  return ret;
}

int ObPartTransCtx::fill_redo_prepare_commit_clear_log_(
    char* buf, const int64_t size, int64_t& pos, int64_t& log_type, int64_t& mutator_size)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  int64_t tmp_redo_log_pos = 0;
  int64_t tmp_log_type = 0;
  int64_t tmp_mutator_size = 0;

  if (OB_ISNULL(buf) || OB_UNLIKELY(size < 0)) {
    TRANS_LOG(WARN, "invalid argument", KP(buf), K(size), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(
                 fill_redo_prepare_commit_log_(buf, size, tmp_pos, tmp_redo_log_pos, tmp_log_type, tmp_mutator_size))) {
    TRANS_LOG(WARN, "fill redo_prepare_commit log error", KR(ret), "context", *this);
  } else if ((tmp_log_type & OB_LOG_TRANS_COMMIT) != 0) {
    if (OB_FAIL(fill_clear_log_(buf, size, tmp_pos))) {
      if (OB_SIZE_OVERFLOW != ret) {
        TRANS_LOG(WARN, "fill clear log failed", K(ret), "context", *this);
      }
      // rewrite ret code
      ret = OB_SUCCESS;
    } else {
      tmp_log_type = tmp_log_type | OB_LOG_TRANS_CLEAR;
    }
  } else {
    // do nothing
  }
  if (OB_SUCC(ret)) {
    if ((tmp_log_type & OB_LOG_TRANS_CLEAR) != 0) {
      pos = tmp_pos;
      log_type = tmp_log_type;
    } else {
      pos = tmp_redo_log_pos;
      log_type = OB_LOG_TRANS_REDO;
    }
    mutator_size = tmp_mutator_size;
  }

  return ret;
}

int ObPartTransCtx::fill_sp_redo_log_(ObSpTransRedoLog& sp_redo_log, const int64_t capacity, int64_t& mutator_size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!sp_redo_log.is_valid()) || OB_UNLIKELY(capacity < 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t available_capacity = capacity;
    ObTransMutator& mutator = sp_redo_log.get_mutator();
    if (available_capacity > mutator.get_capacity()) {
      available_capacity = mutator.get_capacity();
    }
    if (OB_FAIL(mt_ctx_.fill_redo_log(mutator.get_data(), available_capacity, mutator.get_position()))) {
      if (EXECUTE_COUNT_PER_SEC(16)) {
        TRANS_LOG(DEBUG, "sp trans fill redo log error", KR(ret), K(sp_redo_log), "context", *this);
      }
    }
    if (OB_SUCCESS == ret || OB_EAGAIN == ret) {
      mutator_size = mutator.get_position();
    }
  }

  return ret;
}

int ObPartTransCtx::fill_sp_abort_log_(char* buf, const int64_t size, int64_t& pos, int64_t& log_type)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  ObSpTransAbortLog abort_log;

  if (OB_FAIL(abort_log.init(OB_LOG_SP_TRANS_ABORT, self_, trans_id_, cluster_id_))) {
    TRANS_LOG(WARN, "init abort log error", KR(ret), "partition", self_, K_(trans_id), K_(cluster_id));
  } else if (OB_FAIL(abort_log.serialize(buf, size, tmp_pos))) {
    TRANS_LOG(WARN, "serialize abort log error", KR(ret), K(abort_log));
  } else {
    pos = tmp_pos;
    log_type = OB_LOG_SP_TRANS_ABORT;
  }

  return ret;
}

int ObPartTransCtx::pre_check_sp_trans_log_(
    const int log_type, ObSpTransCommitLog& commit_log, bool& need_wait_prev_trans)
{
  int ret = OB_SUCCESS;
  need_wait_prev_trans = true;
  int state = ObTransResultState::INVALID;

  if (OB_LOG_SP_TRANS_COMMIT == log_type) {
    need_wait_prev_trans = false;
  } else if (OB_FAIL(ctx_dependency_wrap_.get_prev_trans_arr_result(state))) {
    TRANS_LOG(WARN, "get prev trans arr result error", K(ret), "context", *this);
  } else if (OB_LOG_SP_TRANS_REDO == log_type) {
    // We should not depend on big trans predecessors
    if (ObTransResultState::is_commit(state)) {
      if (OB_FAIL(commit_log.set_log_type(OB_LOG_SP_TRANS_REDO))) {
        TRANS_LOG(WARN, "change targe log type error", KR(ret), K(log_type), "context", *this);
      } else if (OB_FAIL(commit_log.set_prev_trans_arr(ObElrTransInfoArray()))) {
        TRANS_LOG(WARN, "sp commit log set prev trans array error", KR(ret), "context", *this);
      } else {
        need_wait_prev_trans = false;
      }
    } else if (ObTransResultState::is_abort(state)) {
      ret = OB_TRANS_NEED_ROLLBACK;
      need_print_trace_log_ = true;
      TRANS_LOG(WARN, "current transaction need to rollback because of prev abort trans", "context", *this);
    } else {
      // The predecessor has not decide, so we need wait for the predecessors decided
      if (OB_FAIL(clog_adapter_->submit_log_id_alloc_task(OB_LOG_SP_ELR_TRANS_COMMIT, this))) {
        TRANS_LOG(WARN, "submit log id alloc task error", KR(ret), K(log_type), "context", *this);
      } else {
        inc_submit_log_pending_count_();
        inc_submit_log_count_();
      }
    }
  } else if (OB_LOG_SP_ELR_TRANS_COMMIT != log_type) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected log type", KR(ret), K(log_type), K(commit_log), "context", *this);
  } else if (OB_FAIL(ctx_dependency_wrap_.check_can_depend_prev_elr_trans(local_trans_version_))) {
    TRANS_LOG(WARN, "check can depend prev elr trans error", KR(ret), "context", *this);
    if (OB_EAGAIN == ret) {
      if (is_prepare_leader_revoke_) {
        ret = OB_TRANS_NEED_ROLLBACK;
        need_print_trace_log_ = true;
        TRANS_LOG(WARN, "current transaction need to rollback because of leader revoke", "context", *this);
      } else if (OB_FAIL(clog_adapter_->submit_log_id_alloc_task(log_type, this))) {
        TRANS_LOG(WARN, "submit log id alloc task error", K(ret), K(log_type), "context", *this);
      } else {
        inc_submit_log_pending_count_();
        inc_submit_log_count_();
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "check can depend prev elr trans unexpected", K(ret), "context", *this);
    }
  } else if (ObTransResultState::is_commit(state)) {
    if (OB_FAIL(commit_log.set_log_type(OB_LOG_SP_TRANS_COMMIT))) {
      TRANS_LOG(WARN, "change targe log type error", KR(ret), K(log_type), "context", *this);
    } else if (OB_FAIL(commit_log.set_prev_trans_arr(ObElrTransInfoArray()))) {
      TRANS_LOG(WARN, "sp commit log set prev trans array error", KR(ret), "context", *this);
    } else {
      need_wait_prev_trans = false;
    }
  } else if (ObTransResultState::is_abort(state)) {
    ret = OB_TRANS_NEED_ROLLBACK;
    need_print_trace_log_ = true;
    TRANS_LOG(WARN, "current transaction need to rollback because of prev abort trans", "context", *this);
  } else {
    // The predecessor txn has not decided, so continue to write SP_ELR_TRANS_COMMIT
    need_wait_prev_trans = false;
  }

  return ret;
}

// Targe_log_type means the log type upper layer wants to submit.
// log_type means the real log type need be submitted
int ObPartTransCtx::fill_sp_commit_log_(const int target_log_type, char* buf, const int64_t size, int64_t& pos,
    int64_t& log_type, bool& has_redo_log, bool& need_submit_log, int64_t& mutator_size)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  int64_t tmp_log_type = 0;
  bool tmp_has_redo_log = true;
  bool tmp_need_submit_log = true;
  int64_t tmp_mutator_size = 0;
  int64_t available_capacity = 0;
  ObSpTransCommitLog commit_log;
  bool need_retry_alloc_id_ts = false;
  int64_t checkpoint = 0;
  uint64_t checksum = INT64_MAX;

  if (OB_FAIL(get_checkpoint_(checkpoint))) {
    TRANS_LOG(WARN, "get checkpoint failed", KR(ret), "context", *this);
  } else {
    ObElrTransArrGuard guard;
    if (OB_FAIL(ctx_dependency_wrap_.get_prev_trans_arr_guard(guard))) {
      TRANS_LOG(WARN, "get prev trans arr guard error", KR(ret), K(*this));
    } else if (OB_FAIL(commit_log.init(target_log_type,
                   self_,
                   tenant_id_,
                   trans_id_,
                   checksum,
                   cluster_id_,
                   prev_redo_log_ids_,
                   trans_param_,
                   redo_log_no_ + mutator_log_no_,
                   trace_info_.get_app_trace_id(),
                   checkpoint,
                   guard.get_elr_trans_arr(),
                   can_elr_,
                   trace_info_.get_app_trace_info(),
                   prev_checkpoint_id_))) {
      TRANS_LOG(WARN,
          "init commit log error",
          KR(ret),
          "partition",
          self_,
          K_(trans_id),
          K_(global_trans_version),
          K_(cluster_id),
          K_(prev_redo_log_ids),
          K(checkpoint));
    } else {
      // do nothing
    }
  }

  if (OB_SUCC(ret)) {
    available_capacity = size - OB_TRANS_REDO_LOG_RESERVE_SIZE - commit_log.get_serialize_size();
    if (available_capacity <= 0) {
      TRANS_LOG(WARN, "size overflow", K(size), K(available_capacity));
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = fill_sp_redo_log_(commit_log, available_capacity, tmp_mutator_size);
      // We need seperate the redo and prepare in order to not take all leased
      // followers' max read version into consideration
      if (OB_SUCCESS == ret && !is_dup_table_trans_) {
        if (OB_FAIL(prepare_sp_redolog_id_ts_(target_log_type, need_retry_alloc_id_ts))) {
          TRANS_LOG(WARN, "prepare sp redo log id and timestamp error", KR(ret), "context", *this);
        } else if (!need_retry_alloc_id_ts) {
          bool unused = false;
          bool need_wait_prev_trans = false;
          checksum = mt_ctx_.calc_checksum4();

          REC_TRANS_TRACE_EXT(tlog_,
              calc_checksum_by_commit,
              OB_ID(ret),
              ret,
              OB_ID(checksum),
              checksum,
              OB_ID(checksum_log_ts),
              mt_ctx_.get_checksum_log_ts());

          // Check the txn's predecessors, and decide the action based on the predecessors' state
          if (OB_FAIL(pre_check_sp_trans_log_(target_log_type, commit_log, need_wait_prev_trans))) {
            TRANS_LOG(
                WARN, "pre check sp trans log error", KR(ret), K(target_log_type), K(commit_log), "context", *this);
          } else if (need_wait_prev_trans) {
            TRANS_LOG(INFO, "need wait slow prev transaction end", "context", *this);
            tmp_need_submit_log = false;
            mt_ctx_.undo_fill_redo_log();
          } else if (OB_FAIL(commit_log.set_checksum(checksum))) {
            TRANS_LOG(WARN, "set sp commit log checksum error", KR(ret), K(checksum), K(commit_log));
          } else if (OB_FAIL(commit_log.serialize(buf, size, tmp_pos))) {
            TRANS_LOG(WARN, "serialize sp commit log error", KR(ret), K(commit_log));
          } else {
            tmp_log_type = target_log_type;
            tmp_has_redo_log = true;
          }
          if (OB_FAIL(ret)) {
            mt_ctx_.undo_fill_redo_log();
          }
        } else {
          // We need to retry apply for the log_id and log_ts, so we cansubmit
          // log when the log_id and log_ts is applied successfully
          tmp_need_submit_log = false;
        }
      } else if (OB_EAGAIN == ret) {
        ObSpTransRedoLog& sp_redo_log = commit_log;
        bool need_wait_prev_trans = false;
        if (OB_FAIL(pre_check_sp_trans_log_(OB_LOG_SP_TRANS_REDO, commit_log, need_wait_prev_trans))) {
          TRANS_LOG(WARN, "pre check sp trans log error", KR(ret), K(target_log_type), K(commit_log), "context", *this);
          mt_ctx_.undo_fill_redo_log();
        } else if (need_wait_prev_trans) {
          TRANS_LOG(INFO, "current big transaction need wait prev trans end", "context", *this);
          tmp_need_submit_log = false;
          mt_ctx_.undo_fill_redo_log();
        } else {
          if (OB_FAIL(prepare_sp_redolog_id_ts_(target_log_type, need_retry_alloc_id_ts))) {
            TRANS_LOG(WARN, "prepare sp redo log id and timestamp error", KR(ret), "context", *this);
          } else if (!need_retry_alloc_id_ts) {
            if (OB_FAIL(sp_redo_log.set_log_type(OB_LOG_SP_TRANS_REDO))) {
              TRANS_LOG(WARN, "set sp trans redo log error", KR(ret), "context", *this);
            } else if (OB_FAIL(sp_redo_log.serialize(buf, size, tmp_pos))) {
              TRANS_LOG(WARN, "serialize sp redo log error", KR(ret), K(sp_redo_log));
            } else {
              tmp_log_type = OB_LOG_SP_TRANS_REDO;
              tmp_has_redo_log = true;
            }
            if (OB_FAIL(ret)) {
              mt_ctx_.undo_fill_redo_log();
            }
          } else {
            // We need to retry apply for the log_id and log_ts, so we cansubmit
            // log when the log_id and log_ts is applied successfully
            tmp_need_submit_log = false;
          }
        }
      } else if (OB_ENTRY_NOT_EXIST == ret) {
        // The first log of sp_trans, we need not write remaining logs
        // (eg: commit) if no redo exits
        if (0 == redo_log_no_ + mutator_log_no_ && 0 == submit_log_count_) {
          ret = OB_SUCCESS;
          tmp_has_redo_log = false;
          tmp_need_submit_log = false;
        } else if (OB_LOG_SP_ELR_TRANS_COMMIT == target_log_type) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "unexpected target log type", KR(ret), K(target_log_type), "context", *this);
        } else if (OB_FAIL(prepare_sp_redolog_id_ts_(target_log_type, need_retry_alloc_id_ts))) {
          TRANS_LOG(WARN, "prepare sp redo log id and timestamp error", KR(ret), "context", *this);
        } else if (!need_retry_alloc_id_ts) {
          checksum = mt_ctx_.calc_checksum4();

          REC_TRANS_TRACE_EXT(tlog_,
              calc_checksum_by_commit,
              OB_ID(ret),
              ret,
              OB_ID(checksum),
              checksum,
              OB_ID(checksum_log_ts),
              mt_ctx_.get_checksum_log_ts());

          if (OB_FAIL(commit_log.set_checksum(checksum))) {
            TRANS_LOG(WARN, "set sp commit log checksum error", KR(ret), K(checksum), K(commit_log));
          } else if (OB_FAIL(commit_log.serialize(buf, size, tmp_pos))) {
            TRANS_LOG(WARN, "serialize commit log error", KR(ret), K(commit_log));
          } else {
            tmp_log_type = target_log_type;
          }
          if (OB_FAIL(ret)) {
            mt_ctx_.undo_fill_redo_log();
          }
        } else {
          // We need to retry apply for the log_id and log_ts, so we cansubmit
          // log when the log_id and log_ts is applied successfully
          tmp_need_submit_log = false;
        }
      } else if (OB_SUCCESS == ret && is_dup_table_trans_) {
        ObSpTransRedoLog& sp_redo_log = commit_log;
        bool need_wait_prev_trans = false;
        if (OB_FAIL(pre_check_sp_trans_log_(OB_LOG_SP_TRANS_REDO, commit_log, need_wait_prev_trans))) {
          TRANS_LOG(WARN, "pre check sp trans log error", KR(ret), K(target_log_type), K(commit_log), "context", *this);
          mt_ctx_.undo_fill_redo_log();
        } else if (need_wait_prev_trans) {
          TRANS_LOG(INFO, "current big transaction need wait prev trans end", "context", *this);
          tmp_need_submit_log = false;
          mt_ctx_.undo_fill_redo_log();
        } else {
          if (OB_FAIL(prepare_sp_redolog_id_ts_(target_log_type, need_retry_alloc_id_ts))) {
            TRANS_LOG(WARN, "prepare sp redo log id and timestamp error", KR(ret), "context", *this);
          } else if (!need_retry_alloc_id_ts) {
            if (OB_FAIL(sp_redo_log.set_log_type(OB_LOG_SP_TRANS_REDO))) {
              TRANS_LOG(WARN, "set sp trans redo log error", KR(ret), "context", *this);
            } else if (OB_FAIL(sp_redo_log.serialize(buf, size, tmp_pos))) {
              TRANS_LOG(WARN, "serialize sp redo log error", KR(ret), K(sp_redo_log));
            } else {
              tmp_log_type = OB_LOG_SP_TRANS_REDO;
              tmp_has_redo_log = true;
              // Mark the redo log fill
              is_dup_table_prepare_ = true;
            }
            if (OB_FAIL(ret)) {
              mt_ctx_.undo_fill_redo_log();
            }
          } else {
            // We need to retry apply for the log_id and log_ts, so we cansubmit
            // log when the log_id and log_ts is applied successfully
            tmp_need_submit_log = false;
          }
        }
      } else {
        TRANS_LOG(WARN, "fill redo log error", KR(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    log_type = tmp_log_type;
    has_redo_log = tmp_has_redo_log;
    need_submit_log = tmp_need_submit_log;
    pos = tmp_pos;
    mutator_size = tmp_mutator_size;
  }

  return ret;
}

int ObPartTransCtx::fill_trans_state_log_(char* buf, const int64_t size, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  ObTransStateLog log;
  ObElrTransArrGuard guard;
  ObTimeGuard timeguard("fill_trans_state_log", 10 * 1000);

  const int64_t available_capacity = size - OB_TRANS_REDO_LOG_RESERVE_SIZE;
  if (OB_UNLIKELY(!stmt_info_.is_task_match())) {
    ret = OB_ERR_UNEXPECTED;
    need_print_trace_log_ = true;
    TRANS_LOG(ERROR, "fill trans state log when task not match", KR(ret), K(*this));
  } else if (OB_FAIL(ctx_dependency_wrap_.get_prev_trans_arr_guard(guard))) {
    TRANS_LOG(WARN, "get prev trans arr guard error", KR(ret), K(*this));
  } else if (OB_FAIL(log.init(OB_LOG_TRANS_STATE,
                 self_,
                 trans_id_,
                 scheduler_,
                 cluster_id_,
                 tenant_id_,
                 trans_expired_time_,
                 trans_param_,
                 is_readonly_,
                 trans_type_,
                 session_id_,
                 proxy_session_id_,
                 commit_task_count_,
                 stmt_info_,
                 trace_info_.get_app_trace_id(),
                 mt_ctx_.get_min_table_version(),
                 guard.get_elr_trans_arr(),
                 can_elr_,
                 proposal_leader_,
                 cluster_version_,
                 snapshot_version_,
                 cur_query_start_time_,
                 stmt_expired_time_,
                 xid_))) {
    TRANS_LOG(WARN, "init trans state log failed", KR(ret));
  } else if (OB_FAIL(log.serialize(buf, available_capacity, tmp_pos))) {
    TRANS_LOG(WARN, "serialize log failed", KR(ret), K(log));
  } else {
    pos = tmp_pos;
  }

  return ret;
}

int ObPartTransCtx::fill_mutator_log_(char* buf, const int64_t size, int64_t& pos, int64_t& mutator_size)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  int64_t available_capacity = 0;
  ObTransMutatorLog log;
  ObTimeGuard timeguard("fill_mutator_log", 10 * 1000);

  {
    ObElrTransArrGuard guard;
    if (OB_FAIL(ctx_dependency_wrap_.get_prev_trans_arr_guard(guard))) {
      TRANS_LOG(WARN, "get prev trans arr guard error", KR(ret), K(*this));
    } else if (OB_FAIL(log.init(OB_LOG_MUTATOR,
                   self_,
                   trans_id_,
                   cluster_id_,
                   tenant_id_,
                   trans_expired_time_,
                   trans_param_,
                   redo_log_no_ + mutator_log_no_,
                   guard.get_elr_trans_arr(),
                   can_elr_,
                   cluster_version_))) {
      TRANS_LOG(WARN, "init mutator log failed", KR(ret));
    } else {
      // do nothing
    }
  }

  if (OB_SUCC(ret)) {
    // Besides the reserved size and redo log header, the rest is used to fill
    // the mutator
    available_capacity = size - OB_TRANS_REDO_LOG_RESERVE_SIZE - log.get_serialize_size();
    ObTransMutator& mutator = log.get_mutator();
    if (available_capacity > mutator.get_capacity()) {
      available_capacity = mutator.get_capacity();
    }
    if (available_capacity <= 0) {
      TRANS_LOG(WARN, "size overflow", K(size), K(available_capacity));
      ret = OB_ERR_UNEXPECTED;
    } else {
      timeguard.click();
      ret = mt_ctx_.fill_redo_log(mutator.get_data(), available_capacity, mutator.get_position());
      timeguard.click();
      if (OB_SUCCESS == ret || OB_EAGAIN == ret) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = log.serialize(buf, size, tmp_pos))) {
          TRANS_LOG(WARN, "serialize redo log error", K(tmp_ret), K(log));
          mt_ctx_.undo_fill_redo_log();
          ret = tmp_ret;
        } else {
          pos = tmp_pos;
          mutator_size = mutator.get_position();
        }
      } else if (OB_ENTRY_NOT_EXIST == ret) {
        // do nothing
      } else {
        TRANS_LOG(WARN, "fill redo log error", KR(ret));
      }
      timeguard.click();
    }
  }
  if (timeguard.get_diff() > 5 * 1000) {
    TRANS_LOG(WARN, "fill mutator log cost too much time", K_(trans_id), K_(self), K(timeguard));
  }

  return ret;
}

int ObPartTransCtx::fill_mutator_state_log_(char* buf, const int64_t size, int64_t& pos, int64_t& log_type)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  int64_t tmp_log_type = 0;
  int64_t tmp_mutator_size = 0;

  if (OB_ISNULL(buf) || OB_UNLIKELY(size < 0)) {
    TRANS_LOG(WARN, "invalid argument", KP(buf), K(size), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCC(fill_mutator_log_(buf, size, tmp_pos, tmp_mutator_size))) {
    tmp_log_type = tmp_log_type | OB_LOG_MUTATOR;
    if (OB_FAIL(fill_trans_state_log_(buf, size, tmp_pos))) {
      if (OB_SIZE_OVERFLOW != ret) {
        TRANS_LOG(WARN, "fill trans state log failed", K(ret), "context", *this);
      }
      if (tmp_mutator_size > 0) {
        // rewrite ret
        ret = OB_SUCCESS;
      }
    } else {
      tmp_log_type = tmp_log_type | OB_LOG_TRANS_STATE;
    }
  } else if (OB_EAGAIN == ret) {
    tmp_log_type = tmp_log_type | OB_LOG_MUTATOR;
    // rewrite ret
    ret = OB_SUCCESS;
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    // rewrite ret
    ret = OB_SUCCESS;
    if (OB_FAIL(fill_trans_state_log_(buf, size, tmp_pos))) {
      TRANS_LOG(WARN, "fill trans state log failed", KR(ret), "context", *this);
    } else {
      tmp_log_type = tmp_log_type | OB_LOG_TRANS_STATE;
    }
  } else {
    TRANS_LOG(WARN, "fill mutator log error", KR(ret), "context", *this);
  }
  if (OB_SUCC(ret)) {
    pos = tmp_pos;
    log_type = tmp_log_type;
  }

  return ret;
}

int ObPartTransCtx::fill_mutator_abort_log_(char* buf, const int64_t size, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  ObTransMutatorAbortLog log;
  ObTimeGuard timeguard("fill_mutator_abort_log", 10 * 1000);

  const int64_t available_capacity = size - OB_TRANS_REDO_LOG_RESERVE_SIZE;
  if (OB_FAIL(log.init(OB_LOG_MUTATOR_ABORT, self_, trans_id_, cluster_id_))) {
    TRANS_LOG(WARN, "init mutator abort log failed", KR(ret));
  } else if (OB_FAIL(log.serialize(buf, available_capacity, tmp_pos))) {
    TRANS_LOG(WARN, "serialize log failed", KR(ret), K(log));
  } else {
    pos = tmp_pos;
  }

  return ret;
}

bool ObPartTransCtx::is_idential_tenant_()
{
  bool bool_ret = true;
  const ObPartitionKey& pkey = participants_.at(0);

  for (int64_t i = 1; i < participants_.count(); ++i) {
    if (participants_.at(i).get_tenant_id() != pkey.get_tenant_id()) {
      bool_ret = false;
      break;
    }
  }

  return bool_ret;
}

int ObPartTransCtx::generate_local_trans_version_(const int64_t log_type)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t gts = 0;
  MonotonicTs receive_gts_ts;
  ObITsMgr* ts_mgr = get_ts_mgr_();

  if (!is_gts_waiting_) {
    // The first participant of distributed txn or single-partitioned txn need
    // alloc gts
    bool need_get_gts = false;
    if (OB_ISNULL(ts_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "ts mgr is null", KR(ret));
    } else if (is_dup_table_trans_) {
      if (!ts_mgr->is_external_consistent(self_.get_tenant_id())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "dup table need to open gts", KR(ret), K(*this));
      } else {
        need_get_gts = true;
        if (is_dup_table_prepare_) {
          stc_ = MonotonicTs::current_time();
        }
      }
    } else if (!ts_mgr->is_external_consistent(self_.get_tenant_id())) {
      // If GTS is not supported, we use the local timestamp
      need_get_gts = false;
    } else if (is_sp_trans_()) {
      need_get_gts = true;
    } else if (participants_.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected participants", KR(ret), "context", *this);
      need_print_trace_log_ = true;
      // All participants need fetch gts for cross-tenant txns
    } else if (!is_idential_tenant_()) {
      need_get_gts = true;
      need_print_trace_log_ = true;
      if (EXECUTE_COUNT_PER_SEC(16)) {
        TRANS_LOG(INFO, "multi tenant transaction", "context", *this);
      }
    } else if (self_ == participants_.at(0)) {
      need_get_gts = true;
    } else {
      // do nothing
    }
    if (OB_SUCC(ret)) {
      gts_request_ts_ = ObTimeUtility::current_time();
      const int64_t GET_GTS_AHEAD_INTERVAL = GCONF._ob_get_gts_ahead_interval;
      const MonotonicTs stc_ahead = stc_ - MonotonicTs(GET_GTS_AHEAD_INTERVAL);
      if (need_get_gts) {
        // Obtain the suitable gts according to stc
        if (OB_FAIL(ts_mgr->get_local_trans_version(self_.get_tenant_id(), stc_ahead, this, gts, receive_gts_ts))) {
          if (OB_EAGAIN != ret) {
            TRANS_LOG(WARN, "participant get gts failed", KR(ret), K_(tenant_id), "context", *this);
          }
        } else if (gts < snapshot_version_) {
          need_print_trace_log_ = true;
          int64_t cache_gts = 0;
          (void)ts_mgr->get_gts(self_.get_tenant_id(), NULL, cache_gts);
          TRANS_LOG(ERROR, "unexpected gts", K(ret), K(gts), K_(snapshot_version), K(cache_gts), K(*this));
        } else {
          set_trans_need_wait_wrap_(receive_gts_ts, GET_GTS_AHEAD_INTERVAL);
        }
      } else {
        // Obtain the the newest value of gts cache.
        // Under the scenerio of ts source switch, we need register asynchronous callback
        if (OB_FAIL(ts_mgr->get_local_trans_version(self_.get_tenant_id(), this, gts))) {
          TRANS_LOG(WARN, "participant get gts cache failed", KR(ret), K_(tenant_id), "context", *this);
        } else if (gts < snapshot_version_) {
          need_print_trace_log_ = true;
          int64_t cache_gts = 0;
          (void)ts_mgr->get_gts(self_.get_tenant_id(), NULL, cache_gts);
          TRANS_LOG(ERROR, "unexpected gts", K(ret), K(gts), K_(snapshot_version), K(cache_gts), K(*this));
        } else {
          // do nothing
        }
      }
      if (OB_SUCC(ret)) {
        if (gts <= 0) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "invalid gts, unexpected error", KR(ret), K(log_type), K(gts), "context", *this);
          need_print_trace_log_ = true;
        } else if (gts > local_trans_version_) {
          local_trans_version_ = gts;
        } else {
          // do nothing
        }
      } else if (OB_EAGAIN != ret) {
        TRANS_LOG(WARN, "participant get gts error", KR(ret), K_(stc), "context", *this);
      } else if (OB_SUCCESS != (tmp_ret = partition_mgr_->acquire_ctx_ref(trans_id_))) {
        TRANS_LOG(ERROR, "get trans ctx error", "ret", tmp_ret, "context", *this);
      } else {
        // Record the log type when gts cannot obtained successfully, and the
        // log type is used when asynchronous callback for continuing logging
        is_gts_waiting_ = true;
        log_type_ = log_type;
        REC_TRANS_TRACE_EXT(tlog_, wait_get_gts, Y(log_type));
      }
    }
  }

  return ret;
}

int ObPartTransCtx::generate_log_id_timestamp_(const int64_t log_type)
{
  UNUSED(log_type);
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_UNLIKELY(!batch_commit_trans_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "only allow to generate log_id and timestamp in batch_commit_trans", KR(ret));
  } else if (OB_FAIL(clog_adapter_->get_log_id_timestamp(self_, local_trans_version_, pg_, preassigned_log_meta_))) {
    if (OB_EAGAIN == ret) {
      // The 1pc is complicated, we simply let coordinator retry
      ret = OB_ALLOC_LOG_ID_NEED_RETRY;
    } else if (OB_TOO_LARGE_LOG_ID == ret) {
      ret = OB_ALLOC_LOG_ID_NEED_RETRY;
    } else {
      TRANS_LOG(WARN, "alloc log id and timestamp error", KR(ret), "context", *this);
    }
  } else if (OB_UNLIKELY(local_trans_version_ <= 0) ||
             OB_UNLIKELY(OB_INVALID_TIMESTAMP == preassigned_log_meta_.get_submit_timestamp()) ||
             OB_UNLIKELY(local_trans_version_ > preassigned_log_meta_.get_submit_timestamp()) ||
             OB_UNLIKELY(global_trans_version_ > preassigned_log_meta_.get_submit_timestamp()) ||
             OB_UNLIKELY(snapshot_version_ > preassigned_log_meta_.get_submit_timestamp())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR,
        "unexpected local_trans_version or cur_log_timestamp",
        KR(ret),
        K_(local_trans_version),
        K_(preassigned_log_meta),
        K_(global_trans_version),
        K_(snapshot_version),
        "context",
        *this);
    need_print_trace_log_ = true;
    // We need fill nop if unexpected error happens, otherwise the clog will be
    // blocked by the prealloced log instance
    if (OB_SUCCESS != (tmp_ret = clog_adapter_->backfill_nop_log(self_, pg_, preassigned_log_meta_))) {
      TRANS_LOG(WARN, "backfill nop log error", K(tmp_ret), "context", *this);
      if (OB_EAGAIN == tmp_ret &&
          OB_SUCCESS != (tmp_ret = clog_adapter_->submit_backfill_nop_log_task(self_, preassigned_log_meta_))) {
        TRANS_LOG(WARN, "submit backfill nop log task fail", K(tmp_ret), "context", *this);
      }
    }
  } else {
    // Need to push up local_trans_version_ and global_trans_version_ here
    int64_t prepare_version = preassigned_log_meta_.get_submit_timestamp();
    state_.set_prepare_version(prepare_version);
    global_trans_version_ = preassigned_log_meta_.get_submit_timestamp();
    // The reason why the publish version is pushed up here is to reduce the
    // probability of tsc and to ensure that single-machine multi-partitioned
    // txns can response the client in the 2pc prepare phase.
    // Issues involved:
    //   - Cannot read the submitted data: 11493766
    //   - Hotspot Row update performance rollback: 11536015.
    // With GTS, the local publish versioncannot be pushed up here
    //
    // if (OB_FAIL(trans_version_mgr_->update_publish_version(local_trans_version_))) {
    //  TRANS_LOG(DEBUG, "set publish version error", "publish_version", local_trans_version_);
    if (!in_xa_prepare_state_) {
      mt_ctx_.set_prepare_version(prepare_version);
      is_prepared_ = true;
    }
  }

  return ret;
}

int ObPartTransCtx::alloc_local_trans_version_(const int64_t log_type)
{
  int ret = OB_SUCCESS;
  int64_t start = ObTimeUtility::fast_current_time();

  if (is_xa_local_trans() && in_xa_prepare_state_) {
    // Take the smallest valid value here to avoid reporting errors caused by
    // invalid value
    local_trans_version_ = 1;
  } else {
    if (!is_prepared_) {
      mt_ctx_.before_prepare();
    }
    if (OB_FAIL(generate_local_trans_version_(log_type))) {
      if (OB_EAGAIN != ret) {
        TRANS_LOG(WARN, "generate local transaction version error", KR(ret), "context", *this);
      }
    } else {
      // do nothing
    }
  }
  REC_TRANS_TRACE_EXT(tlog_,
      alloc_logid_ts,
      Y(ret),
      OB_ID(log_id),
      preassigned_log_meta_.get_log_id(),
      OB_ID(tenant_id),
      self_.get_tenant_id(),
      OB_ID(uref),
      get_uref(),
      OB_ID(used),
      ObTimeUtility::fast_current_time() - start);

  return ret;
}

int ObPartTransCtx::get_gts_callback(const MonotonicTs srr, const int64_t gts, const MonotonicTs receive_gts_ts)
{
  int ret = OB_SUCCESS;
  bool need_revert_ctx = false;
  {
    CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      TRANS_LOG(WARN, "ObPartTransCtx not inited", K(ret));
    } else if (OB_UNLIKELY(!srr.is_valid()) || OB_UNLIKELY(gts <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", KR(ret), K(srr), K(gts), K_(log_type), "context", *this);
    } else if (OB_UNLIKELY(is_exiting_)) {
      ret = OB_TRANS_IS_EXITING;
      TRANS_LOG(WARN, "transaction is exiting", KR(ret), "context", *this);
    } else if (OB_UNLIKELY(for_replay_)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "invalid state, transaction is replaying", KR(ret), "context", *this);
      need_print_trace_log_ = true;
    } else if (OB_UNLIKELY(!is_gts_waiting_)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "transaction is not waiting gts", KR(ret), "context", *this);
      need_print_trace_log_ = true;
    } else {
      if (EXECUTE_COUNT_PER_SEC(16)) {
        TRANS_LOG(INFO, "get gts callback", K_(stc), K(srr), K(gts), "context", *this);
      }
      if (srr < stc_) {
        ret = OB_EAGAIN;
      } else {
        set_trans_need_wait_wrap_(receive_gts_ts, GET_GTS_AHEAD_INTERVAL);
        if (gts > local_trans_version_) {
          local_trans_version_ = gts;
        } else {
          TRANS_LOG(INFO, "gts is smaller than local_trans_version", K(srr), K(gts), "context", *this);
        }
        if (is_dup_table_trans_ && is_dup_table_prepare_) {
          // When writing the prepare log or sp commit log, we only need to take
          // the latest value once
          is_dup_table_prepare_ = false;
        }
        if (OB_FAIL(retry_submit_log_(log_type_))) {
          TRANS_LOG(WARN, "gts retry submit log error", KR(ret), K_(log_type), "context", *this);
        }
        is_gts_waiting_ = false;
        need_revert_ctx = true;
      }
    }
    REC_TRANS_TRACE_EXT(tlog_, get_gts_callback, Y(ret), OB_ID(srr), srr.mts_, Y(gts));
  }
  if (need_revert_ctx) {
    ret = revert_self_();
  }
  return ret;
}

int ObPartTransCtx::gts_elapse_callback(const MonotonicTs srr, const int64_t gts)
{
  int ret = OB_SUCCESS;
  bool need_revert_ctx = false;
  {
    CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      TRANS_LOG(WARN, "ObPartTransCtx not inited", K(ret));
    } else if (OB_UNLIKELY(!srr.is_valid()) || OB_UNLIKELY(gts <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", KR(ret), K(srr), K(gts), K_(log_type), "context", *this);
    } else if (OB_UNLIKELY(is_exiting_)) {
      ret = OB_TRANS_IS_EXITING;
      TRANS_LOG(WARN, "transaction is exiting", KR(ret), "context", *this);
    } else if (OB_UNLIKELY(for_replay_)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "invalid state, transaction is replaying", KR(ret), "context", *this);
      need_print_trace_log_ = true;
    } else if (OB_UNLIKELY(!is_gts_waiting_)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "transaction is not waiting gts", KR(ret), "context", *this);
      need_print_trace_log_ = true;
    } else {
      if (EXECUTE_COUNT_PER_SEC(16)) {
        TRANS_LOG(INFO, "gts elapse callback", K(srr), K(gts), "context", *this);
      }
      if (global_trans_version_ > gts) {
        ret = OB_EAGAIN;
      } else {
        if (OB_LOG_SP_TRANS_COMMIT == log_type_) {
          if (OB_FAIL(trans_sp_end_(true))) {
            TRANS_LOG(WARN, "transaction sp end error", KR(ret), "context", *this);
          }
        } else if (OB_LOG_SP_ELR_TRANS_COMMIT == log_type_) {
          TRANS_LOG(INFO, "current transaction can commit", K_(log_type), "context", *this);
          if (ObSpState::PREPARE != get_state_()) {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(ERROR, "unexpected state", KR(ret), "context", *this);
          } else if (OB_FAIL(trans_sp_end_(true))) {
            TRANS_LOG(WARN, "transaction sp end error", KR(ret), "context", *this);
          } else {
            // do nothing
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "unexpected log type", KR(ret), K_(log_type), "context", *this);
        }
        need_revert_ctx = true;
        is_gts_waiting_ = false;
        async_applying_log_ts_ = INT64_MAX;
      }
    }
    REC_TRANS_TRACE_EXT(tlog_, gts_elapse_callback, Y(ret), OB_ID(srr), srr.mts_, Y(gts));
  }
  if (need_revert_ctx) {
    ret = revert_self_();
  }
  return ret;
}

int ObPartTransCtx::generate_redo_prepare_log_info(char* buf, const int64_t size, int64_t& pos,
    const int64_t request_id, const PartitionLogInfoArray& partition_log_info_arr, const int64_t commit_version,
    const bool have_prev_trans, clog::ObLogInfo& log_info, clog::ObISubmitLogCb*& cb)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (OB_UNLIKELY(for_replay_)) {
    TRANS_LOG(WARN, "partition is not master", "context", *this);
    ret = OB_NOT_MASTER;
  } else if (OB_UNLIKELY(is_changing_leader_)) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "transaction is preparing changing leader", KR(ret), "context", *this);
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(0 >= size) || OB_UNLIKELY(request_id <= 0) ||
             OB_UNLIKELY(partition_log_info_arr.count() <= 0) || OB_UNLIKELY(commit_version <= 0)) {
    TRANS_LOG(WARN,
        "invalid argument",
        KP(buf),
        K(size),
        K(request_id),
        K(partition_log_info_arr),
        K(commit_version),
        "context",
        *this);
    ret = OB_INVALID_ARGUMENT;
#ifdef TRANS_ERROR
  } else if (0 == ObRandom::rand(1, 100) % 20) {
    ret = OB_BUF_NOT_ENOUGH;
    TRANS_LOG(WARN, "generate redo prepare log random error", KR(ret), "context", *this);
#endif
  } else if (OB_FAIL(ctx_dependency_wrap_.check_can_depend_prev_elr_trans(commit_version))) {
    TRANS_LOG(WARN, "check prev elr trans error", KR(ret), K(request_id), K(commit_version));
  } else if (OB_FAIL(partition_log_info_arr_.assign(partition_log_info_arr))) {
    TRANS_LOG(WARN, "partition log info array assign error", KR(ret), K(partition_log_info_arr));
  } else {
    int64_t tmp_pos = pos;
    int64_t header_pos = pos;
    int64_t redo_log_pos = 0;
    int64_t real_log_type = ObStorageLogType::OB_LOG_UNKNOWN;
    int64_t mutator_size = 0;
    const bool need_flush = true;
    if (OB_FAIL(reserve_log_header_(buf, size, tmp_pos))) {
      TRANS_LOG(WARN, "reserve log header error", KR(ret));
    } else if (OB_FAIL(fill_redo_prepare_log_(buf, size, tmp_pos, redo_log_pos, real_log_type, mutator_size))) {
      if (OB_SIZE_OVERFLOW != ret) {
        TRANS_LOG(WARN, "fill redo log prepare log error", KR(ret), "context", *this);
      }
    } else if ((OB_LOG_TRANS_PREPARE & real_log_type) == 0) {
      TRANS_LOG(WARN, "batch commit buf not enough", K(real_log_type), "context", *this);
      ret = OB_BUF_NOT_ENOUGH;
    } else if (OB_FAIL(fill_log_header_(buf, size, header_pos, real_log_type, trans_id_.get_inc_num()))) {
      TRANS_LOG(WARN, "fill log header error", KR(ret));
    } else if (OB_FAIL(log_info.set(buf + pos, tmp_pos - pos, preassigned_log_meta_, need_flush))) {
      TRANS_LOG(WARN, "log info set error", KR(ret), "context", *this);
    } else if (OB_FAIL(submit_log_cb_.set_log_type(real_log_type))) {
      TRANS_LOG(WARN, "set log type error", KR(ret), "context", *this, K(real_log_type));
    } else if (OB_FAIL(submit_log_cb_.set_real_submit_timestamp(ObClockGenerator::getRealClock()))) {
      TRANS_LOG(WARN, "set submit timestamp error", KR(ret));
    } else if (OB_FAIL(submit_log_cb_.set_have_prev_trans(have_prev_trans))) {
      TRANS_LOG(WARN, "set have prev trans error", KR(ret), K(have_prev_trans), "context", *this);
    } else {
      cb = &submit_log_cb_;
      pos = tmp_pos;
      inc_submit_log_count_();
      inc_submit_log_pending_count_();
      // Record the commit version of the txn. If you can elr later, you
      // need to record it in the trans_result_info_mgr
      global_trans_version_ = commit_version;
      int64_t tmp_config = ObServerConfig::get_instance().trx_2pc_retry_interval;
      if (OB_FAIL(unregister_timeout_task_())) {
        TRANS_LOG(WARN, "unregister timeout handler error", K(ret), "context", *this);
      } else if (OB_FAIL(register_timeout_task_(std::min(tmp_config, (int64_t)MAX_TRANS_2PC_TIMEOUT_US)))) {
        TRANS_LOG(WARN, "register timeout handler error", K(ret), "context", *this);
      } else {
        // do nothing
      }
    }
    if (OB_SUCC(ret)) {
      if (mutator_size > 0) {
        batch_commit_state_ = ObBatchCommitState::GENERATE_REDO_PREPARE_LOG;
      } else {
        batch_commit_state_ = ObBatchCommitState::GENERATE_PREPARE_LOG;
      }
    } else {
      if (mutator_size > 0) {
        mt_ctx_.undo_fill_redo_log();
      }
    }
  }
  REC_TRANS_TRACE_EXT(tlog_,
      generate_redo_prepare_log,
      Y(ret),
      OB_ID(log_id),
      preassigned_log_meta_.get_log_id(),
      OB_ID(arg1),
      preassigned_log_meta_.get_submit_timestamp());

  return ret;
}

int ObPartTransCtx::submit_log(const int64_t log_type)
{
  int ret = OB_SUCCESS;
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);
  if (OB_FAIL(retry_submit_log_(log_type))) {
    TRANS_LOG(WARN, "retry_sumbit_log_ fail", KR(ret), K(log_type), "context", *this);
  }
  return ret;
}

int ObPartTransCtx::retry_submit_log_(const int64_t log_type)
{
  int ret = OB_SUCCESS;
  bool has_redo_log = false;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (OB_UNLIKELY(!ObTransLogType::is_valid(log_type))) {
    TRANS_LOG(WARN, "invalid argument", K(log_type), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else {
    dec_submit_log_pending_count_();
    dec_submit_log_count_();
    if (OB_LOG_SP_TRANS_COMMIT == log_type || OB_LOG_SP_ELR_TRANS_COMMIT == log_type) {
      if (OB_FAIL(submit_log_impl_(log_type, false, false, has_redo_log))) {
        TRANS_LOG(WARN, "submit log error", KR(ret), K(log_type), "context", *this);
      }
    } else if (OB_LOG_TRANS_PREPARE == log_type && is_xa_local_trans()) {
      // for xa prepare version
      if (OB_FAIL(alloc_local_trans_version_(log_type))) {
        if (OB_EAGAIN != ret) {
          TRANS_LOG(WARN, "alloc log id and timestamp error", KR(ret), "context", *this);
        } else {
          ret = OB_SUCCESS;
          inc_submit_log_pending_count_();
          inc_submit_log_count_();
        }
      } else {
        if (OB_FAIL(do_prepare_(OB_SUCCESS))) {
          TRANS_LOG(WARN, "do prepare error", KR(ret), "context", *this);
        } else if (OB_FAIL(submit_log_impl_(log_type, false, false, has_redo_log))) {
          TRANS_LOG(WARN, "submit log error", KR(ret), K(log_type), "context", *this);
        } else {
          is_xa_trans_prepared_ = true;
        }
      }
    } else {
      if (OB_FAIL(alloc_local_trans_version_(log_type))) {
        if (OB_EAGAIN != ret) {
          TRANS_LOG(WARN, "alloc log id and timestamp error", KR(ret), "context", *this);
        } else {
          ret = OB_SUCCESS;
          inc_submit_log_pending_count_();
          inc_submit_log_count_();
        }
      } else {
        if (OB_FAIL(do_prepare_(OB_SUCCESS))) {
          TRANS_LOG(WARN, "do prepare error", KR(ret), "context", *this);
          // In the process of applying for log_id and log_ts, the
          // global_trans_version has been updated. To push up the global
          // publish version, we need to wait for the submitted log's callback
          //} else if ((log_type & OB_LOG_TRANS_COMMIT) && OB_FAIL(do_commit_(local_trans_version_,
          // PartitionLogInfoArray()))) {
          //  TRANS_LOG(WARN, "do commit error", KR(ret), "context", *this);
        } else if (OB_FAIL(submit_log_impl_(log_type, false, false, has_redo_log))) {
          TRANS_LOG(WARN, "submit log error", KR(ret), K(log_type), "context", *this);
        } else {
          // do nothing
        }
      }
    }
  }
  REC_TRANS_TRACE_EXT(tlog_, retry_submit_log, OB_ID(ret), ret, OB_ID(log_type), log_type, OB_ID(uref), get_uref());

  return ret;
}

// Log submission state machine
//
// Because there only exits one on-the-fly log for one txn, we need maintain the
// log submission state machine. There will be 4 log submission chance now:
//
// 1. Normal log submission after commit request
// 2. Leader transfer and not kill the txns
// 3. Immedidate logging
// 4. Logging when rollback to satiesfy the storage requirment
// 5. Logging when mark unlogged trans node for minor merge
//
// So each one may blocked with each other, we handle as the following:
// 1. Normal logging for commit: Set part_trans_action as COMMIT and let the
//    majority process to drive redo/prepare/commit log
// 2. Leader transfer: Set preparing_changing_leader_state to LOGGING_NOT_FINISH
// 3. Immedidate logging: keep the in memory pending_log_size, and let the
//    majority process to check whether need to continue to log
// 4. Logging when rollback: wait until logging to the clog
// 5. Logging for minor: keep the in memory pending callback, and let the
//    majority process to check whether need to log for marked callback
//
// NB: Be careful to modify the comment when change the logging style
int ObPartTransCtx::submit_log_impl_(const int64_t log_type, const bool pending, const bool sync, bool& has_redo_log)
{
  int ret = OB_SUCCESS;
  ClogBuf* clogbuf = NULL;
  int64_t real_log_type = log_type;
  int64_t redo_log_pos = 0;
  bool need_submit_log = true;
  int64_t mutator_size = 0;
  ObTimeGuard timeguard("submit_log_impl", 30 * 1000);

  if (OB_UNLIKELY(submit_log_count_ > 0)) {
    TRANS_LOG(DEBUG, "already submit log count or pending count", KR(ret), "context", *this, K(log_type));
    has_redo_log = false;
  } else if (OB_UNLIKELY(!ObTransLogType::is_valid(log_type))) {
    TRANS_LOG(WARN, "invalid argument", K(log_type), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(clogbuf = ClogBufFactory::alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(ERROR, "alloc clog buffer error", KR(ret));
  } else {
    timeguard.click();
    int64_t pos = 0;
    int64_t header_pos = 0;
    const int64_t size = clogbuf->get_size();
    char* buf = clogbuf->get_buf();
    if (OB_FAIL(reserve_log_header_(buf, size, pos))) {
      TRANS_LOG(WARN, "reserve log header error", KR(ret));
    } else {
      has_redo_log = false;
      switch (log_type) {
        case OB_LOG_SP_TRANS_COMMIT: {
          ret = fill_sp_commit_log_(
              OB_LOG_SP_TRANS_COMMIT, buf, size, pos, real_log_type, has_redo_log, need_submit_log, mutator_size);
          break;
        }
        case OB_LOG_SP_ELR_TRANS_COMMIT: {
          ret = fill_sp_commit_log_(
              OB_LOG_SP_ELR_TRANS_COMMIT, buf, size, pos, real_log_type, has_redo_log, need_submit_log, mutator_size);
          break;
        }
        case OB_LOG_SP_TRANS_ABORT: {
          ret = fill_sp_abort_log_(buf, size, pos, real_log_type);
          break;
        }
        case OB_LOG_TRANS_REDO: {
          ret = fill_redo_log_(buf, size, pos, mutator_size);
          if (OB_EAGAIN == ret) {
            ret = OB_SUCCESS;
          }
          if (OB_SUCCESS == ret) {
            has_redo_log = (mutator_size > 0);
          }
          break;
        }
        case OB_LOG_TRANS_PREPARE: {
          ret = fill_prepare_log_(buf, size, pos);
          break;
        }
        case OB_LOG_TRANS_REDO_WITH_PREPARE: {
          ret = fill_redo_prepare_log_(buf, size, pos, redo_log_pos, real_log_type, mutator_size);
          if (OB_SUCCESS == ret) {
            has_redo_log = (mutator_size > 0);
          }
          break;
        }
        case OB_LOG_TRANS_COMMIT: {
          ret = fill_commit_log_(buf, size, pos);
          break;
        }
        case OB_LOG_TRANS_RECORD: {
          ret = fill_record_log_(buf, size, pos);
          break;
        }
        case OB_LOG_TRANS_PREPARE_WITH_COMMIT: {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        case OB_LOG_TRANS_REDO_WITH_PREPARE_WITH_COMMIT: {
          ret = fill_redo_prepare_commit_log_(buf, size, pos, redo_log_pos, real_log_type, mutator_size);
          if (OB_SUCCESS == ret) {
            has_redo_log = (mutator_size > 0);
          }
          break;
        }
        case OB_LOG_TRANS_REDO_WITH_PREPARE_WITH_COMMIT_WITH_CLEAR: {
          ret = fill_redo_prepare_commit_clear_log_(buf, size, pos, real_log_type, mutator_size);
          if (OB_SUCCESS == ret) {
            has_redo_log = (mutator_size > 0);
          }
          break;
        }
        case OB_LOG_TRANS_ABORT: {
          ret = fill_abort_log_(buf, size, pos);
          break;
        }
        case OB_LOG_TRANS_CLEAR: {
          ret = fill_clear_log_(buf, size, pos);
          break;
        }
        case OB_LOG_TRANS_STATE: {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        case OB_LOG_MUTATOR: {
          ret = fill_mutator_log_(buf, size, pos, mutator_size);
          if (OB_EAGAIN == ret || OB_ENTRY_NOT_EXIST == ret) {
            // We judge the log is full or empty through mutator_size
            ret = OB_SUCCESS;
          }

          if (OB_SUCC(ret)) {
            if (mutator_size > 0) {
              has_redo_log = true;
            } else {
              has_redo_log = false;
              need_submit_log = false;
            }
          }

          break;
        }
        case OB_LOG_MUTATOR_WITH_STATE: {
          ret = fill_mutator_state_log_(buf, size, pos, real_log_type);
          if (OB_SUCCESS == ret) {
            has_redo_log = true;
          }
          break;
        }
        case OB_LOG_MUTATOR_ABORT: {
          ret = fill_mutator_abort_log_(buf, size, pos);
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
      }
    }
    if (OB_SUCC(ret) && need_submit_log) {
      if (OB_FAIL(fill_log_header_(buf, size, header_pos, real_log_type, trans_id_.get_inc_num()))) {
        TRANS_LOG(WARN, "fill log header error", KR(ret));
      }
    }
    timeguard.click();
    if (OB_SUCC(ret) && need_submit_log) {
      if (OB_UNLIKELY(!can_submit_log_(real_log_type))) {
        ret = OB_NOT_SUPPORTED;
        TRANS_LOG(WARN, "not support submit log now", K(ret), K(real_log_type), K(*this));
      } else if (OB_FAIL(submit_log_cb_.set_log_type(real_log_type))) {
        TRANS_LOG(WARN, "set log type error", KR(ret), "context", *this, K(real_log_type));
      } else {
        uint64_t cur_log_id = OB_INVALID_ID;
        int64_t cur_log_timestamp = OB_INVALID_TIMESTAMP;
        const bool with_need_update_version = need_update_trans_version(real_log_type);
        const bool with_base_ts = (OB_SUCCESS == get_status_()) && need_carry_base_ts(real_log_type);
        if (!pending) {
          if (with_need_update_version) {
            if (OB_FAIL(clog_adapter_->submit_log(self_,
                    ObVersion(2),
                    buf,
                    pos,
                    local_trans_version_,
                    &submit_log_cb_,
                    pg_,
                    cur_log_id,
                    cur_log_timestamp))) {
              if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
                TRANS_LOG(WARN, "submit log error", KR(ret), "context", *this);
              }
            } else {
              state_.set_prepare_version(cur_log_timestamp);
              global_trans_version_ = cur_log_timestamp;
              update_max_submitted_log_timestamp_(cur_log_timestamp);
              if (!is_prepared_ && !in_xa_prepare_state_) {
                mt_ctx_.set_prepare_version(cur_log_timestamp);
                is_prepared_ = true;
              }
            }
          } else if (with_base_ts) {
            if (OB_FAIL(clog_adapter_->submit_log(self_,
                    ObVersion(2),
                    buf,
                    pos,
                    global_trans_version_,
                    &submit_log_cb_,
                    pg_,
                    cur_log_id,
                    cur_log_timestamp))) {
              TRANS_LOG(WARN, "submit log error", KR(ret), "context", *this);
              need_print_trace_log_ = true;
            } else {
              update_max_submitted_log_timestamp_(cur_log_timestamp);
            }
          } else {
            if (OB_FAIL(clog_adapter_->submit_log(
                    self_, ObVersion(2), buf, pos, &submit_log_cb_, pg_, cur_log_id, cur_log_timestamp))) {
              TRANS_LOG(WARN, "submit log error", KR(ret), "context", *this);
              need_print_trace_log_ = true;
            } else {
              update_max_submitted_log_timestamp_(cur_log_timestamp);
            }
          }
          if (OB_SUCCESS == ret) {
            inc_submit_log_count_();
            log_type_ = real_log_type;
            if (OB_LOG_SP_ELR_TRANS_COMMIT == real_log_type || OB_LOG_SP_TRANS_COMMIT == real_log_type) {
              if (can_elr_) {
                set_elr_preparing_();
                if (OB_FAIL(check_and_early_release_lock_())) {
                  TRANS_LOG(WARN, "early release lock error", KR(ret), "context", *this);
                }
              }
            }
            TRANS_LOG(DEBUG, "submit log success", "context", *this);
          }
        }
        if (pending || (!sync && ObClogAdapter::need_retry(ret))) {
          // rewrite ret
          ret = OB_SUCCESS;
          log_type_ = real_log_type;
          if (OB_FAIL(clog_adapter_->submit_log_task(self_,
                  ObVersion(2),
                  buf,
                  pos,
                  with_need_update_version,
                  local_trans_version_,
                  with_base_ts,
                  global_trans_version_,
                  &submit_log_cb_))) {
          } else {
            inc_submit_log_pending_count_();
            inc_submit_log_count_();
          }
        }
        timeguard.click();
        REC_TRANS_TRACE_EXT(tlog_, submit_log, Y(ret), OB_ID(uref), get_uref(), Y(timeguard));
      }
    }
    if (OB_SUCCESS != ret && has_redo_log) {
      // The log fails during the process of generating or submitting, and needs to be undone
      mt_ctx_.undo_fill_redo_log();
    }
    ClogBufFactory::release(clogbuf);
    timeguard.click();
    // The temporary modification, follow-up needs to be made controllable
    if (timeguard.get_diff() > 30 * 1000) {
      need_print_trace_log_ = true;
      TRANS_LOG(WARN,
          "submitting log costs too much time",
          KR(ret),
          K(timeguard),
          K_(trans_id),
          "partition",
          self_,
          K_(state),
          K(real_log_type),
          "log_size",
          pos);
    }
  }

  return ret;
}

int ObPartTransCtx::post_response_(
    const common::ObPartitionKey& receiver, const int64_t raw_msg_type, const int64_t sql_no, bool& partition_exist)
{
  UNUSED(sql_no);
  int ret = OB_SUCCESS;
  int msg_type = msg_type_switch_(raw_msg_type);
  ObTransMsgUnion msg_union(msg_type);
  ObTransMsg& msg = msg_union.data_.trans_msg;
  const bool nonblock = true;
  int64_t trans_version = state_.get_prepare_version();

  // After the transaction started 2pc, sql_no is not assigned in msg,
  //  so checking sql_no is unnecessary.
  if (OB_UNLIKELY(!receiver.is_valid()) ||
      (!ObTransMsgTypeChecker::is_valid_msg_type(msg_type) && !ObTransMsgType2Checker::is_valid_msg_type(msg_type))) {
    TRANS_LOG(WARN, "invalid argument", K(receiver), K(msg_type), K(sql_no), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else {
    // The code commented below has bugs. For a single partition transaction
    // with multiple redo logs, the commit version will become smaller
    // (see bug:11315769)
    //
    // if (ObTransVersion::INVALID_TRANS_VERSION != global_trans_version_) {
    //  trans_version = global_trans_version_;
    //}
    if (OB_TRANS_2PC_LOG_ID_RESPONSE == msg_type) {
      if (batch_commit_trans_) {
        if (OB_FAIL(msg.init(tenant_id_,
                trans_id_,
                msg_type,
                trans_expired_time_,
                self_,
                receiver,
                trans_param_,
                prepare_log_id_,
                prepare_log_timestamp_,
                addr_,
                get_status_(),
                request_id_,
                partition_log_info_arr_))) {
          TRANS_LOG(WARN, "transaction 2pc log id response init error", KR(ret), K(receiver), K(msg_type));
        }
      } else {
        // For transactions that are not batch commit trans,
        // partition_log_inf_arr does not need to be passed in the prepare ok
        // message
        PartitionLogInfoArray partition_log_info_arr;
        if (OB_FAIL(msg.init(tenant_id_,
                trans_id_,
                msg_type,
                trans_expired_time_,
                self_,
                receiver,
                trans_param_,
                prepare_log_id_,
                prepare_log_timestamp_,
                addr_,
                get_status_(),
                request_id_,
                partition_log_info_arr))) {
          TRANS_LOG(WARN, "transaction 2pc log id response init error", KR(ret), K(receiver), K(msg_type));
        }
      }
    } else if (OB_TRANS_2PC_PREPARE_RESPONSE == msg_type) {
      bool is_xa_prepare = false;
      if (is_xa_local_trans() && Ob2PCState::INIT == get_state_()) {
        // in_xa_prepare_state
        is_xa_prepare = true;
      }
      if (batch_commit_trans_ || Ob2PCState::COMMIT == state_.get_state()) {
        if (OB_FAIL(msg.init(tenant_id_,
                trans_id_,
                msg_type,
                trans_expired_time_,
                self_,
                receiver,
                scheduler_,
                coordinator_,
                participants_,
                trans_param_,
                prepare_log_id_,
                prepare_log_timestamp_,
                addr_,
                get_status_(),
                state_.get_state(),
                trans_version,
                request_id_,
                partition_log_info_arr_,
                get_remaining_wait_interval_us_(),
                trace_info_.get_app_trace_info(),
                xid_,
                is_xa_prepare))) {
          TRANS_LOG(WARN, "transaction 2pc prepare response init error", KR(ret), K(receiver), K(msg_type));
        }
      } else {
        // For transactions that are not batch commit trans,
        // partition_log_inf_arr does not need to be passed in the prepare ok
        // message
        PartitionLogInfoArray partition_log_info_arr;
        if (OB_FAIL(msg.init(tenant_id_,
                trans_id_,
                msg_type,
                trans_expired_time_,
                self_,
                receiver,
                scheduler_,
                coordinator_,
                participants_,
                trans_param_,
                prepare_log_id_,
                prepare_log_timestamp_,
                addr_,
                get_status_(),
                state_.get_state(),
                trans_version,
                request_id_,
                partition_log_info_arr,
                get_remaining_wait_interval_us_(),
                trace_info_.get_app_trace_info(),
                xid_,
                is_xa_prepare))) {
          TRANS_LOG(WARN, "transaction 2pc prepare response init error", KR(ret), K(receiver), K(msg_type));
        }
      }
    } else if (OB_TRANS_2PC_COMMIT_RESPONSE == msg_type || OB_TRANS_2PC_PRE_COMMIT_RESPONSE == msg_type) {
      if (OB_SUCCESS != get_status_()) {
        TRANS_LOG(ERROR, "unexpected status", "status", get_status_(), "context", *this);
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(msg.init(tenant_id_,
                     trans_id_,
                     msg_type,
                     trans_expired_time_,
                     self_,
                     receiver,
                     scheduler_,
                     coordinator_,
                     participants_,
                     trans_param_,
                     get_global_trans_version_(),
                     addr_,
                     request_id_,
                     partition_log_info_arr_,
                     get_status_()))) {
        TRANS_LOG(WARN, "transaction 2pc commit response init error", KR(ret), K(receiver), K(msg_type));
      } else {
        // do nothing
      }
    } else if (OB_TRANS_2PC_ABORT_RESPONSE == msg_type) {
      if (OB_SUCCESS == get_status_()) {
        TRANS_LOG(WARN, "unexpected status", "status", get_status_(), "context", *this);
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(msg.init(tenant_id_,
                     trans_id_,
                     msg_type,
                     trans_expired_time_,
                     self_,
                     receiver,
                     scheduler_,
                     coordinator_,
                     participants_,
                     trans_param_,
                     addr_,
                     request_id_,
                     get_status_()))) {
        TRANS_LOG(WARN, "transaction 2pc abort response init error", KR(ret), K(receiver), K(msg_type));
      } else {
        // do nothing
      }
    } else if (OB_TRANS_2PC_CLEAR_RESPONSE == msg_type || OB_TRANS_2PC_COMMIT_CLEAR_RESPONSE == msg_type) {
      if (OB_FAIL(msg.init(tenant_id_,
              trans_id_,
              msg_type,
              trans_expired_time_,
              self_,
              receiver,
              scheduler_,
              coordinator_,
              participants_,
              trans_param_,
              addr_,
              get_status_(),
              request_id_,
              stc_))) {
        TRANS_LOG(WARN, "transaction 2pc clear response init error", KR(ret), K(receiver), K(msg_type));
      }
    } else if (OB_TRX_2PC_CLEAR_RESPONSE == msg_type) {
      ObTrx2PCClearResponse& trx_2pc_clear_res = msg_union.data_.trx_2pc_clear_res;
      if (OB_FAIL(trx_2pc_clear_res.init(tenant_id_,
              trans_id_,
              msg_type,
              trans_expired_time_,
              self_,
              receiver,
              scheduler_,
              coordinator_,
              participants_,
              trans_param_,
              addr_,
              request_id_,
              get_status_(),
              clear_log_base_ts_))) {
        TRANS_LOG(WARN, "transaction 2pc clear response init error", K(ret), K(receiver), K(msg_type));
      }
    } else if (OB_TRX_2PC_ABORT_RESPONSE == msg_type) {
      ObTrx2PCAbortResponse& trx_2pc_abort_res = msg_union.data_.trx_2pc_abort_res;
      if (OB_SUCCESS == get_status_()) {
        TRANS_LOG(WARN, "unexpected status", "status", get_status_(), "context", *this);
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(trx_2pc_abort_res.init(tenant_id_,
                     trans_id_,
                     msg_type,
                     trans_expired_time_,
                     self_,
                     receiver,
                     scheduler_,
                     coordinator_,
                     participants_,
                     trans_param_,
                     addr_,
                     request_id_,
                     get_status_()))) {
        TRANS_LOG(WARN, "transaction 2pc clear response init error", K(ret), K(receiver), K(msg_type));
      } else {
        // do nothing
      }
    } else if (OB_TRX_2PC_COMMIT_RESPONSE == msg_type) {
      ObTrx2PCCommitResponse& trx_2pc_commit_res = msg_union.data_.trx_2pc_commit_res;
      if (OB_SUCCESS != get_status_()) {
        TRANS_LOG(ERROR, "unexpected status", "status", get_status_(), "context", *this);
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(trx_2pc_commit_res.init(tenant_id_,
                     trans_id_,
                     msg_type,
                     trans_expired_time_,
                     self_,
                     receiver,
                     scheduler_,
                     coordinator_,
                     participants_,
                     trans_param_,
                     get_global_trans_version_(),
                     addr_,
                     request_id_,
                     partition_log_info_arr_,
                     get_status_(),
                     clear_log_base_ts_))) {
        TRANS_LOG(WARN, "transaction 2pc commit response init error", K(ret), K(receiver), K(msg_type));
      } else {
        // do nothing
      }
    } else if (OB_TRX_2PC_PRE_COMMIT_RESPONSE == msg_type) {
      ObTrx2PCPreCommitResponse& trx_2pc_pre_commit_res = msg_union.data_.trx_2pc_pre_commit_res;
      if (OB_SUCCESS != get_status_()) {
        TRANS_LOG(ERROR, "unexpected status", "status", get_status_(), "context", *this);
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(trx_2pc_pre_commit_res.init(tenant_id_,
                     trans_id_,
                     msg_type,
                     trans_expired_time_,
                     self_,
                     receiver,
                     scheduler_,
                     coordinator_,
                     participants_,
                     trans_param_,
                     get_global_trans_version_(),
                     addr_,
                     request_id_,
                     partition_log_info_arr_,
                     get_status_()))) {
        TRANS_LOG(WARN, "transaction 2pc commit response init error", K(ret), K(receiver), K(msg_type));
      } else {
        // do nothing
      }
    } else if (OB_TRX_2PC_PREPARE_RESPONSE == msg_type) {
      int tmp_ret = OB_SUCCESS;
      int64_t publish_version = 0;
      bool is_xa_prepare = false;
      ObITsMgr* ts_mgr = get_ts_mgr_();
      if (is_xa_local_trans() && in_xa_prepare_state_) {
        // The two-phase commit state here is Ob2PCState::INIT, and the
        // is_redo_prepared is true
        is_xa_prepare = true;
      }
      if (OB_ISNULL(ts_mgr)) {
        TRANS_LOG(WARN, "ts mgr is null");
        // ignore this failure
      } else if (OB_SUCCESS != (tmp_ret = ts_mgr->get_publish_version(tenant_id_, publish_version))) {
        TRANS_LOG(WARN, "get publish version error", K(tmp_ret), K(publish_version), "context", *this);
        // ignore this failure
      } else {
        // do nothing
      }
      ObTrx2PCPrepareResponse& trx_2pc_prepare_res = msg_union.data_.trx_2pc_prepare_res;
      if (batch_commit_trans_ || Ob2PCState::COMMIT == state_.get_state()) {
        if (OB_FAIL(trx_2pc_prepare_res.init(tenant_id_,
                trans_id_,
                msg_type,
                trans_expired_time_,
                self_,
                receiver,
                scheduler_,
                coordinator_,
                participants_,
                trans_param_,
                prepare_log_id_,
                prepare_log_timestamp_,
                addr_,
                get_status_(),
                state_.get_state(),
                trans_version,
                request_id_,
                partition_log_info_arr_,
                get_remaining_wait_interval_us_(),
                trace_info_.get_app_trace_info(),
                publish_version,
                xid_,
                is_xa_prepare))) {
          TRANS_LOG(WARN, "transaction 2pc prepare response init error", KR(ret), K(receiver), K(msg_type));
        }
      } else {
        // For transactions that are not batch commit trans,
        // partition_log_inf_arr does not need to be passed in the prepare ok
        // message
        PartitionLogInfoArray partition_log_info_arr;
        if (OB_FAIL(trx_2pc_prepare_res.init(tenant_id_,
                trans_id_,
                msg_type,
                trans_expired_time_,
                self_,
                receiver,
                scheduler_,
                coordinator_,
                participants_,
                trans_param_,
                prepare_log_id_,
                prepare_log_timestamp_,
                addr_,
                get_status_(),
                state_.get_state(),
                trans_version,
                request_id_,
                partition_log_info_arr,
                get_remaining_wait_interval_us_(),
                trace_info_.get_app_trace_info(),
                publish_version,
                xid_,
                is_xa_prepare))) {
          TRANS_LOG(WARN, "transaction 2pc prepare response init error", K(ret), K(receiver), K(msg_type));
        } else if (OB_FAIL(ObTransSplitAdapter::set_split_info_for_prepare_response(
                       partition_service_, self_, trx_2pc_prepare_res))) {
          TRANS_LOG(WARN, "failed to set split info in 2pc prepare response", K(ret));
        }
      }
    } else {
      TRANS_LOG(ERROR, "invalid message type", "context", *this, K(msg_type));
      ret = OB_TRANS_INVALID_MESSAGE_TYPE;
    }

    if (OB_SUCC(ret)) {
      bool need_response = true;
      int tmp_ret = OB_SUCCESS;
      if (same_leader_batch_partitions_count_ > 0) {
        ObPartitionArray batch_partitions;
        int64_t same_leader_batch_base_ts = 0;
        int64_t base_ts = 0;
        if (OB_TRX_2PC_COMMIT_RESPONSE == msg_type) {
          base_ts = clear_log_base_ts_;
        }
        if (OB_UNLIKELY(OB_SUCCESS !=
                        (tmp_ret = trans_service_->get_part_trans_same_leader_batch_rpc_mgr()->add_batch_rpc(trans_id_,
                             msg_type,
                             self_,
                             base_ts,
                             same_leader_batch_partitions_count_,
                             need_response,
                             batch_partitions,
                             same_leader_batch_base_ts)))) {
          TRANS_LOG(WARN, "add batch rpc error", K(tmp_ret), K(*this), K(msg_type));
        } else if (need_response && OB_UNLIKELY(OB_SUCCESS != (tmp_ret = msg_union.set_batch_same_leader_partitions(
                                                                   batch_partitions, same_leader_batch_base_ts)))) {
          TRANS_LOG(WARN,
              "set batch same leader partitions error",
              K(tmp_ret),
              K(msg_type),
              K(batch_partitions),
              K(same_leader_batch_base_ts));
        } else {
          // do nothing
        }
      }
      if (OB_SUCCESS != tmp_ret) {
        // We need send rpc directly
        need_response = true;
      }
      if (need_response) {
        if (ObTransMsgType2Checker::is_valid_msg_type(msg_type)) {
          if (OB_FAIL(post_trans_msg_(receiver, msg_union.data_.trx_msg_base, msg_type, nonblock, partition_exist))) {
            TRANS_LOG(DEBUG, "post transaction message error", K(ret), K(receiver));
          }
        } else if (OB_FAIL(post_trans_msg_(receiver, msg, msg_type, nonblock, partition_exist))) {
          TRANS_LOG(DEBUG, "post transaction message error", K(ret), K(receiver));
        } else {
          // do nothing
        }
      }
    }
  }

  return ret;
}

int ObPartTransCtx::post_trans_response_(const int64_t msg_type)
{
  int ret = OB_SUCCESS;
  ObTransMsg msg;

  if (OB_UNLIKELY(!ObTransMsgTypeChecker::is_valid_msg_type(msg_type))) {
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

int ObPartTransCtx::post_stmt_response_(
    const int64_t msg_type, const int64_t sql_no, const int status, const int64_t request_timeout)
{
  int ret = OB_SUCCESS;
  ObTransMsg msg;

  if (OB_UNLIKELY(!ObTransMsgTypeChecker::is_valid_msg_type(msg_type)) || OB_UNLIKELY(sql_no < 0)) {
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
              status,
              request_id_))) {
        TRANS_LOG(WARN, "message init error", K(ret), K_(scheduler), K_(tmp_scheduler), K(msg_type));

        // Record the sending timestamp of the request in the response, which is
        // used by the scheduler to verify the timeout of the message

      } else if (OB_FAIL(msg.set_msg_timeout(request_timeout))) {
        TRANS_LOG(INFO,
            "set message start timestamp error",
            K(ret),
            K(msg_type),
            K(sql_no),
            K(status),
            K(request_timeout),
            K(*this));
      } else {
        // do nothing
      }
    } else if (OB_TRANS_STMT_ROLLBACK_RESPONSE == msg_type) {
      if (OB_FAIL(msg.init(tenant_id_,
              trans_id_,
              msg_type,
              trans_expired_time_,
              self_,
              SCHE_PARTITION_ID,
              trans_param_,
              addr_,
              sql_no,
              status,
              request_id_))) {
        TRANS_LOG(WARN, "message init error", K(ret), K_(scheduler), K_(tmp_scheduler), K(msg_type));
        // record request timestamp into response for checking timeout in scheduler
      } else if (OB_FAIL(msg.set_msg_timeout(request_timeout))) {
        TRANS_LOG(INFO,
            "set message start timestamp error",
            K(ret),
            K(msg_type),
            K(sql_no),
            K(status),
            K(request_timeout),
            K(*this));
      } else {
        // do nothing
      }
    } else if (OB_TRANS_SAVEPOINT_ROLLBACK_RESPONSE == msg_type) {
      if (OB_FAIL(msg.init(tenant_id_,
              trans_id_,
              msg_type,
              trans_expired_time_,
              self_,
              SCHE_PARTITION_ID,
              trans_param_,
              addr_,
              sql_no,
              status,
              request_id_))) {
        TRANS_LOG(WARN, "message init error", K(ret), K_(scheduler), K_(tmp_scheduler), K(msg_type));
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
              status,
              request_id_))) {
        TRANS_LOG(WARN, "message init error", K(ret), K_(scheduler), K_(tmp_scheduler), K(msg_type));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(post_trans_msg_(tenant_id_, tmp_scheduler_, msg, msg_type))) {
      TRANS_LOG(WARN, "post transaction message error", KR(ret), K_(tmp_scheduler), K(msg_type));
    }
  }

  return ret;
}

int ObPartTransCtx::post_2pc_response_(const ObPartitionKey& partition, const int64_t msg_type)
{
  int ret = OB_SUCCESS;
  ObTransMsg msg;
  bool partition_exist = true;

  if (OB_UNLIKELY(!partition.is_valid()) || OB_UNLIKELY(!ObTransMsgTypeChecker::is_valid_msg_type(msg_type))) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(msg_type), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(post_response_(partition, msg_type, stmt_info_.get_sql_no(), partition_exist))) {
    TRANS_LOG(DEBUG, "post response error", KR(ret), K(partition), K(msg_type));
    if (!partition_exist) {
      ObPartitionKey dummy_pkey;
      // TODO. rename get_gts_pkey to get_dummy_pkey
      if (OB_FAIL(get_gts_pkey(partition.get_tenant_id(), dummy_pkey))) {
        TRANS_LOG(WARN, "get dummy pkey failed", KR(ret), K_(tenant_id), K(msg_type));
      } else if (OB_FAIL(post_response_(dummy_pkey, msg_type, stmt_info_.get_sql_no(), partition_exist))) {
        TRANS_LOG(WARN, "post response to dummy partition failed", KR(ret), K(dummy_pkey), K(msg_type));
      } else {
        TRANS_LOG(INFO, "post response to dummy partition success", K(dummy_pkey), K(msg_type));
      }
    }
  } else {
    // do nothing
  }

  return ret;
}

int ObPartTransCtx::update_global_trans_version_(const int64_t trans_version)
{
  int ret = OB_SUCCESS;
  global_trans_version_ = trans_version;
  REC_TRANS_TRACE_EXT(tlog_, update_trans_version, OB_ID(trans_version), trans_version, OB_ID(uref), get_uref());
  return ret;
}

int ObPartTransCtx::handle_start_stmt_request_(const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_UNLIKELY(Ob2PCState::INIT != get_state_())) {
    // Filter the start_stmt_request message. see bug:11471629
    need_print_trace_log_ = true;
    ret = OB_STATE_NOT_MATCH;
    TRANS_LOG(INFO, "state not match, discard message", KR(ret), "context", *this, K(msg));
  } else if (OB_UNLIKELY(is_in_2pc_())) {
    TRANS_LOG(WARN, "transaction is in 2pc", "context", *this);
    ret = OB_TRANS_HAS_DECIDED;
  } else {
    if (OB_FAIL(start_stmt_(msg.get_sql_no()))) {
      TRANS_LOG(WARN, "partiticpant start stmt error", KR(ret), "context", *this, K(msg));
    }
    // also use status_ before 2PC. reply ret to scheduler via msg
    if (OB_SUCCESS != (tmp_ret = post_stmt_response_(OB_TRANS_START_STMT_RESPONSE, msg.get_sql_no(), ret))) {
      TRANS_LOG(WARN, "post rpc create ctx response fail", "ret", tmp_ret, K(msg));
    }
  }

  return ret;
}

// We neednot response after handle the message
int ObPartTransCtx::handle_trans_discard_request_(const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(Ob2PCState::INIT != get_state_())) {
    TRANS_LOG(ERROR, "protocol error", K(msg), "context", *this);
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(OB_TRANS_DISCARD_REQUEST != msg.get_msg_type())) {
    TRANS_LOG(WARN, "unexpected msg_type", K(msg), "context", *this);
    ret = OB_ERR_UNEXPECTED;
  } else if (is_logging_()) {
    TRANS_LOG(INFO, "transaction is logging", "context", *this);
  } else if (has_logged_()) {
    TRANS_LOG(INFO, "transaction has logged", "context", *this);
    if (Ob2PCState::INIT == get_state_()) {
      if (redo_log_no_ > 0) {
        if (OB_FAIL(compensate_prepare_no_log_())) {
          TRANS_LOG(WARN, "compensate prepare no log error", KR(ret), "context", *this);
        }
      } else if (mutator_log_no_ > 0 || has_trans_state_log_) {
        if (OB_FAIL(compensate_mutator_abort_log_())) {
          TRANS_LOG(WARN, "compensate mutator abort log error", KR(ret), "context", *this);
        }
      } else {
        // do nothing
      }
    }
  } else {
    TRANS_LOG(DEBUG, "handle discard msg success", K(msg), "context", *this);
    set_exiting_();
    (void)unregister_timeout_task_();
    const bool commit = false;
    (void)trans_end_(commit, get_global_trans_version_());
    (void)trans_clear_();
  }

  return ret;
}

int ObPartTransCtx::handle_trans_clear_request_(const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;
  bool has_redo_log = false;

  if (OB_UNLIKELY(Ob2PCState::INIT != get_state_())) {
    TRANS_LOG(ERROR, "protocol error", K(msg), "context", *this);
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(OB_TRANS_CLEAR_REQUEST != msg.get_msg_type())) {
    TRANS_LOG(WARN, "unexpected msg_type", K(msg), "context", *this);
    ret = OB_ERR_UNEXPECTED;
  } else if (is_logging_()) {
    TRANS_LOG(INFO, "is logging when handle trans clear request", "context", *this);
    if (Ob2PCState::INIT == get_state_()) {
      part_trans_action_ = ObPartTransAction::DIED;
    }
  } else if (has_logged_()) {
    TRANS_LOG(INFO, "transaction has logged", "context", *this);
    if (Ob2PCState::INIT == get_state_()) {
      if (redo_log_no_ > 0) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unexpected error, redo_log_no_ > 0", KR(ret), "context", *this);
      } else if (mutator_log_no_ > 0 || has_trans_state_log_) {
        if (OB_FAIL(submit_log_async_(OB_LOG_MUTATOR_ABORT, has_redo_log))) {
          TRANS_LOG(WARN, "submit mutator abort log failed", KR(ret), "context", *this);
        } else {
          TRANS_LOG(INFO, "submit mutator abort log success", "context", *this);
        }
      } else {
        // do nothing
      }
    }
  } else {
    TRANS_LOG(DEBUG, "handle clear msg success", K(msg), "context", *this);
    set_exiting_();
    (void)unregister_timeout_task_();
    const bool commit = false;
    (void)trans_end_(commit, get_global_trans_version_());
    (void)trans_clear_();
    const int64_t msg_type = OB_TRANS_CLEAR_RESPONSE;
    if (OB_FAIL(post_trans_response_(msg_type))) {
      TRANS_LOG(WARN, "post transaction response error", KR(ret), K(msg_type));
    }
  }

  return ret;
}

int ObPartTransCtx::handle_stmt_rollback_request_(const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const bool is_rollback = true;
  bool need_response = true;

  if (OB_UNLIKELY(Ob2PCState::INIT != get_state_())) {
    // unexpect to receive a stmt rollback request in other state
    TRANS_LOG(ERROR, "protocol error", K(msg), "context", *this);
    ret = OB_ERR_UNEXPECTED;
  } else if (is_in_2pc_()) {
    TRANS_LOG(WARN, "transaction is in 2pc", "context", *this);
    ret = OB_TRANS_HAS_DECIDED;
  } else {
    if (OB_FAIL(end_stmt_(is_rollback, msg.get_sql_no(), msg.get_additional_sql_no(), need_response))) {
      TRANS_LOG(WARN, "participant end stmt error", KR(ret), "context", *this, K(is_rollback));
    }
    if (need_response) {
      if (OB_SUCCESS != (tmp_ret = post_stmt_response_(
                             OB_TRANS_STMT_ROLLBACK_RESPONSE, msg.get_sql_no(), ret, msg.get_msg_timeout()))) {
        TRANS_LOG(WARN, "post stmt rollback response error", "ret", tmp_ret, K(msg), "context", *this);
      }
    }
  }

  return ret;
}

int ObPartTransCtx::handle_2pc_log_id_request_(const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(post_2pc_response_(coordinator_, OB_TRANS_2PC_LOG_ID_RESPONSE))) {
    TRANS_LOG(WARN, "submit 2pc log id response error", KR(ret), K(msg));
  } else {
    TRANS_LOG(INFO, "handle 2pc log id response success", K(msg), "context", *this);
  }

  return ret;
}

int ObPartTransCtx::handle_2pc_prepare_request_raw_(int status)
{
  int ret = OB_SUCCESS;
  const int64_t state = get_state_();
  part_trans_action_ = ObPartTransAction::COMMIT;
  trans_type_ = TransType::DIST_TRANS;

  switch (state) {
    case Ob2PCState::INIT: {
      in_xa_prepare_state_ = false;
      if (is_logging_()) {
        // it is logging now. just discard message
        TRANS_LOG(DEBUG, "participant is logging now, discard message", K_(self), K_(trans_id), K(state));
      } else {
        bool unused = false;
        if (is_xa_local_trans() && is_redo_prepared_) {
          if (OB_SUCCESS != get_status_() || (OB_SUCCESS != status && OB_TRANS_ROLLBACKED != status)) {
            // The status of the coordinator at the time of xa commit is OB_SUCCESS
            // The status of the coordinator at the time of xa rollback is OB_TRANS_ROLLBACKED
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(ERROR, "cannot prepare this participant", "context", *this);
          } else if (OB_FAIL(do_prepare_(status))) {
            TRANS_LOG(WARN, "do prepare error", K(ret), "context", *this);
          } else if (OB_FAIL(alloc_local_trans_version_(OB_LOG_TRANS_PREPARE))) {
            if (OB_EAGAIN != ret) {
              TRANS_LOG(WARN, "alloc log id and timestamp error, need retry", K(ret), "context", *this);
            } else {
              ret = OB_SUCCESS;
              inc_submit_log_pending_count_();
              inc_submit_log_count_();
            }
          } else if (OB_FAIL(submit_log_async_(OB_LOG_TRANS_PREPARE, unused))) {
            TRANS_LOG(WARN, "submit prepare log error", K(ret), "context", *this);
          } else {
            // do nothing
          }
          // The coordinator will retry on failure
          ret = OB_SUCCESS;
        } else if (OB_SUCCESS != get_status_() || OB_SUCCESS != status) {
          TRANS_LOG(WARN, "2pc prepare status not ok, write prepare-no", K(status), K(status_), K(*this));
          if (OB_FAIL(alloc_local_trans_version_(OB_LOG_TRANS_PREPARE))) {
            if (OB_EAGAIN != ret) {
              TRANS_LOG(WARN, "alloc log id and timestamp error", KR(ret), "context", *this);
            } else {
              ret = OB_SUCCESS;
              inc_submit_log_pending_count_();
              inc_submit_log_count_();
            }
          } else if (OB_FAIL(do_prepare_(status))) {
            TRANS_LOG(WARN, "do prepare error", KR(ret), "context", *this);
          } else if (OB_FAIL(submit_log_async_(OB_LOG_TRANS_PREPARE, unused))) {
            TRANS_LOG(WARN, "submit prepare log error", KR(ret), "context", *this);
          } else {
            // do nothing
          }
        } else {
          bool can_alloc_log = false;
          int state = ObTransResultState::INVALID;
          if (ctx_dependency_wrap_.get_prev_trans_arr_count() <= 0) {
            can_alloc_log = true;
          } else if (OB_FAIL(ctx_dependency_wrap_.get_prev_trans_arr_result(state))) {
            TRANS_LOG(WARN, "get prev trans arr result error", K(ret), "context", *this);
          } else if (ObTransResultState::is_commit(state)) {
            // The status of predecessors are commit, so we can go on to alloc the log
            can_alloc_log = true;
            ctx_dependency_wrap_.reset_prev_trans_arr();
          } else if (ObTransResultState::is_abort(state)) {
            can_alloc_log = true;
            set_status_(OB_TRANS_ROLLBACKED);
            ctx_dependency_wrap_.reset_prev_trans_arr();
            need_print_trace_log_ = true;
            TRANS_LOG(WARN, "current transaction need to rollback because of prev abort trans", "context", *this);
          } else {
            // The predecessor transaction status is not decided, and the coordinator cannot response here
            can_alloc_log = false;
            TRANS_LOG(INFO, "prev transaction has not decided, need to retry", "context", *this);
          }
          if (can_alloc_log && OB_SUCC(ret)) {
            if (OB_FAIL(alloc_local_trans_version_(OB_LOG_TRANS_REDO_WITH_PREPARE))) {
              if (OB_EAGAIN != ret) {
                TRANS_LOG(WARN, "alloc log id and timestamp error", KR(ret), "context", *this);
              } else {
                ret = OB_SUCCESS;
                inc_submit_log_pending_count_();
                inc_submit_log_count_();
              }
            } else if (OB_FAIL(do_prepare_(get_status_()))) {
              TRANS_LOG(WARN, "do prepare error", KR(ret), "context", *this);
            } else if (OB_FAIL(submit_log_async_(OB_LOG_TRANS_REDO_WITH_PREPARE, unused))) {
              TRANS_LOG(WARN, "submit redo_prepare log error", KR(ret), "context", *this);
            } else {
              // do nothing
            }
          }
        }

        // The predecessor transaction status is not decided, and the
        // coordinator cannot response here
        if (OB_SUCCESS != ret && !has_logged_()) {
          const bool commit = false;
          (void)trans_end_(commit, get_global_trans_version_());
          (void)trans_clear_();
          set_exiting_();
          (void)unregister_timeout_task_();
          set_status_(OB_TRANS_ROLLBACKED);
        }
      }
      break;
    }
    case Ob2PCState::PRE_PREPARE: {
      if (!batch_commit_trans_) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "state not match", K(ret), "context", *this);
        need_print_trace_log_ = true;
      } else {
        TRANS_LOG(DEBUG, "still in pre_prepare, unfinish write prepare log", "context", *this);
      }
      break;
    }
    case Ob2PCState::PREPARE: {
      bool can_response = false;
      int state = ObTransResultState::INVALID;
      if (ctx_dependency_wrap_.get_prev_trans_arr_count() <= 0) {
        can_response = true;
      } else if (OB_FAIL(ctx_dependency_wrap_.get_prev_trans_arr_result(state))) {
        TRANS_LOG(WARN, "get prev trans arr result error", K(ret), "context", *this);
      } else if (ObTransResultState::is_commit(state)) {
        can_response = true;
      } else if (ObTransResultState::is_abort(state) || OB_SUCCESS != get_status_()) {
        set_status_(OB_TRANS_ROLLBACKED);
        can_response = true;
        need_print_trace_log_ = true;
        TRANS_LOG(WARN, "current transaction need to rollback because of prev abort trans", "context", *this);
      } else {
        // The predecessor transaction status is not decided, and the coordinator cannot response here
      }
      if (can_response && OB_FAIL(post_2pc_response_(coordinator_, OB_TRANS_2PC_PREPARE_RESPONSE))) {
        TRANS_LOG(WARN, "submit 2pc response error", K(ret), K(*this));
      }
      break;
    }
    case Ob2PCState::COMMIT:
      // For the scenerio that a participant is in the commit phase, and the
      // other participants are in the preare phase, this is possible, So at
      // this time, we need to carry global_trans_version in the reply prepare
      // ok message, otherwise it may create inconsistency: see bug 11495025
      if (global_trans_version_ < state_.get_prepare_version()) {
        TRANS_LOG(ERROR,
            "global trans version is lower than local trans version, unexpected error",
            K_(global_trans_version),
            K_(local_trans_version),
            "context",
            *this);
      } else {
        state_.set_prepare_version(global_trans_version_);
        if (OB_FAIL(post_2pc_response_(coordinator_, OB_TRANS_2PC_PREPARE_RESPONSE))) {
          TRANS_LOG(WARN, "submit 2pc response error", K(ret), K(*this));
        }
      }
      break;
    case Ob2PCState::ABORT:
    // go through
    case Ob2PCState::CLEAR: {
      if (OB_FAIL(post_2pc_response_(coordinator_, OB_TRANS_2PC_PREPARE_RESPONSE))) {
        TRANS_LOG(WARN, "submit 2pc response error", K(ret), K(*this));
      }
      break;
    }
    default: {
      TRANS_LOG(WARN, "invalid 2pc state", K(state), K(*this));
      ret = OB_TRANS_INVALID_STATE;
      break;
    }
  }

  return ret;
}

int ObPartTransCtx::handle_2pc_prepare_redo_request_raw_(int status)
{
  int ret = OB_SUCCESS;
  const int64_t state = get_state_();
  part_trans_action_ = ObPartTransAction::COMMIT;
  trans_type_ = TransType::DIST_TRANS;

  switch (state) {
    case Ob2PCState::INIT: {
      if (is_redo_prepared_) {
        if (OB_FAIL(post_2pc_response_(coordinator_, OB_TRANS_2PC_PREPARE_RESPONSE))) {
          TRANS_LOG(WARN, "submit 2pc response error", K(ret), K(*this));
        }
      } else {
        in_xa_prepare_state_ = true;
        if (is_logging_()) {
          // it is logging now. just discard message
          TRANS_LOG(DEBUG, "participant is logging now, discard message", K_(self), K_(trans_id), K(state));
        } else {
          bool unused = false;
          if (OB_SUCCESS != get_status_() || OB_SUCCESS != status) {
            if (OB_FAIL(do_prepare_(status))) {
              TRANS_LOG(WARN, "do prepare error", KR(ret), "context", *this);
            } else if (OB_FAIL(post_2pc_response_(coordinator_, OB_TRANS_2PC_PREPARE_RESPONSE))) {
              TRANS_LOG(WARN, "submit 2pc response error", KR(ret), K(*this));
            } else {
              TRANS_LOG(INFO, "submit response for prepare redo due to error status", KR(ret), K(*this));
            }
          } else {
            bool can_alloc_log = false;
            int state = ObTransResultState::INVALID;
            if (ctx_dependency_wrap_.get_prev_trans_arr_count() <= 0) {
              can_alloc_log = true;
            } else if (OB_FAIL(ctx_dependency_wrap_.get_prev_trans_arr_result(state))) {
              TRANS_LOG(WARN, "get prev trans arr result error", K(ret), "context", *this);
            } else if (ObTransResultState::is_commit(state)) {
              // The status of predecessors are commit, so we can go on to alloc the log
              can_alloc_log = true;
              ctx_dependency_wrap_.reset_prev_trans_arr();
            } else if (ObTransResultState::is_abort(state)) {
              can_alloc_log = true;
              set_status_(OB_TRANS_ROLLBACKED);
              ctx_dependency_wrap_.reset_prev_trans_arr();
              need_print_trace_log_ = true;
              TRANS_LOG(WARN, "current transaction need to rollback because of prev abort trans", "context", *this);
            } else {
              // The predecessor transaction status is not decided, and the coordinator cannot response here
              can_alloc_log = false;
              TRANS_LOG(INFO, "prev transaction has not decided, need to retry", "context", *this);
            }
            if (can_alloc_log && OB_SUCC(ret)) {
              if (OB_FAIL(alloc_local_trans_version_(OB_LOG_TRANS_REDO_WITH_PREPARE))) {
                if (OB_EAGAIN != ret) {
                  TRANS_LOG(WARN, "alloc log id and timestamp error", KR(ret), "context", *this);
                } else {
                  ret = OB_SUCCESS;
                  inc_submit_log_pending_count_();
                  inc_submit_log_count_();
                }
              } else if (OB_FAIL(do_prepare_(get_status_()))) {
                TRANS_LOG(WARN, "do prepare error", KR(ret), "context", *this);
              } else if (OB_FAIL(submit_log_async_(OB_LOG_TRANS_REDO, unused))) {
                TRANS_LOG(WARN, "submit redo log error", KR(ret), "context", *this);
              } else {
                // do nothing
              }
            }
          }

          // If the log submission fails, ctx can be released if the transaction
          // does not enter 2pc at this time; otherwise, it cannot be released
          if (OB_SUCCESS != ret && !has_logged_()) {
            const bool commit = false;
            (void)trans_end_(commit, get_global_trans_version_());
            (void)trans_clear_();
            set_exiting_();
            (void)unregister_timeout_task_();
            set_status_(OB_TRANS_ROLLBACKED);
          }
        }
      }
      break;
    }
    case Ob2PCState::PRE_PREPARE: {
      if (!batch_commit_trans_) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "state not match", K(ret), "context", *this);
        need_print_trace_log_ = true;
      } else {
        TRANS_LOG(DEBUG, "still in pre_prepare, unfinish write prepare log", "context", *this);
      }
      break;
    }
    case Ob2PCState::PREPARE: {
      bool can_response = false;
      int state = ObTransResultState::INVALID;
      if (ctx_dependency_wrap_.get_prev_trans_arr_count() <= 0) {
        can_response = true;
      } else if (OB_FAIL(ctx_dependency_wrap_.get_prev_trans_arr_result(state))) {
        TRANS_LOG(WARN, "get prev trans arr result error", K(ret), "context", *this);
      } else if (ObTransResultState::is_commit(state)) {
        can_response = true;
      } else if (ObTransResultState::is_abort(state) || OB_SUCCESS != get_status_()) {
        set_status_(OB_TRANS_ROLLBACKED);
        can_response = true;
        need_print_trace_log_ = true;
        TRANS_LOG(WARN, "current transaction need to rollback because of prev abort trans", "context", *this);
      } else {
        // The predecessor transaction status is not decided, and the coordinator cannot response here
      }
      if (can_response && OB_FAIL(post_2pc_response_(coordinator_, OB_TRANS_2PC_PREPARE_RESPONSE))) {
        TRANS_LOG(WARN, "submit 2pc response error", K(ret), K(*this));
      }
      break;
    }
    case Ob2PCState::COMMIT:
      // For the scenerio that a participant is in the commit phase, and the
      // other participants are in the preare phase, this is possible, So at
      // this time, we need to carry global_trans_version in the reply prepare
      // ok message, otherwise it may create inconsistency: see bug 11495025
      if (global_trans_version_ < state_.get_prepare_version()) {
        TRANS_LOG(ERROR,
            "global trans version is lower than local trans version, unexpected error",
            K_(global_trans_version),
            K_(local_trans_version),
            "context",
            *this);
      } else {
        state_.set_prepare_version(global_trans_version_);
        if (OB_FAIL(post_2pc_response_(coordinator_, OB_TRANS_2PC_PREPARE_RESPONSE))) {
          TRANS_LOG(WARN, "submit 2pc response error", K(ret), K(*this));
        }
      }
      break;
    case Ob2PCState::ABORT:
    // go through
    case Ob2PCState::CLEAR: {
      if (OB_FAIL(post_2pc_response_(coordinator_, OB_TRANS_2PC_PREPARE_RESPONSE))) {
        TRANS_LOG(WARN, "submit 2pc response error", K(ret), K(*this));
      }
      break;
    }
    default: {
      TRANS_LOG(WARN, "invalid 2pc state", K(state), K(*this));
      ret = OB_TRANS_INVALID_STATE;
      break;
    }
  }

  return ret;
}

int ObPartTransCtx::handle_2pc_prepare_request_(const ObTransMsg& msg)
{
  return handle_2pc_prepare_request_raw_(msg.get_status());
}

int ObPartTransCtx::check_and_early_release_lock_()
{
  int ret = OB_SUCCESS;
  bool updated = false;
  ObITsMgr* ts_mgr = get_ts_mgr_();

  if (!can_elr_) {
    // do nothing
  } else if (OB_FAIL(ts_mgr->update_local_trans_version(self_.get_tenant_id(), global_trans_version_, updated))) {
    TRANS_LOG(WARN, "update local trans version failed", K(ret), "context", *this);
  } else if (OB_FAIL(partition_mgr_->acquire_ctx_ref(trans_id_))) {
    TRANS_LOG(WARN, "get trans ctx error", K(ret), "context", *this);
  } else {
    set_elr_prepared_();
    TRANS_LOG(DEBUG, "relocate transaction when early release lock", "context", *this);
  }

  return ret;
}

// If the batch commit fails, the coordinator should rollback previous state and invoke the clean 2pc
int ObPartTransCtx::batch_submit_log_over(const bool submit_log_succ, const int64_t commit_version)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (OB_UNLIKELY(for_replay_)) {
    TRANS_LOG(WARN, "transaction is replaying", "context", *this);
    need_print_trace_log_ = true;
  } else if (submit_log_succ && commit_version <= 0) {
    TRANS_LOG(WARN, "invalid argument", K(commit_version), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else {
    // After the batch_commit_log is completed, regardless of success or failure, you need to modify the status of 2pc
    // to INIT After the execution of the operation is completed, it means that leader transfer and kill operation can
    // be executed If the log submission fails, we need to add the nop log to the clog, and rollback the position of the
    // fill redo log at the same time
    if (submit_log_succ) {
      dec_submit_log_pending_count_();
      if (can_elr_) {
        set_elr_preparing_();
        (void)check_and_early_release_lock_();
      }
      batch_commit_state_ = ObBatchCommitState::INIT;
      update_max_submitted_log_timestamp_(preassigned_log_meta_.get_submit_timestamp());
    } else {
      // If the one-phase commit log fails, the transaction will be recovered to the 2pc afterwards.
      // At this time, the optimization of early lock release cannot be performed in advance!!!
      can_elr_ = false;
      if (bc_has_alloc_log_id_ts_()) {
        if (!preassigned_log_meta_.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "invalid log meta", KR(ret), "context", *this);
        } else {
          state_.set_prepare_version(ObTransVersion::INVALID_TRANS_VERSION);
          if (OB_SUCCESS != (tmp_ret = clog_adapter_->backfill_nop_log(self_, pg_, preassigned_log_meta_))) {
            TRANS_LOG(WARN, "backfill nop log error", K(tmp_ret), "context", *this);
            if (OB_EAGAIN == tmp_ret &&
                OB_SUCCESS != (tmp_ret = clog_adapter_->submit_backfill_nop_log_task(self_, preassigned_log_meta_))) {
              TRANS_LOG(WARN, "submit backfill nop log task fail", K(tmp_ret), "context", *this);
            }
          }
          preassigned_log_meta_.reset();
        }
      }
      if (OB_SUCC(ret) && bc_has_generate_redo_log_()) {
        mt_ctx_.undo_fill_redo_log();
        TRANS_LOG(INFO, "undo fill redo log success", KR(ret), "context", *this);
      }
      if (OB_SUCC(ret) && bc_has_generate_prepare_log_()) {
        if (submit_log_count_ <= 0) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "unexpected submit log count", KR(ret), "context", *this);
        } else {
          dec_submit_log_count_();
          dec_submit_log_pending_count_();
        }
      }
      if (OB_SUCC(ret)) {
        if (Ob2PCState::INIT != get_state_() && Ob2PCState::PRE_PREPARE != get_state_()) {
          ret = OB_STATE_NOT_MATCH;
          TRANS_LOG(ERROR, "no need to batch submit log over", K(ret), K(submit_log_succ), K(*this));
          print_trace_log_();
        } else {
          set_state_(Ob2PCState::INIT);
          batch_commit_state_ = ObBatchCommitState::INIT;
          partition_log_info_arr_.reset();
          batch_commit_trans_ = false;
          global_trans_version_ = ObTransVersion::INVALID_TRANS_VERSION;
        }
      }
    }
  }

  return ret;
}

// Before coordinator generates the prepare version, set the before_prepare for
// all participants to avoid the concurrent problem for lock_for_read
int ObPartTransCtx::before_prepare()
{
  int ret = OB_SUCCESS;
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (OB_UNLIKELY(for_replay_)) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "invalid state, transaction is replaying", KR(ret), "context", *this);
  } else if (!is_prepared_) {
    mt_ctx_.before_prepare();
  } else {
    // do nothing
  }

  return ret;
}

int ObPartTransCtx::handle_2pc_local_prepare_request(const int64_t request_id, const ObAddr& scheduler,
    const ObPartitionKey& coordinator, const ObPartitionArray& participants, const common::ObString& app_trace_info,
    const MonotonicTs stc, const int status, const bool is_xa_prepare)
{
  int ret = OB_SUCCESS;
  UNUSED(is_xa_prepare);
  int64_t msg_type = OB_TRANS_2PC_PRE_PREPARE_REQUEST;
  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(request_id <= 0) || OB_UNLIKELY(!scheduler.is_valid()) ||
             OB_UNLIKELY(!coordinator.is_valid()) || OB_UNLIKELY(participants.count() <= 0)) {
    TRANS_LOG(WARN, "invalid argument", K(request_id), K(scheduler), K(coordinator), K(participants), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (OB_UNLIKELY(for_replay_)) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "invalid state, transaction is replaying", K(ret), "context", *this);
  } else if (OB_UNLIKELY(is_changing_leader_)) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "transaction is preparing changing leader", K(ret), "context", *this);
  } else if (OB_UNLIKELY(is_dup_table_trans_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "duplicate transaction can not batch commit", K(ret), K(*this));
  } else if (OB_FAIL(set_scheduler_(scheduler))) {
    TRANS_LOG(WARN, "set scheduler error", K(ret), K(scheduler), "context", *this);
  } else if (OB_FAIL(set_coordinator_(coordinator))) {
    TRANS_LOG(WARN, "set coordinator error", K(ret), K(coordinator), "context", *this);
  } else if (OB_FAIL(calc_serialize_size_and_set_participants_(participants))) {
    TRANS_LOG(WARN, "set participants error", K(ret), K(participants), "context", *this);
  } else if (Ob2PCState::INIT != get_state_()) {
    ret = OB_EAGAIN;
  } else if (is_logging_() || preassigned_log_meta_.is_valid()) {
    ret = OB_EAGAIN;
  } else if (OB_FAIL(set_app_trace_info_(app_trace_info))) {
    TRANS_LOG(WARN, "set app trace info error", K(ret), K(app_trace_info), K(*this));
  } else {
    if (stc.mts_ > 0) {
      set_stc_(stc);
    } else {
      set_stc_by_now_();
    }
    enable_new_1pc_ = true;
    request_id_ = request_id;
    ret = handle_2pc_prepare_request_raw_(status);
    TRANS_LOG(DEBUG, "handle 2cp local prepare request succ", K(request_id), K(enable_new_1pc_), K(*this));
  }
  REC_TRANS_TRACE_EXT(tlog_, handle_message, OB_ID(ret), ret, OB_ID(msg_type), msg_type, OB_ID(uref), get_uref());

  return ret;
}

int ObPartTransCtx::handle_2pc_local_commit_request(
    const int64_t msg_type, const int64_t trans_version, const PartitionLogInfoArray& partition_log_info_arr)
{
  CtxLockGuard guard(lock_);
  return handle_2pc_commit_request_raw_(msg_type, trans_version, partition_log_info_arr);
}

int ObPartTransCtx::handle_2pc_local_clear_request()
{
  CtxLockGuard guard(lock_);
  return handle_2pc_clear_request_raw_(OB_TRANS_2PC_CLEAR_REQUEST);
}

int ObPartTransCtx::get_prepare_ack_arg_(int& status, int64_t& state, int64_t& prepare_version,
    uint64_t& prepare_log_id, int64_t& prepare_log_ts, int64_t& request_id, int64_t& remain_wait_interval_us,
    bool& is_xa_prepare)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  status = get_status_();
  state = state_.get_state();
  prepare_version = state_.get_prepare_version();
  prepare_log_id = prepare_log_id_;
  prepare_log_ts = prepare_log_timestamp_;
  request_id = request_id_;
  remain_wait_interval_us = get_remaining_wait_interval_us_();
  is_xa_prepare = in_xa_prepare_state_;
  return ret;
}

int ObPartTransCtx::handle_2pc_local_msg_response(
    const ObPartitionKey& partition, const ObTransID& trans_id, const int64_t log_type)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_LOG_TRANS_PREPARE == log_type || OB_LOG_TRANS_REDO_WITH_PREPARE == log_type) {
    int status = OB_SUCCESS;
    int64_t state = Ob2PCState::UNKNOWN;
    int64_t prepare_version = 0;
    uint64_t prepare_log_id = 0;
    int64_t prepare_log_ts = 0;
    int64_t request_id = 0;
    int64_t remain_wait_interval_us = 0;
    bool is_xa_prepare = false;

    if (OB_ISNULL(coord_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected coord ctx", K(ret), K(partition), K(trans_id), K(log_type));
    } else if (OB_FAIL(get_prepare_ack_arg_(status,
                   state,
                   prepare_version,
                   prepare_log_id,
                   prepare_log_ts,
                   request_id,
                   remain_wait_interval_us,
                   is_xa_prepare))) {
      TRANS_LOG(WARN, "get prepare ack argument error", K(ret), K(partition), K(trans_id));
    } else if (OB_FAIL(static_cast<ObCoordTransCtx*>(coord_ctx_)
                           ->handle_2pc_local_prepare_response(OB_TRANS_2PC_PREPARE_RESPONSE,
                               partition,
                               status,
                               state,
                               prepare_version,
                               PartitionLogInfoArray(),
                               prepare_log_id,
                               prepare_log_ts,
                               -1,
                               request_id,
                               remain_wait_interval_us,
                               ObTransSplitInfo(),
                               is_xa_prepare))) {
      if (OB_EAGAIN != ret) {
        TRANS_LOG(WARN, "handle 2pc local prepare response error", K(ret), K(partition), K(trans_id), K(log_type));
      }
      // The local invokation fails, we retry it using message
      {
        CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);
        if (OB_SUCCESS != (tmp_ret = post_2pc_response_(coordinator_, OB_TRANS_2PC_PREPARE_RESPONSE))) {
          TRANS_LOG(WARN, "submit 2pc response error", "ret", tmp_ret, "context", *this);
        }
      }
    }
  } else if (OB_LOG_TRANS_COMMIT == log_type) {
    bool can_post_msg = true;
    // For scenes where the master cut occurs, coord_ctx may be NULL
    if (NULL != coord_ctx_) {
      if (OB_FAIL(
              static_cast<ObCoordTransCtx*>(coord_ctx_)
                  ->handle_2pc_local_commit_response(partition, ObPartitionArray(), OB_TRANS_2PC_COMMIT_RESPONSE))) {
        if (OB_EAGAIN != ret) {
          TRANS_LOG(WARN, "handle 2pc local commit response error", K(ret), K(partition), K(trans_id), K(log_type));
        }
      } else {
        can_post_msg = false;
      }
    }
    if (can_post_msg) {
      // The local invokation fails, we retry it using message
      CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);
      if (OB_SUCCESS != (tmp_ret = post_2pc_response_(coordinator_, OB_TRANS_2PC_COMMIT_RESPONSE))) {
        TRANS_LOG(WARN, "submit 2pc commit response error", "ret", tmp_ret, "context", *this);
      }
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObPartTransCtx::handle_2pc_pre_prepare_request(const int64_t prepare_version, const int64_t request_id,
    const ObAddr& scheduler, const ObPartitionKey& coordinator, const ObPartitionArray& participants,
    const common::ObString& app_trace_info, int& status, int64_t& prepare_log_id, int64_t& prepare_log_ts,
    bool& have_prev_trans)
{
  int ret = OB_SUCCESS;
  int64_t msg_type = OB_TRANS_2PC_PRE_PREPARE_REQUEST;
  int64_t tmp_gts = 0;
  ObITsMgr* ts_mgr = NULL;
  bool is_split_partition = false;
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(prepare_version <= 0) || OB_UNLIKELY(request_id <= 0) || OB_UNLIKELY(!scheduler.is_valid()) ||
             OB_UNLIKELY(!coordinator.is_valid()) || OB_UNLIKELY(participants.count() <= 0)) {
    TRANS_LOG(WARN,
        "invalid argument",
        K(prepare_version),
        K(request_id),
        K(scheduler),
        K(coordinator),
        K(participants),
        "context",
        *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (OB_UNLIKELY(for_replay_)) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "invalid state, transaction is replaying", KR(ret), "context", *this);
  } else if (OB_UNLIKELY(is_changing_leader_)) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "transaction is preparing changing leader", KR(ret), "context", *this);
  } else if (OB_UNLIKELY(is_dup_table_trans_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "duplicate transaction can not batch commit", K(ret), K(*this));
  } else if (OB_FAIL(set_scheduler_(scheduler))) {
    TRANS_LOG(WARN, "set scheduler error", KR(ret), K(scheduler), "context", *this);
  } else if (OB_FAIL(set_coordinator_(coordinator))) {
    TRANS_LOG(WARN, "set coordinator error", KR(ret), K(coordinator), "context", *this);
  } else if (OB_FAIL(calc_serialize_size_and_set_participants_(participants))) {
    TRANS_LOG(WARN, "set participants error", KR(ret), K(participants), "context", *this);
  } else if (Ob2PCState::INIT != get_state_()) {
    ret = OB_EAGAIN;
    TRANS_LOG(WARN, "transaction is not init", KR(ret), K(*this));
  } else if (is_logging_() || preassigned_log_meta_.is_valid()) {
    ret = OB_EAGAIN;
    TRANS_LOG(WARN, "transaction is logging", KR(ret), K(*this));
  } else if (OB_ISNULL(ts_mgr = get_ts_mgr_())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "ts mgr is null", KR(ret));
  } else if (OB_FAIL(set_app_trace_info_(app_trace_info))) {
    TRANS_LOG(WARN, "set app trace info error", K(ret), K(app_trace_info), K(*this));
  } else if (OB_FAIL(check_cur_partition_split_(is_split_partition))) {
    TRANS_LOG(WARN, "check current partition split error", K(ret), K(*this));
  } else if (is_split_partition) {
    ret = OB_EAGAIN;
    TRANS_LOG(WARN, "current partition is split partition, can not execute 1pc trx", K(ret), K(*this));
  } else {
    if (!is_prepared_) {
      mt_ctx_.before_prepare();
    }
    if (OB_FAIL(ts_mgr->get_local_trans_version(self_.get_tenant_id(), NULL, tmp_gts))) {
      TRANS_LOG(WARN, "participant get gts cache failed", KR(ret), K_(tenant_id), "context", *this);
    } else {
      // We need obtain the gts_cache again, otherwise the prepare_version may
      // be lower than expect
      local_trans_version_ = ((tmp_gts > prepare_version) ? tmp_gts : prepare_version);
      request_id_ = request_id;
      set_state_(Ob2PCState::PRE_PREPARE);
      batch_commit_trans_ = true;
      if (OB_FAIL(generate_log_id_timestamp_(OB_LOG_TRANS_REDO_WITH_PREPARE))) {
        if (OB_EAGAIN != ret) {
          TRANS_LOG(WARN, "generate log id and timestamp error", KR(ret), K(prepare_version), "context", *this);
        }
      } else if (OB_SUCCESS == get_status_() && (!preassigned_log_meta_.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unexpected cur log id or timestamp", KR(ret), "context", *this);
        need_print_trace_log_ = true;
      } else {
        status = get_status_();
        prepare_log_id = preassigned_log_meta_.get_log_id();
        prepare_log_ts = preassigned_log_meta_.get_submit_timestamp();
        batch_commit_state_ = ObBatchCommitState::ALLOC_LOG_ID_TS;
        have_prev_trans = (ctx_dependency_wrap_.get_prev_trans_arr_count() > 0 ? true : false);
      }
    }
  }
  REC_TRANS_TRACE_EXT(tlog_, handle_message, OB_ID(ret), ret, OB_ID(msg_type), msg_type, OB_ID(uref), get_uref());

  return ret;
}

int ObPartTransCtx::check_cur_partition_split_(bool& is_split_partition)
{
  int ret = OB_SUCCESS;
  storage::ObIPartitionGroupGuard pkey_guard;

  if (OB_NOT_NULL(pg_)) {
    if (OB_FAIL(pg_->check_cur_partition_split(is_split_partition))) {
      TRANS_LOG(WARN, "check current partition split error", K(is_split_partition), "context", *this);
    }
  } else if (OB_FAIL(partition_service_->get_partition(self_, pkey_guard))) {
    TRANS_LOG(WARN, "get partition error", KR(ret), "context", *this);
  } else if (NULL == pkey_guard.get_partition_group()) {
    TRANS_LOG(ERROR, "partition is null, unexpected error", KP(pkey_guard.get_partition_group()), "context", *this);
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(pkey_guard.get_partition_group()->check_cur_partition_split(is_split_partition))) {
    TRANS_LOG(WARN, "check current partition split error", K(is_split_partition), "context", *this);
  } else {
    // do nothing
  }
  return ret;
}

int ObPartTransCtx::handle_2pc_commit_request_raw_(
    const int64_t msg_type, const int64_t trans_version, const PartitionLogInfoArray& partition_log_info_arr)
{
  int ret = OB_SUCCESS;
  const int64_t state = get_state_();
  if (OB_UNLIKELY(OB_SUCCESS != get_status_())) {
    protocol_error(state, msg_type);
    ret = OB_TRANS_PROTOCOL_ERROR;
  } else {
    switch (state) {
      case Ob2PCState::INIT:
      case Ob2PCState::PRE_PREPARE: {
        protocol_error(state, msg_type);
        ret = OB_TRANS_PROTOCOL_ERROR;
        TRANS_LOG(ERROR, "state not match", K(ret), "context", *this);
        need_print_trace_log_ = true;
        break;
      }
      case Ob2PCState::PREPARE: {
        bool unused = false;
        if (is_logging_()) {
          // it is logging now. just discard message
          TRANS_LOG(DEBUG, "participant is logging now, discard message", "context", *this);
        } else if (OB_FAIL(do_dist_commit_(trans_version, &partition_log_info_arr))) {
          TRANS_LOG(WARN, "do commit error", KR(ret), "context", *this);
        } else {
          if (OB_FAIL(submit_log_async_(OB_LOG_TRANS_COMMIT, unused))) {
            TRANS_LOG(WARN, "submit commit log error", KR(ret), "context", *this);
          }
          //_FILL_TRACE_BUF(tlog_, "submit commit log, ret=%d", ret);
        }
        break;
      }
      case Ob2PCState::COMMIT: {
        if (OB_FAIL(try_respond_coordinator_(OB_TRANS_2PC_COMMIT_RESPONSE, LISTENER_COMMIT))) {
          TRANS_LOG(WARN, "try respond coordinator failed", KR(ret), K(*this));
        }
        break;
      }
      case Ob2PCState::ABORT: {
        protocol_error(state, msg_type);
        ret = OB_TRANS_PROTOCOL_ERROR;
        break;
      }
      case Ob2PCState::CLEAR: {
        if (OB_FAIL(post_2pc_response_(coordinator_, OB_TRANS_2PC_COMMIT_RESPONSE))) {
          TRANS_LOG(WARN, "submit 2pc response error", KR(ret), "context", *this);
        }
        break;
      }
      default: {
        TRANS_LOG(WARN, "invalid 2pc state", K(state));
        ret = OB_TRANS_INVALID_STATE;
        break;
      }
    }
  }

  return ret;
}

int ObPartTransCtx::handle_2pc_commit_request_(const ObTransMsg& msg)
{
  return handle_2pc_commit_request_raw_(msg.get_msg_type(), msg.get_trans_version(), msg.get_partition_log_info_arr());
}

int ObPartTransCtx::handle_2pc_commit_clear_request(const int64_t commit_version)
{
  int ret = OB_SUCCESS;
  int64_t msg_type = OB_TRANS_2PC_COMMIT_CLEAR_REQUEST;
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(commit_version <= 0)) {
    TRANS_LOG(WARN, "invalid message", K(commit_version), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this, K(commit_version));
    ret = OB_TRANS_IS_EXITING;
  } else if (OB_UNLIKELY(for_replay_)) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "invalid state, transaction is replaying", KR(ret), "context", *this);
  } else {
    ret = handle_2pc_commit_clear_request_(commit_version, false);
  }
  REC_TRANS_TRACE_EXT(tlog_,
      handle_message,
      OB_ID(ret),
      ret,
      OB_ID(msg_type),
      msg_type,
      OB_ID(request_id),
      request_id_,
      OB_ID(uref),
      get_uref());

  return ret;
}

int ObPartTransCtx::handle_2pc_commit_clear_request_(const int64_t commit_version, const bool need_response)
{
  int ret = OB_SUCCESS;
  const int64_t state = get_state_();

  if (OB_UNLIKELY(OB_SUCCESS != get_status_())) {
    protocol_error(state, OB_TRANS_2PC_COMMIT_CLEAR_REQUEST);
    ret = OB_TRANS_PROTOCOL_ERROR;
  } else {
    switch (state) {
      case Ob2PCState::INIT: {
        protocol_error(state, OB_TRANS_2PC_COMMIT_CLEAR_REQUEST);
        ret = OB_TRANS_PROTOCOL_ERROR;
        TRANS_LOG(ERROR, "discard this msg", K(commit_version), "context", *this);
        break;
      }
      case Ob2PCState::PRE_PREPARE: {
        if (need_response) {
          protocol_error(state, OB_TRANS_2PC_COMMIT_CLEAR_REQUEST);
          ret = OB_TRANS_PROTOCOL_ERROR;
          TRANS_LOG(ERROR, "discard this msg", K(commit_version), "context", *this);
        }
        break;
      }
      case Ob2PCState::PREPARE: {
        if (!batch_commit_trans_ || is_pre_preparing_()) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(
              ERROR, "unexpected batch commit transaction status", KR(ret), K_(batch_commit_trans), "context", *this);
          need_print_trace_log_ = true;
        } else if (OB_FAIL(do_dist_commit_(commit_version, NULL))) {
          TRANS_LOG(WARN, "do commit error", KR(ret), "context", *this);
        } else if (OB_FAIL(on_dist_commit_())) {
          TRANS_LOG(WARN, "on commit error", KR(ret), "context", *this);
        } else if (OB_FAIL(do_clear_())) {
          TRANS_LOG(WARN, "do clear error", KR(ret), "context", *this);
        } else if (OB_FAIL(on_clear_(need_response))) {
          TRANS_LOG(WARN, "on clear error", KR(ret), "context", *this);
        } else {
          end_log_ts_ = end_log_ts_for_batch_commit_;
          // do nothing
        }
        break;
      }
      case Ob2PCState::COMMIT:
      // go through
      case Ob2PCState::ABORT:
      // go through
      case Ob2PCState::CLEAR: {
        protocol_error(state, OB_TRANS_2PC_COMMIT_CLEAR_REQUEST);
        ret = OB_TRANS_PROTOCOL_ERROR;
        break;
      }
      default: {
        TRANS_LOG(WARN, "invalid 2pc state", K(state), K(commit_version), "context", *this);
        ret = OB_TRANS_INVALID_STATE;
        break;
      }
    }
  }

  return ret;
}

int ObPartTransCtx::handle_2pc_abort_request_raw_(const int64_t msg_type, const int64_t msg_status)
{
  int ret = OB_SUCCESS;

  const int64_t state = get_state_();
  // If the txn state of participant is different from coordinator's, we need
  // update the local state
  if (get_status_() != msg_status) {
    set_status_(msg_status);
  }
  switch (state) {
    case Ob2PCState::INIT:
    // go through
    case Ob2PCState::PRE_PREPARE: {
      protocol_error(state, msg_type);
      ret = OB_TRANS_PROTOCOL_ERROR;
      break;
    }
    case Ob2PCState::PREPARE: {
      bool unused = false;
      if (is_logging_()) {
        // it is logging now. just discard message
        TRANS_LOG(DEBUG, "participant is logging now, discard message", "context", *this);
      } else {
        if (OB_FAIL(submit_log_async_(OB_LOG_TRANS_ABORT, unused))) {
          TRANS_LOG(WARN, "submit abort log error", KR(ret), "context", *this);
        }
        //_FILL_TRACE_BUF(tlog_, "submit abort log, ret=%d", ret);
      }
      break;
    }
    case Ob2PCState::COMMIT: {
      protocol_error(state, msg_type);
      ret = OB_TRANS_PROTOCOL_ERROR;
      break;
    }
    case Ob2PCState::ABORT: {
      if (OB_FAIL(try_respond_coordinator_(OB_TRANS_2PC_ABORT_RESPONSE, LISTENER_ABORT))) {
        TRANS_LOG(WARN, "try respond coordinator failed", KR(ret), K(*this));
      }
      break;
    }
    case Ob2PCState::CLEAR: {
      if (OB_FAIL(post_2pc_response_(coordinator_, OB_TRANS_2PC_ABORT_RESPONSE))) {
        TRANS_LOG(WARN, "submit 2pc response error", KR(ret), "context", *this);
      }
      break;
    }
    default: {
      TRANS_LOG(WARN, "invalid 2pc state", K(state), K(msg_status));
      ret = OB_TRANS_INVALID_STATE;
      break;
    }
  }

  return ret;
}

int ObPartTransCtx::handle_2pc_abort_request_(const ObTransMsg& msg)
{
  return handle_2pc_abort_request_raw_(msg.get_msg_type(), msg.get_status());
}

#ifdef ERRSIM
#define INJECT_2PC_PREPARE_RETRY_ERRSIM                                                                              \
  do {                                                                                                               \
    int64_t participants_serialize_size = participants_.get_serialize_size();                                        \
    if (participants_serialize_size > 4096 && OB_FAIL(E(EventTable::EN_HANDLE_PREPARE_MESSAGE_EAGAIN) OB_SUCCESS)) { \
      ret = OB_EAGAIN;                                                                                               \
      TRANS_LOG(INFO,                                                                                                \
          "ERRSIM Inject OB_EAGAIN error when handle 2pc prepare request",                                           \
          KR(ret),                                                                                                   \
          K(participants_serialize_size));                                                                           \
    }                                                                                                                \
  } while (false);
#else
#define INJECT_2PC_PREPARE_RETRY_ERRSIM
#endif

int ObPartTransCtx::handle_2pc_request(const ObTrxMsgBase &msg,
                                       const int64_t msg_type)
{
  int ret = OB_SUCCESS;

#ifdef ERRSIM
  (void)DEBUG_SYNC_slow_txn_before_handle_message_(msg_type);
#endif

  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);
  ObTrxMsgBase* message = const_cast<ObTrxMsgBase*>(&msg);
  same_leader_batch_partitions_count_ = 0;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!msg.is_valid())) {
    TRANS_LOG(WARN, "invalid message", "context", *this, K(msg));
    ret = OB_TRANS_INVALID_MESSAGE;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this, K(msg));
    ret = OB_TRANS_IS_EXITING;
  } else if (OB_UNLIKELY(for_replay_)) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "invalid state, transaction is replaying", K(ret), "context", *this);
  } else if (OB_TRX_2PC_CLEAR_REQUEST == msg_type) {
    ObTrx2PCClearRequest* req = static_cast<ObTrx2PCClearRequest*>(message);
    request_id_ = req->request_id_;
    same_leader_batch_partitions_count_ = req->batch_same_leader_partitions_.count();
    update_clear_log_base_ts_(req->clear_log_base_ts_);
    if (is_readonly_) {
      if (OB_FAIL(set_scheduler_(req->scheduler_))) {
        TRANS_LOG(WARN, "set scheduler error", K(ret), K(*req));
      } else if (OB_FAIL(set_coordinator_(req->coordinator_))) {
        TRANS_LOG(WARN, "set coordinator error", K(ret), K(*req));
      } else if (OB_FAIL(calc_serialize_size_and_set_participants_(req->participants_))) {
        TRANS_LOG(WARN, "set participants error", K(ret), K(*req));
      } else {
        // do nothing
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(handle_2pc_clear_request_raw_(msg_type))) {
        TRANS_LOG(WARN, "handle request failed", K(ret), K(msg_type), K(*this));
      } else {
        TRANS_LOG(DEBUG, "handle request succ", K(msg_type), K(*this));
      }
    }
  } else if (OB_TRX_2PC_ABORT_REQUEST == msg_type) {
    ObTrx2PCAbortRequest* req = static_cast<ObTrx2PCAbortRequest*>(message);
    request_id_ = req->request_id_;
    same_leader_batch_partitions_count_ = req->batch_same_leader_partitions_.count();
    if (OB_FAIL(handle_2pc_abort_request_raw_(msg_type, req->status_))) {
      TRANS_LOG(WARN, "handle request failed", K(ret), K(msg_type), K(*this), K(*req));
    } else {
      TRANS_LOG(DEBUG, "handle request succ", K(msg_type), K(*this));
    }
  } else if (OB_TRX_2PC_COMMIT_REQUEST == msg_type) {
    ObTrx2PCCommitRequest* req = static_cast<ObTrx2PCCommitRequest*>(message);
    request_id_ = req->request_id_;
    same_leader_batch_partitions_count_ = req->batch_same_leader_partitions_.count();
    if (OB_FAIL(handle_2pc_commit_request_raw_(msg_type, req->trans_version_, req->partition_log_info_arr_))) {
      TRANS_LOG(WARN, "handle request failed", K(ret), K(msg_type), K(*this), K(*req));
    } else {
      TRANS_LOG(DEBUG, "handle request succ", K(msg_type), K(*this));
    }
  } else if (OB_TRX_2PC_PRE_COMMIT_REQUEST == msg_type) {
    ObTrx2PCPreCommitRequest* req = static_cast<ObTrx2PCPreCommitRequest*>(message);
    request_id_ = req->request_id_;
    same_leader_batch_partitions_count_ = req->batch_same_leader_partitions_.count();
    if (OB_FAIL(handle_2pc_pre_commit_request_(req->trans_version_, true))) {
      TRANS_LOG(WARN, "handle request failed", K(ret), K(msg_type), K(*this), K(*req));
    } else {
      TRANS_LOG(DEBUG, "handle request succ", K(msg_type), K(*this));
    }
  } else if (OB_TRX_2PC_PREPARE_REQUEST == msg_type) {
    ObTrx2PCPrepareRequest* req = static_cast<ObTrx2PCPrepareRequest*>(message);
    request_id_ = req->request_id_;
    if (req->sender_addr_ == addr_ && req->stc_.is_valid()) {
      set_stc_(req->stc_);
    } else {
      set_stc_by_now_();
    }
    // TODO do not set scheduler/coordinator/participants if state is init
    // scheduler has already be initialized
    // if (OB_FAIL(set_scheduler_(req->scheduler_))) {
    //  TRANS_LOG(WARN, "set scheduler error", K(ret), K(*req));
    if (OB_FAIL(set_coordinator_(req->coordinator_))) {
      TRANS_LOG(WARN, "set coordinator error", K(ret), K(*req));
    } else if (OB_FAIL(calc_serialize_size_and_set_participants_(req->participants_))) {
      TRANS_LOG(WARN, "set participants error", K(ret), K(*req));
    } else if (OB_FAIL(set_xid_(req->xid_))) {
      TRANS_LOG(WARN, "set xid error", K(ret), K(*this), K(*req));
    } else if (is_xa_local_trans() && req->is_xa_prepare_) {
      if (OB_FAIL(handle_2pc_prepare_redo_request_raw_(req->status_))) {
        TRANS_LOG(WARN, "handle 2pc xa preprare request error", K(ret), K(*this), K(*req));
      }
    } else {
      if (OB_FAIL(handle_2pc_prepare_request_raw_(req->status_))) {
        TRANS_LOG(WARN, "handle 2pc preprare request error", K(ret), K(*this), K(*req));
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t tmp_config = ObServerConfig::get_instance().trx_2pc_retry_interval;
      if (OB_FAIL(unregister_timeout_task_())) {
        TRANS_LOG(WARN, "unregister timeout handler error", K(ret), K(*this));
      } else if (OB_FAIL(register_timeout_task_(std::min(tmp_config, (int64_t)MAX_TRANS_2PC_TIMEOUT_US)))) {
        TRANS_LOG(WARN, "register timeout handler error", K(ret), K(*this));
      } else {
        // do nothing
      }
    }
    INJECT_2PC_PREPARE_RETRY_ERRSIM
  } else {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected msg", K(ret), K(msg_type), K(*this));
  }

  REC_TRANS_TRACE_EXT(tlog_, handle_message, OB_ID(ret), ret, OB_ID(msg_type), msg_type, OB_ID(uref), get_uref());
  return ret;
}

int ObPartTransCtx::handle_2pc_clear_request_raw_(const int64_t msg_type)
{
  int ret = OB_SUCCESS;
  const int64_t state = get_state_();

  switch (state) {
    case Ob2PCState::INIT:
    case Ob2PCState::PRE_PREPARE: {
      if (is_readonly_) {
        if (OB_FAIL(do_clear_())) {
          TRANS_LOG(WARN, "do clear error", KR(ret), "context", *this);
        } else if (OB_FAIL(post_2pc_response_(coordinator_, OB_TRANS_2PC_CLEAR_RESPONSE))) {
          TRANS_LOG(WARN, "submit 2pc response error", KR(ret), "context", *this);
        } else {
          // do nothing
        }
        break;
      }
    }
    case Ob2PCState::PREPARE: {
      if (!batch_commit_trans_) {
        protocol_error(state, msg_type);
        ret = OB_TRANS_PROTOCOL_ERROR;
      } else {
        if (EXECUTE_COUNT_PER_SEC(1)) {
          TRANS_LOG(INFO, "batch commit trans need to wait", K(ret), "context", *this);
        }
      }
      break;
    }
    case Ob2PCState::COMMIT:
    // go through
    case Ob2PCState::ABORT: {
      bool unused = false;
      if (is_logging_()) {
        // it is logging now. just discard message
        TRANS_LOG(DEBUG, "participant is logging now, discard message", "context", *this);
      } else if (OB_FAIL(do_clear_())) {
        TRANS_LOG(WARN, "do clear error", KR(ret), "context", *this);
      } else {
        if (OB_NOT_NULL(listener_handler_)) {
          if (!listener_handler_->is_in_clear()) {
            listener_handler_->set_commit_log_synced(false);
            listener_handler_->clear_mask();
            listener_handler_->set_in_clear(true);
          }
          if (OB_FAIL(listener_handler_->notify_listener(ListenerAction::LISTENER_CLEAR))) {
            TRANS_LOG(WARN, "notify listener clear failed", KR(ret));
          }
          if (!listener_handler_->is_commit_log_synced() && OB_FAIL(submit_log_async_(OB_LOG_TRANS_CLEAR, unused))) {
            TRANS_LOG(WARN, "submit clear log error", KR(ret), "context", *this);
          }
        } else if (OB_FAIL(submit_log_async_(OB_LOG_TRANS_CLEAR, unused))) {
          TRANS_LOG(WARN, "submit clear log error", KR(ret), "context", *this);
        }
        //_FILL_TRACE_BUF(tlog_, "submit clear log, ret=%d", ret);
      }
      break;
    }
    case Ob2PCState::CLEAR: {
      if (OB_FAIL(post_2pc_response_(coordinator_, OB_TRANS_2PC_CLEAR_RESPONSE))) {
        TRANS_LOG(WARN, "submit 2pc response error", KR(ret), "context", *this);
      }
      break;
    }
    default: {
      TRANS_LOG(WARN, "invalid 2pc state", K(state));
      ret = OB_TRANS_INVALID_STATE;
      break;
    }
  }

  return ret;
}

int ObPartTransCtx::handle_2pc_clear_request_(const ObTransMsg& msg)
{
  return handle_2pc_clear_request_raw_(msg.get_msg_type());
}

int ObPartTransCtx::handle_2pc_pre_commit_request(const int64_t commit_version)
{
  int ret = OB_SUCCESS;
  int64_t msg_type = OB_TRANS_2PC_PRE_COMMIT_REQUEST;
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(commit_version <= 0)) {
    TRANS_LOG(WARN, "invalid argument", K(commit_version), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this, K(commit_version));
    ret = OB_TRANS_IS_EXITING;
  } else if (OB_UNLIKELY(for_replay_)) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "invalid state, transaction is replaying", KR(ret), "context", *this);
  } else {
    ret = handle_2pc_pre_commit_request_(commit_version, false);
  }
  REC_TRANS_TRACE_EXT(tlog_,
      handle_message,
      OB_ID(ret),
      ret,
      OB_ID(msg_type),
      msg_type,
      OB_ID(request_id),
      request_id_,
      OB_ID(uref),
      get_uref());

  return ret;
}

int ObPartTransCtx::handle_2pc_pre_commit_request_(const int64_t commit_version, const bool need_response)
{
  int ret = OB_SUCCESS;
  ObITsMgr* ts_mgr = get_ts_mgr_();
  const int64_t state = get_state_();

  if (OB_ISNULL(ts_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "ts mgr is null", KR(ret));
  } else if (OB_UNLIKELY(OB_SUCCESS != get_status_())) {
    protocol_error(state, OB_TRANS_2PC_PRE_COMMIT_REQUEST);
  } else {
    switch (state) {
      case Ob2PCState::INIT:
      // go through
      case Ob2PCState::PRE_PREPARE:
      // go through
      case Ob2PCState::CLEAR: {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "discard this msg", KR(ret), K(commit_version), K(need_response), "context", *this);
        break;
      }
      case Ob2PCState::PREPARE: {
        // Push up the public version and record the commit_version to context.
        // And finally respond with pre-commit ok
        if (commit_version <= 0) {
          ret = OB_INVALID_ARGUMENT;
          TRANS_LOG(WARN, "trans version invalid", KR(ret), K(commit_version));
        } else if (coordinator_.get_tenant_id() == self_.get_tenant_id() ||
                   !ts_mgr->is_external_consistent(self_.get_tenant_id())) {
          if (OB_FAIL(update_publish_version_(commit_version, false))) {
            TRANS_LOG(WARN, "update publish version", KR(ret), K(commit_version));
          } else if (need_response && OB_FAIL(post_2pc_response_(coordinator_, OB_TRANS_2PC_PRE_COMMIT_RESPONSE))) {
            TRANS_LOG(WARN, "post pre-commit response fail", KR(ret), "context", *this);
          } else {
            // do nothing
          }
        } else {
          TRANS_LOG(WARN, "this is multi tenant transaction", K(*this));
          int64_t gts;
          if (OB_FAIL(ts_mgr->get_gts(self_.get_tenant_id(), NULL, gts))) {
            if (OB_EAGAIN != ret) {
              TRANS_LOG(WARN, "get gts error", KR(ret), K(*this));
            } else {
              ret = OB_SUCCESS;
            }
          } else if (gts < commit_version) {
            // We need wait the gts cache until the version is pushed over the
            // commit version, and let coordinator retry
          } else if (OB_FAIL(update_publish_version_(commit_version, false))) {
            TRANS_LOG(WARN, "update publish version", KR(ret), K(commit_version));
          } else if (need_response && OB_FAIL(post_2pc_response_(coordinator_, OB_TRANS_2PC_PRE_COMMIT_RESPONSE))) {
            TRANS_LOG(WARN, "post pre-commit response fail", KR(ret), "context", *this);
          } else {
            // do nothing
          }
        }
        break;
      }
      case Ob2PCState::COMMIT: {
        if (commit_version != global_trans_version_) {
          // the commit version should not be changed
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "unexpected trans version", KR(ret), K(commit_version), "context", *this);
          need_print_trace_log_ = true;
        } else if (need_response && OB_FAIL(post_2pc_response_(coordinator_, OB_TRANS_2PC_PRE_COMMIT_RESPONSE))) {
          TRANS_LOG(WARN, "post 2pc response fail", KR(ret), "context", *this);
        }
        break;
      }
      case Ob2PCState::ABORT: {
        protocol_error(state, OB_TRANS_2PC_PRE_COMMIT_REQUEST);
        ret = OB_TRANS_PROTOCOL_ERROR;
        break;
      }
      default: {
        TRANS_LOG(WARN, "invalid 2pc state", K(state), K(commit_version), "context", *this);
        ret = OB_TRANS_INVALID_STATE;
        break;
      }
    }
  }
  return ret;
}

int ObPartTransCtx::do_prepare_(const int status)
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS == get_status_() && OB_SUCCESS != status) {
    set_status_(status);
  }
  REC_TRANS_TRACE_EXT(
      tlog_, prepare, Y(ret), OB_ID(trans_version), state_.get_prepare_version(), OB_ID(uref), get_uref());

  return ret;
}

int ObPartTransCtx::prepare_sp_redolog_id_ts_(const int64_t log_type, bool& need_retry_alloc_id_ts)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(alloc_local_trans_version_(log_type))) {
    // Rollback some already filled data
    mt_ctx_.undo_fill_redo_log();
    if (OB_EAGAIN != ret) {
      TRANS_LOG(WARN, "alloc log id and timestamp error", KR(ret), "context", *this);
    } else {
      ret = OB_SUCCESS;
      need_retry_alloc_id_ts = true;
      inc_submit_log_pending_count_();
      inc_submit_log_count_();
    }
  }

  return ret;
}

int ObPartTransCtx::handle_trans_ask_scheduler_status_response_(const ObTransMsg& msg)
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
  } else if (!can_be_recycled_()) {
    // do nothing
  } else if (OB_UNLIKELY(scheduler_ != msg.get_scheduler())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected scheduler", KR(ret), K(msg));
  } else if (OB_TRANS_CTX_NOT_EXIST == status || OB_PARTITION_NOT_EXIST == status) {
    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      TRANS_LOG(WARN, "[TRANS GC] scheduler is not exist", K(msg), K(status));
    }
    if (OB_FAIL(force_kill_())) {
      TRANS_LOG(WARN, "force kill part_ctx error", KR(ret), "context", *this);
    }
  } else {
    TRANS_LOG(DEBUG, "scheduler is OB_SUCCESS", K(msg), K(status));
  }

  return ret;
}

// When the participant receives the commit request, we judege whether it can be
// unlocked according to the following logic:
// 1. We can directly unlock for 1pc
// 2. we cannot unlock for distributed txn:
//
//   - If we unlock before the commit log is synced to disk, the mutator of same
//     row from other txn can be executed
//   - Then for follower, the same row may exist two uncommitted txns' mutator
//   - For the sake of storage structure and minor merge, the same row must not
//     exist two uncommitted txns' mutator when replaying to a continuout log
//     flow
int ObPartTransCtx::do_dist_commit_(const int64_t trans_version, const PartitionLogInfoArray* partition_log_info_arr)
{
  int ret = OB_SUCCESS;
  // The logic of early release lock is only suitable for no sp trans
  if (OB_UNLIKELY(is_sp_trans_())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "sp trans can't early release lock", KR(ret), "context", *this);
    need_print_trace_log_ = true;
  } else if (OB_UNLIKELY(trans_version <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "do commit trans version invalid", KR(ret), K(trans_version), "context", *this);
    need_print_trace_log_ = true;
    // Need push up global_trans_version and public_version
  } else if (OB_FAIL(update_publish_version_(trans_version, false))) {
    TRANS_LOG(WARN, "update publish version fail", KR(ret), K(trans_version), "context", *this);
  } else {
    if (coordinator_ == self_) {
      ObTransStatus trans_status(trans_id_, OB_SUCCESS);
      (void)trans_status_mgr_->set_status(trans_id_, trans_status);
    }
    if (batch_commit_trans_) {
      const bool commit = true;
      if (OB_FAIL(trans_end_(commit, global_trans_version_))) {
        TRANS_LOG(WARN, "transaciton end error", KR(ret), "context", *this);
      }
    } else {
      if (OB_ISNULL(partition_log_info_arr)) {
        ret = OB_INVALID_ARGUMENT;
        TRANS_LOG(WARN, "partition log info arr is NULL", KR(ret));
      } else if (OB_FAIL(partition_log_info_arr_.assign(*partition_log_info_arr))) {
        TRANS_LOG(
            WARN, "assign partition logid array error", KR(ret), "partition_log_info_arr", *partition_log_info_arr);
      } else {
        commit_log_checksum_ = mt_ctx_.calc_checksum4();
        if (0 == commit_log_checksum_) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "commit log checksum is invalid", KR(ret), "context", *this);
          need_print_trace_log_ = true;
        }

        REC_TRANS_TRACE_EXT(tlog_,
            calc_checksum_by_commit,
            OB_ID(ret),
            ret,
            OB_ID(checksum),
            commit_log_checksum_,
            OB_ID(checksum_log_ts),
            mt_ctx_.get_checksum_log_ts());
      }
    }
  }

  return ret;
}

int ObPartTransCtx::do_abort_()
{
  const bool commit = false;
  (void)trans_end_(commit, get_global_trans_version_());
  (void)trans_clear_();
  return OB_SUCCESS;
}

int ObPartTransCtx::do_clear_()
{
  int ret = OB_SUCCESS;

  if (is_readonly_) {
    set_state_(Ob2PCState::CLEAR);
    const bool commit = false;
    (void)trans_end_(commit, get_global_trans_version_());
    (void)trans_clear_();
    (void)unregister_timeout_task_();
    set_exiting_();
  }

  return ret;
}

int ObPartTransCtx::on_prepare_redo_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (0 == redo_log_no_) {
    TRANS_LOG(WARN, "[XA] unexpected redo log number for xa prepare redo", "context", *this);
  } else {
    is_redo_prepared_ = true;
    // Reply prepare no to the coordinator, without judging the predecessor information
    if (OB_SUCCESS != (tmp_ret = post_2pc_response_(coordinator_, OB_TRANS_2PC_PREPARE_RESPONSE))) {
      TRANS_LOG(WARN, "[XA] submit 2pc xa prepare response error", "ret", tmp_ret, "context", *this);
    } else {
      TRANS_LOG(DEBUG, "[XA] on prepare redo success");
    }
  }

  return ret;
}

int ObPartTransCtx::on_prepare_(const bool batch_committed, const int64_t timestamp)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObITsMgr* ts_mgr = NULL;

  if (0 == redo_log_no_) {
    TRANS_LOG(DEBUG, "participant enter into 2pc", "context", *this);
  }
  set_state_(Ob2PCState::PREPARE);
  is_redo_prepared_ = true;
  trans_2pc_timeout_ = ObServerConfig::get_instance().trx_2pc_retry_interval;
  trans_2pc_timeout_ = std::min(trans_2pc_timeout_, (int64_t)MAX_TRANS_2PC_TIMEOUT_US);
  if (batch_committed) {
    // Set up end_log_ts_ for 1pc
    end_log_ts_ = timestamp;
    // 1pc can release lock and context immediately when clog is callback it
    if (OB_UNLIKELY(!batch_commit_trans_ || is_pre_preparing_() || global_trans_version_ <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected batch commit transaction status", K(ret), K_(batch_commit_trans), "context", *this);
      need_print_trace_log_ = true;
    } else if (OB_ISNULL(ts_mgr = get_ts_mgr_())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "ts mgr is null", KP(ts_mgr));
    } else if (OB_FAIL(ts_mgr->wait_gts_elapse(self_.get_tenant_id(), global_trans_version_))) {
      if (OB_EAGAIN != ret) {
        TRANS_LOG(WARN, "wait gts elapse fail", K(ret), "context", *this);
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      // When gts has pushed over the gts, you can directly release the lock
      if (OB_FAIL(do_dist_commit_(global_trans_version_, NULL))) {
        TRANS_LOG(WARN, "do commit error", K(ret), "context", *this);
      } else if (OB_FAIL(on_dist_commit_())) {
        TRANS_LOG(WARN, "on commit error", K(ret), "context", *this);
      } else if (OB_FAIL(do_clear_())) {
        TRANS_LOG(WARN, "do clear error", K(ret), "context", *this);
      } else if (OB_FAIL(on_clear_(false))) {
        TRANS_LOG(WARN, "on clear error", K(ret), "context", *this);
      } else {
        // do nothing
      }
    }
  } else {
    // Set up end_log_ts_ for 1pc
    if (batch_commit_trans_) {
      end_log_ts_for_batch_commit_ = timestamp;
      if (OB_FAIL(partition_service_->inc_pending_batch_commit_count(self_, mt_ctx_, timestamp))) {
        TRANS_LOG(WARN, "failed to inc batch commit count", K(ret), "context", *this);
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_SUCCESS != get_status_()) {
        // Need response coordinator witj prepare no without predecessor information
        if (OB_SUCCESS != (tmp_ret = post_2pc_response_(coordinator_, OB_TRANS_2PC_PREPARE_RESPONSE))) {
          TRANS_LOG(WARN, "submit 2pc prepare response error", "ret", tmp_ret, "context", *this);
        }
      } else {
        bool can_post_msg = false;
        int state = ObTransResultState::INVALID;
        if (OB_FAIL(ctx_dependency_wrap_.get_prev_trans_arr_result(state))) {
          TRANS_LOG(WARN, "get prev trans arr result error", K(ret), "context", *this);
        } else if (ObTransResultState::is_commit(state)) {
          // We can response immediately, the status is on default success
          can_post_msg = true;
        } else if (ObTransResultState::is_abort(state)) {
          can_post_msg = true;
          set_status_(OB_TRANS_ROLLBACKED);
          need_print_trace_log_ = true;
          TRANS_LOG(WARN, "current transaction need to rollback because of prev abort trans", "context", *this);
        } else {
          // We cannot response to coordinator if the predecessors' state is not decided
          can_post_msg = false;
        }
        if (!can_post_msg) {
          enable_new_1pc_ = false;
        } else if (enable_new_1pc_) {
          if (NULL == coord_ctx_) {
            bool alloc = false;
            if (OB_SUCCESS != (tmp_ret = trans_service_->get_coord_trans_ctx_mgr().get_trans_ctx(
                                   coordinator_, trans_id_, for_replay_, is_readonly_, alloc, coord_ctx_))) {
              if (OB_TRANS_CTX_NOT_EXIST != tmp_ret) {
                TRANS_LOG(WARN, "get transaction context error", K(tmp_ret), "context", *this);
              }
              // If the coordinator context is not obtained, we ignore the new 1pc
              enable_new_1pc_ = false;
            }
          }
        } else {
          if (OB_SUCCESS != (tmp_ret = post_2pc_response_(coordinator_, OB_TRANS_2PC_PREPARE_RESPONSE))) {
            TRANS_LOG(WARN, "submit 2pc prepare response error", "ret", tmp_ret, "context", *this);
          }
        }
      }
    }
  }

  return ret;
}

int ObPartTransCtx::on_dist_abort_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const bool commit = false;
  trans_2pc_timeout_ = MAX_TRANS_2PC_TIMEOUT_US;
  (void)trans_end_(commit, get_global_trans_version_());
  set_state_(Ob2PCState::ABORT);
  if (coordinator_ == self_) {
    ObTransStatus trans_status(trans_id_, OB_TRANS_ROLLBACKED);
    (void)trans_status_mgr_->set_status(trans_id_, trans_status);
  }
  if (!is_sp_trans_()) {
    if (OB_SUCCESS != (tmp_ret = try_respond_coordinator_(OB_TRANS_2PC_ABORT_RESPONSE, LISTENER_ABORT))) {
      TRANS_LOG(WARN, "try respond coordinator failed", KR(ret), K(*this));
    }
  }

  return ret;
}

// Need to wait for gts has pushed over the gts
int ObPartTransCtx::on_sp_commit_(const bool commit, const int64_t timestamp)
{
  int ret = OB_SUCCESS;
  ObITsMgr* ts_mgr = get_ts_mgr_();

  // Rollback donot need to wait
  if (OB_ISNULL(ts_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "ts mgr is null", KR(ret));
  } else if (!commit) {
    if (OB_FAIL(trans_sp_end_(commit))) {
      TRANS_LOG(WARN, "transaction abort error", KR(ret), "context", *this);
    }
  } else {
    bool need_wait = false;
    if (OB_FAIL(ts_mgr->wait_gts_elapse(self_.get_tenant_id(), global_trans_version_, this, need_wait))) {
      TRANS_LOG(WARN, "wait gts elapse failed", KR(ret), "context", *this);
    } else if (need_wait) {
      if (OB_INVALID_TIMESTAMP != timestamp) {
        async_applying_log_ts_ = timestamp;
      }
      is_gts_waiting_ = true;
      gts_request_ts_ = ObTimeUtility::current_time();
      if (OB_FAIL(partition_mgr_->acquire_ctx_ref(trans_id_))) {
        TRANS_LOG(WARN, "get trans ctx error", KR(ret), "context", *this);
      }
      REC_TRANS_TRACE_EXT(tlog_, wait_gts_elapse);
    } else if (OB_FAIL(trans_sp_end_(commit))) {
      TRANS_LOG(WARN, "transaction commit error", KR(ret), "context", *this);
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObPartTransCtx::trans_sp_end_(const bool commit)
{
  int ret = OB_SUCCESS;
  const int64_t start = ObTimeUtility::fast_current_time();
  int64_t trans_end = 0;
  int64_t end = 0;

  if (OB_UNLIKELY(!is_sp_trans_()) && OB_UNLIKELY(!is_mini_sp_trans_())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected transaction type", KR(ret), K(commit), "context", *this);
  } else if (commit && OB_FAIL(update_publish_version_(global_trans_version_, false))) {
    TRANS_LOG(WARN, "update publish version error", KR(ret), K_(global_trans_version), "context", *this);
  } else {
#ifdef ERRSIM
    ret = E(EventTable::EN_LOG_SYNC_SLOW) OB_SUCCESS;
    if (can_elr_ && OB_FAIL(ret)) {
      TRANS_LOG(INFO, "ERRSIM log sync slow", K_(trans_id));
      sleep(5);
    }
#endif
    if (OB_FAIL(trans_end_(commit, global_trans_version_))) {
      TRANS_LOG(WARN, "transaciton end error", KR(ret), "context", *this);
    } else if (OB_FAIL(trans_clear_())) {
      TRANS_LOG(WARN, "transaciton clear error", KR(ret), "context", *this);
    } else {
      if (commit) {
        set_state_(ObSpState::COMMIT);
      } else {
        set_state_(ObSpState::ABORT);
      }
      TRANS_STAT_TOTAL_USED_TIME(tenant_id_, ctx_create_time_);
      TRANS_STAT_COMMIT_ABORT_TRANS_TIME(!commit, tenant_id_);
      // If the pending count is not 0, it means that after the clog callback
      // transaction, the clog adapter has not yet called back the transaction.
      // At this time, if set_exiting releases ctx, it may cause the
      // on_submit_log_success of the current transaction to be called back to
      // the ctx of the transaction, and there is even a risk of core dump, so
      // when submit_log_pending_count is reduced to 0, the set_exiting
      // operation should be invoked
      if (0 == submit_log_pending_count_) {
        set_exiting_();
      }
      (void)unregister_timeout_task_();
      trans_end = ObTimeUtility::fast_current_time();
      if (USER_COMMIT == sp_user_request_) {
        if (commit) {
          end_trans_callback_(OB_SUCCESS);
        } else {
          end_trans_callback_(OB_TRANS_KILLED);
          TRANS_LOG(WARN, "user request commit but trx rollbacked", K(*this));
        }
      } else if (USER_ABORT == sp_user_request_) {
        end_trans_callback_(OB_SUCCESS);
      } else {
        end_trans_callback_(OB_TRANS_UNKNOWN);
        TRANS_LOG(WARN, "user request is unknown", K(*this));
      }
    }
  }
  end = ObTimeUtility::fast_current_time();
  if (end - start > 10 * 1000) {
    TRANS_LOG(WARN,
        "transaction commit too much time",
        "context",
        *this,
        "trans_end_used",
        trans_end - start,
        "callback_sql_used",
        end - trans_end);
  }

  return ret;
}

int ObPartTransCtx::on_dist_commit_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_sp_trans_())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected trans type", KR(ret), "context", *this);
    need_print_trace_log_ = true;
  } else if (enable_new_1pc_) {
    const bool commit = true;
    trans_2pc_timeout_ = MAX_TRANS_2PC_TIMEOUT_US;
    // Distributed transactions need to wait for the commit log majority successfully before unlocking.
    // If you want to know the reason, it is in the ::do_dist_commit
    if (OB_FAIL(trans_end_(commit, global_trans_version_))) {
      TRANS_LOG(WARN, "transaciton end error", K(ret), "context", *this);
    }
    set_state_(Ob2PCState::COMMIT);
  } else if (batch_commit_trans_) {
    set_state_(Ob2PCState::COMMIT);
  } else {
    const bool commit = true;
    trans_2pc_timeout_ = MAX_TRANS_2PC_TIMEOUT_US;
    // Distributed transactions need to wait for the commit log majority successfully before unlocking.
    // If you want to know the reason, it is in the ::do_dist_commit
    if (OB_FAIL(trans_end_(commit, global_trans_version_))) {
      TRANS_LOG(WARN, "transaciton end error", KR(ret), "context", *this);
    }
    set_state_(Ob2PCState::COMMIT);
    if (OB_SUCCESS != (tmp_ret = try_respond_coordinator_(OB_TRANS_2PC_COMMIT_RESPONSE, LISTENER_COMMIT))) {
      TRANS_LOG(WARN, "try respond coordinator failed", KR(tmp_ret), K(*this));
    }
  }

  return ret;
}

int ObPartTransCtx::on_clear_(const bool need_response)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  (void)trans_clear_();
  set_state_(Ob2PCState::CLEAR);
  if (0 == submit_log_pending_count_) {
    set_exiting_();
  }
  (void)unregister_timeout_task_();
  if (!is_sp_trans_() && !is_mini_sp_trans_()) {
    if (batch_commit_trans_) {
      // For local invokation, do not reply to the message here, otherwise deadlock will occur
      if (need_response &&
          OB_SUCCESS != (tmp_ret = post_2pc_response_(coordinator_, OB_TRANS_2PC_COMMIT_CLEAR_RESPONSE))) {
        TRANS_LOG(WARN, "submit 2pc commit clear response error", "ret", tmp_ret, "context", *this);
      }
    } else if (enable_new_1pc_) {
      if (NULL != coord_ctx_ && !submit_log_cb_.is_commit_log_callbacking()) {
        trans_service_->get_coord_trans_ctx_mgr().revert_trans_ctx(coord_ctx_);
        coord_ctx_ = NULL;
      }
    } else {
      // Distributed transaction, can not take single-machine transaction optimization
      if (OB_SUCCESS != (tmp_ret = post_2pc_response_(coordinator_, OB_TRANS_2PC_CLEAR_RESPONSE))) {
        TRANS_LOG(WARN, "submit 2pc clear response error", "ret", tmp_ret, "context", *this);
      }
    }
  }

  return ret;
}

void ObPartTransCtx::inc_submit_log_pending_count_()
{
  if (OB_UNLIKELY(submit_log_pending_count_ < 0)) {
    TRANS_LOG(ERROR, "unexpected value", K_(submit_log_pending_count), "context", *this);
  } else {
    ++submit_log_pending_count_;
  }
  REC_TRANS_TRACE_EXT(tlog_,
      log_pending,
      OB_ID(logging),
      submit_log_count_,
      OB_ID(pending),
      submit_log_pending_count_,
      OB_ID(uref),
      get_uref());
}

void ObPartTransCtx::dec_submit_log_pending_count_()
{
  if (OB_UNLIKELY(submit_log_pending_count_ <= 0)) {
    TRANS_LOG(ERROR, "unexpected value", K_(submit_log_pending_count), "context", *this);
  } else {
    --submit_log_pending_count_;
  }
  REC_TRANS_TRACE_EXT(tlog_,
      log_pending_cb,
      OB_ID(logging),
      submit_log_count_,
      OB_ID(pending),
      submit_log_pending_count_,
      OB_ID(uref),
      get_uref());
}

void ObPartTransCtx::inc_submit_log_count_()
{
  if (OB_UNLIKELY(submit_log_count_ < 0)) {
    TRANS_LOG(ERROR, "unexpected value", K_(submit_log_count), "context", *this);
  } else {
    ++submit_log_count_;
  }
  REC_TRANS_TRACE_EXT(tlog_,
      inc_submit_log_count,
      OB_ID(logging),
      submit_log_count_,
      OB_ID(pending),
      submit_log_pending_count_,
      OB_ID(uref),
      get_uref());
}

void ObPartTransCtx::dec_submit_log_count_()
{
  if (OB_UNLIKELY(submit_log_count_ <= 0)) {
    TRANS_LOG(ERROR, "unexpected value", K_(submit_log_count), "context", *this);
  } else {
    --submit_log_count_;
  }
  REC_TRANS_TRACE_EXT(tlog_,
      dec_submit_log_count,
      OB_ID(logging),
      submit_log_count_,
      OB_ID(pending),
      submit_log_pending_count_,
      OB_ID(uref),
      get_uref());
}

int ObPartTransCtx::calc_batch_commit_version_(int64_t& commit_version)
{
  int ret = OB_SUCCESS;
  int64_t tmp_commit_version = 0;
  if (OB_UNLIKELY(0 >= partition_log_info_arr_.count())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected error, partition log info is empty", KR(ret));
    need_print_trace_log_ = true;
  } else {
    for (int64_t i = 0; i < partition_log_info_arr_.count(); ++i) {
      const int64_t ts = partition_log_info_arr_.at(i).get_log_timestamp();
      if (tmp_commit_version < ts) {
        tmp_commit_version = ts;
      }
    }
  }
  if (OB_SUCCESS == ret) {
    commit_version = tmp_commit_version;
  }
  return ret;
}

int ObPartTransCtx::commit_by_checkpoint_(const int64_t commit_version)
{
  ObTimeGuard tg("commit_by_checkpoint", 50 * 1000);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == commit_version)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected error, invalid commit version", KR(ret), K(commit_version));
    need_print_trace_log_ = true;
  } else {
    end_log_ts_ = end_log_ts_for_batch_commit_;
    tg.click();
    if (OB_FAIL(trans_replay_commit_(commit_version, 0))) {
      TRANS_LOG(WARN, "replay transaction commit error", KR(ret));
    } else {
      tg.click();
      if (OB_FAIL(trans_clear_())) {
        TRANS_LOG(WARN, "transaction clear error", KR(ret));
      } else {
        tg.click();
        set_exiting_();
        INC_ELR_STATISTIC(end_trans_by_checkpoint_count);
        tg.click();
      }
    }
  }
  return ret;
}

void ObPartTransCtx::update_last_checkpoint_(const int64_t checkpoint)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  if (NULL != pg_) {
    if (OB_FAIL(pg_->update_last_checkpoint(checkpoint))) {
      TRANS_LOG(WARN, "update last checkpoint failed", KR(ret), K_(self), K(checkpoint));
    }
  } else if (OB_FAIL(partition_service_->get_partition(self_, guard))) {
    TRANS_LOG(WARN, "get partition failed", KR(ret), K_(self));
  } else if (NULL == (partition = guard.get_partition_group())) {
    TRANS_LOG(WARN, "partition is null", K_(self));
  } else if (OB_FAIL(partition->update_last_checkpoint(checkpoint))) {
    TRANS_LOG(WARN, "update last checkpoint failed", KR(ret), K_(self), K(checkpoint));
  } else {
  }
  UNUSED(ret);
}

int ObPartTransCtx::prepare_changing_leader_(const ObAddr& proposal_leader)
{
  int ret = OB_SUCCESS;
  bool has_redo_log = false;
  common::ObTimeGuard timeguard("prepare_changing_leader_", 1000);

  if (OB_UNLIKELY(is_dup_table_trans_)) {
    TRANS_LOG(INFO, "dup table trans skip prepare changing leader", "context", *this);
  } else if (cluster_version_before_2271_()) {
    TRANS_LOG(INFO, "trans changing leader are banned during upgrade", "context", *this);
  } else {
    int state = ObTransResultState::INVALID;
    if (OB_FAIL(ctx_dependency_wrap_.get_prev_trans_arr_result(state))) {
      TRANS_LOG(WARN, "get prev trans arr result error", K(ret), "context", *this);
    } else if (ObTransResultState::is_commit(state)) {

      ctx_dependency_wrap_.reset_prev_trans_arr();
    } else if (ObTransResultState::is_abort(state)) {
      set_status_(OB_TRANS_ROLLBACKED);
      need_print_trace_log_ = true;
      ctx_dependency_wrap_.reset_prev_trans_arr();
      TRANS_LOG(WARN, "current transaction need to rollback because of prev abort trans", "context", *this);
    } else {
      // do nothing
    }

    timeguard.click();

    if (OB_SUCCESS == get_status_()) {
      // set proposal leader before submit log
      proposal_leader_ = proposal_leader;
      if (OB_FAIL(submit_log_async_(OB_LOG_MUTATOR_WITH_STATE, has_redo_log))) {
        TRANS_LOG(WARN, "submit trans mutator with state log failed", KR(ret), "context", *this);
      } else {
        TRANS_LOG(INFO, "submit trans mutator with state log success", "context", *this);
      }
      if (OB_SUCCESS == ret) {
        // If the MUTATOR log is triggered when leader transfer, the MUTATOR
        // STATE log needs to be triggered by marking prepare_changing_leader_state_
        prepare_changing_leader_state_ = CHANGING_LEADER_STATE::LOGGING_NOT_FINISH;
        is_trans_state_sync_finished_ = false;
        is_changing_leader_ = true;
        TRANS_LOG(DEBUG, "prepare changing leader success", "context", *this, K(proposal_leader));
      } else {
        TRANS_LOG(WARN, "prepare changing leader failed", KR(ret), "context", *this, K(proposal_leader));
      }
    }
    timeguard.click();
  }
  return ret;
}

int ObPartTransCtx::get_checkpoint_(int64_t& checkpoint)
{
  int ret = OB_SUCCESS;
  int64_t tmp_checkpoint = 0;
  storage::ObIPartitionGroupGuard pkey_guard;
  if (quick_checkpoint_ <= 0) {
    if (NULL != pg_) {
      if (OB_FAIL(pg_->get_weak_read_timestamp(tmp_checkpoint))) {
        TRANS_LOG(WARN, "get weak read timestamp error", K(tmp_checkpoint), "context", *this);
      } else {
        quick_checkpoint_ = tmp_checkpoint;
      }
    } else if (OB_FAIL(partition_service_->get_partition(self_, pkey_guard))) {
      TRANS_LOG(WARN, "get partition error", KR(ret), "context", *this);
    } else if (NULL == pkey_guard.get_partition_group()) {
      TRANS_LOG(ERROR, "partition is null, unexpected error", KP(pkey_guard.get_partition_group()), "context", *this);
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(pkey_guard.get_partition_group()->get_weak_read_timestamp(tmp_checkpoint))) {
      TRANS_LOG(WARN, "get weak read timestamp error", K(tmp_checkpoint), "context", *this);
    } else {
      quick_checkpoint_ = tmp_checkpoint;
    }
  } else {
    tmp_checkpoint = quick_checkpoint_;
  }
  if (OB_SUCCESS == ret) {
    checkpoint = tmp_checkpoint;
  }
  return ret;
}

int ObPartTransCtx::get_prepare_version_if_prepared(bool& is_prepared, int64_t& prepare_version)
{
  int ret = OB_SUCCESS;
  is_prepared = false;
  int64_t cur_state = Ob2PCState::UNKNOWN;
  int64_t cur_prepare_version = ObTransVersion::INVALID_TRANS_VERSION;
  if (!is_xa_local_trans() || (is_xa_local_trans() && is_xa_trans_prepared_)) {
    state_.get_state_and_version(cur_state, cur_prepare_version);
  }
  if (ObTransVersion::INVALID_TRANS_VERSION != cur_prepare_version && ObSpState::COMMIT != cur_state &&
      ObSpState::ABORT != cur_state && Ob2PCState::COMMIT != cur_state && Ob2PCState::ABORT != cur_state &&
      Ob2PCState::CLEAR != cur_state && ObRunningState::ABORT != cur_state) {
    is_prepared = true;
    prepare_version = cur_prepare_version;
  }
  return ret;
}

/*
 * return prepare logtimestamp if the prepare_logtimestamp smaller than freeze_ts, but end_log_ts bigger
 * */
int ObPartTransCtx::get_prepare_version_before_logts(const int64_t freeze_ts, bool& has_prepared, int64_t& prepare_version)
{
  int ret = OB_SUCCESS;
  const int64_t SLEEP_US = 100;
  int64_t begin_time = common::ObTimeUtility::current_time();
  int64_t cur_state = Ob2PCState::UNKNOWN;
  int64_t cur_prepare_version = ObTransVersion::INVALID_TRANS_VERSION;

  has_prepared = false;
  if (!is_xa_local_trans() || (is_xa_local_trans() && is_xa_trans_prepared_)) {
    // cur_prepare_version is the redo log timestamp before prepare.
    state_.get_state_and_version(cur_state, cur_prepare_version);
  }
  if (ObTransVersion::INVALID_TRANS_VERSION != cur_prepare_version &&  // xa really prepared, normal trans has redo log
      OB_INVALID_TIMESTAMP != prepare_log_timestamp_ &&                // trans really prepared
      prepare_log_timestamp_ <= freeze_ts &&                           // prepare before freeze ts
      end_log_ts_ > freeze_ts) {                                       // trans end after freeze ts
    has_prepared = true;
    prepare_version = prepare_log_timestamp_;
  }
  return ret;
}

int64_t ObPartTransCtx::get_snapshot_version() const
{
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);
  return snapshot_version_;
}

int ObPartTransCtx::recover_dist_trans(const ObAddr& scheduler)
{
  int ret = OB_SUCCESS;
  const int64_t left_time = trans_expired_time_ - ObClockGenerator::getClock();
  const int64_t usec_per_sec = 1000 * 1000;
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (OB_UNLIKELY(!scheduler.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(scheduler), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else {
    scheduler_ = scheduler;
    trans_type_ = TransType::DIST_TRANS;
    is_local_trans_ = false;
    if (!for_replay_) {
      // After sp_trans is switched to dist_trans, we need to register the timing task
      const int64_t delay = left_time + trans_id_.hash() % usec_per_sec;
      (void)unregister_timeout_task_();
      if (OB_FAIL(register_timeout_task_(delay))) {
        TRANS_LOG(WARN, "register timeout handler error", KR(ret), "context", *this);
      }
    }
  }
  REC_TRANS_TRACE_EXT(tlog_, recover_dist_trans, OB_ID(ret), ret, OB_ID(uref), get_uref());

  return ret;
}

int ObPartTransCtx::set_trans_app_trace_id_str(const ObString& app_trace_id_str)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else {
    ret = set_app_trace_id_(app_trace_id_str);
  }
  return ret;
}

int ObPartTransCtx::get_min_log(uint64_t& min_log_id, int64_t& min_log_ts) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (is_exiting_ || is_readonly_ || OB_INVALID_ID == min_log_id_) {
    ATOMIC_STORE(&min_log_id, UINT64_MAX);
    min_log_ts = INT64_MAX;
  } else {
    ATOMIC_STORE(&min_log_id, min_log_id_);
    min_log_ts = min_log_ts_;
  }

  return ret;
}

int ObPartTransCtx::relocate_data(memtable::ObIMemtable* memtable)
{
  int ret = OB_SUCCESS;
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(memtable)) {
    TRANS_LOG(WARN, "invalid argument", KP(memtable), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (is_exiting_ || is_readonly_) {
    // do nothing
  } else if (!for_replay_) {
    if (!is_in_2pc_() && !is_logging_() && !mt_ctx_.is_prepared() && stmt_info_.is_task_match()) {
      if (OB_FAIL(mt_ctx_.set_leader_host(static_cast<ObMemtable*>(memtable), for_replay_))) {
        TRANS_LOG(WARN, "ObPartTransCtx set host error", KP(memtable), "context", *this);
      }
    }

  } else if ((!is_sp_trans_() && state_.get_state() < Ob2PCState::COMMIT) &&
             (is_sp_trans_() && state_.get_state() < ObSpState::COMMIT) && redo_log_no_ + mutator_log_no_ > 0) {
    if (OB_FAIL(mt_ctx_.set_replay_host(static_cast<ObMemtable*>(memtable), for_replay_))) {
      TRANS_LOG(WARN, "ObPartTransCtx set replay host error", KP(memtable), "context", *this);
    }
  } else {
    // do nothing
  }

  return ret;
}

int ObPartTransCtx::get_memtable_key_arr(ObMemtableKeyArray& memtable_key_arr)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == lock_.try_lock()) {
    if (IS_NOT_INIT || is_readonly_ || for_replay_ || is_exiting_) {
      TRANS_LOG(DEBUG, "part_ctx not need to get memtable key");
    } else if (OB_FAIL(mt_ctx_.get_memtable_key_arr(memtable_key_arr))) {
      TRANS_LOG(WARN, "get_memtable_key_arr fail", K(ret), K(memtable_key_arr), K(mt_ctx_));
    } else {
      // do nothing
    }
    lock_.unlock();
  } else {
    ObMemtableKeyInfo info;
    info.init(1, 1);
    memtable_key_arr.push_back(info);
  }

  return ret;
}

int ObPartTransCtx::prepare_changing_leader(const ObAddr& proposal_leader)
{
  int ret = OB_SUCCESS;
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!proposal_leader.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(proposal_leader));
  } else if (is_bounded_staleness_read() || is_readonly()) {
    // do nothing
  } else if (is_exiting_) {
    if (!is_dirty_) {
      TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    } else {
      TRANS_LOG(INFO, "dirty transaction is prepare changing leader", "context", *this);
    }
  } else if (for_replay_) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "invalid state, transaction is replaying", KR(ret), "context", *this);
  } else if (is_in_2pc_()) {
    TRANS_LOG(INFO, "transaction already in 2pc", "context", *this);
  } else if (is_pre_preparing_()) {
    TRANS_LOG(INFO, "transaction is pre preparing", "context", *this);
  } else if (!stmt_info_.is_task_match()) {
    is_changing_leader_ = true;
    prepare_changing_leader_state_ = CHANGING_LEADER_STATE::STATEMENT_NOT_FINISH;
    proposal_leader_ = proposal_leader;
    TRANS_LOG(INFO, "current stmt task not match", "context", *this);
  } else if (is_logging_()) {
    is_changing_leader_ = true;
    prepare_changing_leader_state_ = CHANGING_LEADER_STATE::LOGGING_NOT_FINISH;
    proposal_leader_ = proposal_leader;
    TRANS_LOG(INFO, "transaction is logging when prepare changing leader", "context", *this);
  } else if (OB_FAIL(prepare_changing_leader_(proposal_leader))) {
    TRANS_LOG(WARN, "prepare changing leader failed", KR(ret), "context", *this);
  }
  need_print_trace_log_ = true;
  REC_TRANS_TRACE_EXT(tlog_, migrate_trx, OB_ID(ret), ret, OB_ID(proposal_leader), proposal_leader);
  return ret;
}

bool ObPartTransCtx::can_be_recycled_()
{
  bool bool_ret = false;
  if (IS_NOT_INIT) {
    bool_ret = false;
  } else if (is_exiting_) {
    bool_ret = false;
  } else if (is_sp_trans_() || is_mini_sp_trans_()) {
    // Single-partitioned transaction starts to leader transfer, the read-only
    // context of the follower needs garbage collection
    if (for_replay_ && is_readonly_) {
      bool_ret = true;
    } else {
      bool_ret = false;
    }
  } else if (is_in_2pc_()) {
    bool_ret = false;
  } else if (for_replay_ && !is_readonly_) {
    bool_ret = false;
  } else if (is_logging_()) {
    // If the txn is logging, it cannot be recycled
    bool_ret = false;
  } else {
    TRANS_LOG(DEBUG, "can be recycled", "context", *this);
    bool_ret = true;
  }

  return bool_ret;
}

bool ObPartTransCtx::need_to_ask_scheduler_status_()
{
  bool bool_ret = false;
  if (can_be_recycled_()) {
    if (ObTimeUtility::current_time() - last_ask_scheduler_status_ts_ < CHECK_SCHEDULER_STATUS_INTERVAL) {
      bool_ret = false;
    } else {
      bool_ret = true;
    }
  }
  return bool_ret;
}

int ObPartTransCtx::check_rs_scheduler_is_alive_(bool& is_alive)
{
  int ret = OB_SUCCESS;
  int64_t trace_time = 0;
  int64_t cur_time = ObTimeUtility::current_time();
  share::ObAliveServerTracer* server_tracer = NULL;

  is_alive = true;
  if (0 != last_ask_scheduler_status_ts_ &&
      cur_time - last_ask_scheduler_status_response_ts_ < CHECK_RS_SCHEDULER_STATUS_INTERVAL) {
    // If it's not the time to check, don't ask rs about the scheduler's machine
    // status, and assume that it will survive by default.
    is_alive = true;
  } else if (OB_ISNULL(trans_service_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "trans service is NULL", KR(ret), K(*this));
  } else if (OB_ISNULL(server_tracer = trans_service_->get_server_tracer())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "server tracer is NULL", KR(ret), K(*this));
  } else if (OB_FAIL(server_tracer->is_alive(scheduler_, is_alive, trace_time))) {
    TRANS_LOG(WARN, "server tracer error", KR(ret), "context", *this);
    // To be conservative, if the server tracer reports an error, the scheduler
    // is alive by default
    is_alive = true;
  } else {
    // do nothing
  }

  return ret;
}

int ObPartTransCtx::force_kill_()
{
  int ret = OB_SUCCESS;
  bool has_redo_log = false;

  TRANS_LOG(INFO, "[TRANS GC]part ctx force kill", "context", *this);
  need_print_trace_log_ = true;
  if (has_logged_() || is_logging_()) {
    TRANS_LOG(INFO, "transaction has logged", "context", *this);
    if (redo_log_no_ > 0) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected error, redo_log_no_ > 0", KR(ret), "context", *this);
    } else if (is_logging_()) {
      part_trans_action_ = ObPartTransAction::DIED;
      TRANS_LOG(WARN, "in logging when compensate mutator abort", K(ret), "context", *this);
    } else if (mutator_log_no_ > 0 || has_trans_state_log_) {
      if (OB_FAIL(submit_log_async_(OB_LOG_MUTATOR_ABORT, has_redo_log))) {
        TRANS_LOG(WARN, "submit mutator abort log failed", KR(ret), "context", *this);
      } else {
        TRANS_LOG(INFO, "submit mutator abort log success", "context", *this);
      }
    } else {
      // do nothing
    }
  } else {
    const bool commit = false;
    (void)unregister_timeout_task_();
    (void)trans_end_(commit, get_global_trans_version_());
    (void)trans_clear_();
    set_exiting_();
  }

  return ret;
}

int ObPartTransCtx::post_ask_scheduler_status_msg_()
{
  int ret = OB_SUCCESS;
  // Send OB_TRANS_ASK_SCHEDULER_STATUS_REQUEST request to scheduler
  ObTransMsg msg;

  if (OB_FAIL(msg.init(tenant_id_,
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

int ObPartTransCtx::check_scheduler_status()
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS == lock_.try_lock()) {
    CtxTransTableLockGuard guard(lock_, trans_table_seqlock_, false);
    if (!need_to_ask_scheduler_status_()) {
      TRANS_LOG(DEBUG, "don't need ask scheduler status", K(ret), K(*this));
    } else {
      // 1. check the status of scheduler on rs
      bool is_alive = true;
      if (OB_FAIL(check_rs_scheduler_is_alive_(is_alive))) {
        TRANS_LOG(WARN, "check rs scheduler is alive error", KR(ret), K(is_alive), "context", *this);
        // scheduler down
      } else if (!is_alive) {
        if (OB_FAIL(force_kill_())) {
          TRANS_LOG(WARN, "force kill part_ctx error", KR(ret), "context", *this);
        }
      } else {
        // do nothing
      }
      // 2. Ask for the status of scheduler
      ret = OB_SUCCESS;
      if (is_alive && OB_FAIL(post_ask_scheduler_status_msg_())) {
        TRANS_LOG(WARN, "post ask scheduler status msg error", KR(ret), "context", *this);
      }
    }
  }

  return ret;
}

int ObPartTransCtx::elr_next_trans_callback(const int64_t task_type, const ObTransID& trans_id, int state)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int result = ObTransResultState::INVALID;
  const bool commit = false;
  if (OB_FAIL(lock_.try_lock())) {
    if (OB_EAGAIN != ret) {
      TRANS_LOG(ERROR, "try lock error", K(ret), K(task_type), K(trans_id));
    }
  } else {
    CtxTransTableLockGuard guard(lock_, trans_table_seqlock_, false);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      TRANS_LOG(WARN, "part ctx not init", KR(ret), K(*this));
    } else if (OB_UNLIKELY(is_exiting_)) {
      ret = OB_TRANS_IS_EXITING;
      TRANS_LOG(WARN, "part ctx is exiting", KR(ret), K(*this));
    } else if (OB_UNLIKELY(!trans_id.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "trans id is invalid", KR(ret), K(*this), K(trans_id));
    } else if (OB_UNLIKELY(!ObTransResultState::is_decided_state(state))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "prev trans end callback state not decided", KR(ret), K(state), K_(trans_id));
    } else if (ObTransRetryTaskType::CALLBACK_NEXT_TRANS_TASK == task_type) {
      TRANS_LOG(DEBUG,
          "start to callback next transaction",
          K(task_type),
          K(state),
          "caller_trans_id",
          trans_id,
          "called_trans_id",
          trans_id_,
          K_(self));
      // After the predecessor transaction ends, the callback continues
      if (OB_FAIL(ctx_dependency_wrap_.mark_prev_trans_result(trans_id, state, result))) {
        TRANS_LOG(WARN, "mark prev trans result error", KR(ret), K(trans_id), K(state), K(*this));
      } else if (ObTransResultState::is_commit(state) &&
                 OB_FAIL(ctx_dependency_wrap_.remove_prev_trans_item(trans_id))) {
        TRANS_LOG(WARN, "remove prev trans item error", K(ret), K(trans_id));
      } else if (OB_FAIL(drive_by_prev_trans_(result))) {
        TRANS_LOG(WARN, "drive by prev trans error", KR(ret), K(result), K(*this));
      } else {
        // do nothing
      }
    } else if (ObTransRetryTaskType::CALLBACK_PREV_TRANS_TASK == task_type) {
      //  The successor will not callback the predecessor, and it is not expected to appear here
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected transaction task type", K(ret), K(task_type), K(*this));
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR,
          "unexpected task type",
          KR(ret),
          K(task_type),
          "prev_or_next_trans_id",
          trans_id,
          K(state),
          "context",
          *this);
    }

    REC_TRANS_TRACE_EXT(
        tlog_, elr_prev_next_callback, OB_ID(type), task_type, OB_ID(inc), trans_id.get_inc_num(), OB_ID(state), state);
  }

  return ret;
}

int ObPartTransCtx::drive_by_prev_trans_(int state)
{
  int ret = OB_SUCCESS;
  if (ObTransResultState::is_commit(state)) {
    if (for_replay_) {
      if (is_sp_trans_()) {
        if (ObSpState::PREPARE == get_state_()) {
          const uint64_t checksum = 0;
          if (OB_FAIL(trans_replay_commit_(global_trans_version_, checksum))) {
            TRANS_LOG(WARN, "trans replay commit failed", KR(ret), "context", *this);
          } else if (OB_FAIL(trans_clear_())) {
            TRANS_LOG(WARN, "transaction clear error", KR(ret), "context", *this);
          } else {
            end_log_ts_ = end_log_ts_for_elr_;
            set_state_(ObSpState::COMMIT);
            set_exiting_();
            need_print_trace_log_ = true;
            TRANS_LOG(INFO, "transaciton replay committed by prev trans", K(state), "context", *this);
            INC_ELR_STATISTIC(end_trans_by_prev_count);
          }
        }
      } else if (Ob2PCState::PREPARE == get_state_()) {
        // CLOG thinks the bacth commit success, and we can finish the txn in advance
        if (ObBatchCommitState::BATCH_COMMITTED == batch_commit_state_) {
          int64_t commit_version = 0;
          if (OB_FAIL(calc_batch_commit_version_(commit_version))) {
            TRANS_LOG(WARN, "calc batch commit version failed", KR(ret));
          } else if (OB_FAIL(commit_by_checkpoint_(commit_version))) {
            TRANS_LOG(WARN, "commit by checkpoint failed", K(commit_version));
          } else {
            need_print_trace_log_ = true;
            TRANS_LOG(INFO, "transaciton replay committed by prev trans", K(state), "context", *this);
          }
        }
      }
    } else {
      if (is_sp_trans_() && ObSpState::PREPARE == get_state_()) {
        if (OB_SUCCESS != get_status_()) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "unexpected status", KR(ret), "context", *this);
        } else if (OB_FAIL(on_sp_commit_(true))) {
          TRANS_LOG(WARN, "sp trans on commit error", KR(ret), "context", *this);
        } else {
          end_log_ts_ = end_log_ts_for_elr_;
        }
      } else if (OB_FAIL(drive_())) {
        TRANS_LOG(WARN, "drive error", KR(ret), K(state), K(*this));
      } else {
        // do nothing
      }
    }
  } else if (ObTransResultState::is_abort(state)) {
    if (for_replay_) {
      // Reply on abort log to drive the txn
      TRANS_LOG(INFO, "current transaciton need to abort", K(state), "context", *this);
    } else {
      // The precursor txn is aborted, so we can drives the abort of this txn
      set_status_(OB_TRANS_NEED_ROLLBACK);
      if (OB_FAIL(drive_())) {
        TRANS_LOG(WARN, "drive error", KR(ret), K(state), K(*this));
      }
    }
  } else {
    // do nothing
  }

  return ret;
}

int ObPartTransCtx::submit_elr_callback_task(const int64_t callback_type, const ObTransID& trans_id, int state)
{
  int ret = OB_SUCCESS;
  CallbackTransTask* task = NULL;

  if (OB_UNLIKELY(NULL == (task = CallbackTransTaskFactory::alloc()))) {
    TRANS_LOG(ERROR, "alloc callback trans task error", KP(task));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    if (OB_FAIL(task->make(callback_type, self_, trans_id, trans_id_, state))) {
      TRANS_LOG(WARN,
          "elr callback task init error",
          KR(ret),
          K(callback_type),
          K_(self),
          K(trans_id),
          K(trans_id_),
          K(state));
    } else if (OB_FAIL(trans_service_->push(task))) {
      TRANS_LOG(WARN, "transaction service push task error", KR(ret), "rpc_task", *task);
    } else {
      // do nothing
    }
    if (OB_FAIL(ret)) {
      CallbackTransTaskFactory::release(task);
    }
  }

  return ret;
}

int ObPartTransCtx::check_elr_prepared(bool& elr_prepared, int64_t& elr_commit_version)
{
  int ret = OB_SUCCESS;

  if (!can_elr_) {
    if (is_elr_prepared_()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected elr prepare state", KR(ret), K_(elr_prepared_state), "context", *this);
    }
  } else if (for_replay_) {
    // After leader becomes follower, readonly txn will not be killed, the read
    // need wait
    reset_elr_state_();
  } else if (is_elr_prepared_()) {
    if (OB_SUCCESS != get_status_()) {
      reset_elr_state_();
    } else {
      elr_prepared = is_elr_prepared_();
      elr_commit_version = global_trans_version_;
    }
  } else if (OB_SUCCESS == lock_.try_lock()) {
    CtxTransTableLockGuard guard(lock_, trans_table_seqlock_, false);
    if (is_elr_preparing_() && OB_FAIL(check_and_early_release_lock_())) {
      TRANS_LOG(WARN, "checn and early release lock error", KR(ret), "context", *this);
    } else {
      elr_prepared = is_elr_prepared_();
      elr_commit_version = global_trans_version_;
    }
  } else {
    elr_prepared = false;
  }

  return ret;
}

// Register the predecessor information register the successor information of
// the predecessor at the same time. This method can be invoked without locking
int ObPartTransCtx::insert_prev_trans(const uint32_t ctx_id, ObTransCtx* prev_trans_ctx)
{
  int ret = OB_SUCCESS;

  if (for_replay_) {
    ret = OB_EAGAIN;
    TRANS_LOG(
        WARN, "insert prev trans error", K(ret), "prev_trans_id", prev_trans_ctx->get_trans_id(), "context", *this);
  } else if (is_dup_table_trans_) {
    ret = OB_EAGAIN;
  } else if (self_ != prev_trans_ctx->get_partition()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unepxected partition info", K(ret), K(ctx_id), "prev_pkey", prev_trans_ctx->get_partition());
  } else if (OB_FAIL(ctx_dependency_wrap_.register_dependency_item(ctx_id, prev_trans_ctx))) {
    TRANS_LOG(DEBUG,
        "insert prev trans error",
        KR(ret),
        "prev_ctx_id",
        ctx_id,
        "prev_trans_id",
        prev_trans_ctx->get_trans_id(),
        "prev_pkey",
        prev_trans_ctx->get_partition(),
        "cur_trans_id",
        trans_id_,
        "cur_pkey",
        self_);
  } else {
    TRANS_LOG(DEBUG,
        "insert prev trans success",
        KR(ret),
        "prev_ctx_id",
        ctx_id,
        "prev_trans_id",
        prev_trans_ctx->get_trans_id(),
        "prev_pkey",
        prev_trans_ctx->get_partition(),
        "cur_trans_id",
        trans_id_,
        "cur_pkey",
        self_);
  }
  return ret;
}

// The timing of updating the status of the trans result info and the handling
// of each abnormal situation are as follows:
//
// 1. on leader:
//   - Single-machine multi-partitioned transaction, if can_elr=true, the
//     coordinator will register after submitting the log; After the participant
//     receives the commit clear message, modify the trans result info to commit
//   - Single-partitioned transaction, if can_elr=true, after the participant
//     submits sp commit log/sp elr commit log, start to register; wait for the
//     status of the transaction to be determined, then modify the trans result
//     info;
//
// 2. on follower:
//    - Single-Machine multi-partitioned transaction, register during the
//      process of replaying the first redo log; if it is checkpointed, mark the
//      transaction as commit; if the abort log is finally replayed, it means
//      that the transaction was aborted
//    - For single-partitioned transactions, register during the replay of the
//      first sp redo log; during the replay of sp_commit log, mark the
//      transaction as commit; But replaying sp_elr_commit log cannot determine the
//      state, we need to wait for the further confirmation by checkpoint; if sp
//       abort log is finally replayed, mark transaction as aborted
int ObPartTransCtx::register_trans_result_info_(const int state)
{
  int ret = OB_SUCCESS;
  bool registered = false;
  int64_t register_global_trans_version =
      (ObTransVersion::INVALID_TRANS_VERSION == global_trans_version_) ? INT64_MAX : global_trans_version_;
  ObTransResultInfo* trans_result_info_ = result_info_.get_trans_result_info();
  // 1. can_elr = true, trans_result_info = NULL, BUG!!!!
  // 2. can_elr = true, trans_result_info != NULL, ELR txn.
  // 3. can_elr = false, trans_result_info = NULL, non-ELR txn
  // 4. can_elr = false, trans_result_info != NULL, there may exists leader transfer
  if (can_elr_ && OB_ISNULL(trans_result_info_)) {
    TRANS_LOG(ERROR, "unexpected trans result info", K(state), K(*this));
  } else if (!can_elr_ && NULL != trans_result_info_) {
    TRANS_LOG(WARN, "current elr transaction maybe not allowed 1pc", K(*trans_result_info_), K(*this));
  } else {
    // do nothing
  }

  // If the trans_result_info is not NULL, we need register predecessor trans
  // state table to prevent memory leak
  if (OB_ISNULL(trans_result_info_)) {
    // do nothing
  } else if (OB_FAIL(
                 trans_result_info_->init(state, register_global_trans_version, min_log_id_, min_log_ts_, trans_id_))) {
    TRANS_LOG(WARN,
        "trans result info init error",
        K(ret),
        K(state),
        K(register_global_trans_version),
        K(min_log_id_),
        K(*this));
  } else if (OB_FAIL(partition_mgr_->insert_in_TRIM(trans_result_info_, registered))) {
    if (registered) {
      TRANS_LOG(ERROR, "register transaction result info error", K(ret), K(registered), "context", *this);
    } else {
      TRANS_LOG(WARN, "register transaction result info error", K(ret), K(registered), "context", *this);
    }
  } else {
    if (!registered) {
      TRANS_LOG(ERROR, "unexpected registered status error", K(registered), "context", *this);
    }
    // After predecessor registers successfully, the memory reclaim is managed
    // by predecessor state table
    result_info_.set_registered(true);
  }

  return ret;
}

void ObPartTransCtx::check_memtable_ctx_ref()
{
  CtxLockGuard guard(lock_);
  if (is_in_2pc_() && mt_ctx_.get_ref() != 0) {
    TRANS_LOG(ERROR, "memtable_ctx ref not match!!!", K(mt_ctx_.get_ref()), K(*this));
  }
}

/*
void ObPartTransCtx::audit_partition(const bool is_rollback, const sql::stmt::StmtType stmt_type)
{
  int tmp_ret = OB_SUCCESS;

  if (!GCONF.enable_sql_audit) {
    // do nothing
  } else if (is_rollback) {
    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = mt_ctx_.audit_partition(PART_AUDIT_ROLLBACK_SQL, 1)))) {
      TRANS_LOG(WARN, "audit partition error", K(tmp_ret), K(*this));
    } else if (sql::stmt::T_INSERT == stmt_type &&
               OB_UNLIKELY(OB_SUCCESS != (tmp_ret = mt_ctx_.audit_partition(PART_AUDIT_ROLLBACK_INSERT_SQL, 1)))) {
      TRANS_LOG(WARN, "audit partition error", K(tmp_ret), K(*this));
    } else if (sql::stmt::T_UPDATE == stmt_type &&
               OB_UNLIKELY(OB_SUCCESS != (tmp_ret = mt_ctx_.audit_partition(PART_AUDIT_ROLLBACK_UPDATE_SQL, 1)))) {
      TRANS_LOG(WARN, "audit partition error", K(tmp_ret), K(*this));
    } else if (sql::stmt::T_DELETE == stmt_type &&
               OB_UNLIKELY(OB_SUCCESS != (tmp_ret = mt_ctx_.audit_partition(PART_AUDIT_ROLLBACK_DELETE_SQL, 1)))) {
      TRANS_LOG(WARN, "audit partition error", K(tmp_ret), K(*this));
    } else {
      // do ntohing
    }
  } else {
    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = mt_ctx_.audit_partition(PART_AUDIT_SQL, 1)))) {
      TRANS_LOG(WARN, "audit partition error", K(tmp_ret), K(*this));
    } else if (sql::stmt::T_SELECT == stmt_type &&
               OB_UNLIKELY(OB_SUCCESS != (tmp_ret = mt_ctx_.audit_partition(PART_AUDIT_QUERY_SQL, 1)))) {
      TRANS_LOG(WARN, "audit partition error", K(tmp_ret), K(*this));
    } else if (sql::stmt::T_INSERT == stmt_type &&
               OB_UNLIKELY(OB_SUCCESS != (tmp_ret = mt_ctx_.audit_partition(PART_AUDIT_INSERT_SQL, 1)))) {
      TRANS_LOG(WARN, "audit partition error", K(tmp_ret), K(*this));
    } else if (sql::stmt::T_UPDATE == stmt_type &&
               OB_UNLIKELY(OB_SUCCESS != (tmp_ret = mt_ctx_.audit_partition(PART_AUDIT_UPDATE_SQL, 1)))) {
      TRANS_LOG(WARN, "audit partition error", K(tmp_ret), K(*this));
    } else if (sql::stmt::T_DELETE == stmt_type &&
               OB_UNLIKELY(OB_SUCCESS != (tmp_ret = mt_ctx_.audit_partition(PART_AUDIT_DELETE_SQL, 1)))) {
      TRANS_LOG(WARN, "audit partition error", K(tmp_ret), K(*this));
    } else {
      // do ntohing
    }
  }
}*/

int ObPartTransCtx::post_redo_log_sync_to_not_mask_addr_(
    const uint64_t log_id, const int64_t log_ts, const int64_t log_type)
{
  int ret = OB_SUCCESS;
  ObAddrLogIdArray addr_logid_array;

  if (OB_UNLIKELY(log_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(log_id), K(*this));
  } else if (OB_UNLIKELY(!is_dup_table_trans_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected trans type", KR(ret), K(*this));
  } else if (OB_FAIL(dup_table_msg_mask_set_.get_not_mask(addr_logid_array))) {
    TRANS_LOG(WARN, "get not mask addr error", KR(ret), K(*this), K(log_id));
  } else if (OB_FAIL(post_redo_log_sync_request_(addr_logid_array, log_id, log_ts, log_type))) {
    TRANS_LOG(WARN, "post redo log sync request error", KR(ret), K(*this), K(log_id));
  } else {
    if (REACH_TIME_INTERVAL(3 * 1000 * 1000)) {
      TRANS_LOG(INFO, "[DUP TABLE] current trans not mask addr", K(*this), K(addr_logid_array));
    }
  }

  return ret;
}

int ObPartTransCtx::post_redo_log_sync_request_(
    const ObAddrLogIdArray& addr_logid_array, const uint64_t log_id, const int64_t log_ts, const int64_t log_type)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObIDupTableRpc* rpc = NULL;
  ObRedoLogSyncRequestMsg msg;

  if (OB_UNLIKELY(!is_dup_table_trans_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected trans type", KR(ret), K(*this));
  } else if (OB_ISNULL(rpc = trans_service_->get_dup_table_rpc())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "dup table rpc is NULL", KR(ret), K(*this));
  } else {
    for (int i = 0; i < addr_logid_array.count() && OB_SUCC(ret); i++) {
      if (OB_UNLIKELY(log_id != addr_logid_array.at(i).get_log_id())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unexpected redo log sync log id", KR(ret), K(*this), K(log_id), K(addr_logid_array));
      } else if (addr_logid_array.at(i).get_addr() == addr_) {
        ObAddrLogId addr_logid(addr_, log_id);
        if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = dup_table_msg_mask_set_.mask(addr_logid)))) {
          TRANS_LOG(WARN, "dup_table_msg_mask_set mask error", K(tmp_ret), K(msg), K(*this));
        }
      } else if (OB_FAIL(msg.init(self_, addr_logid_array.at(i).get_log_id(), log_ts, log_type, trans_id_))) {
        TRANS_LOG(WARN, "ObRedoLogSyncRequestMsg init error", KR(ret), K(msg));
      } else if (OB_FAIL(msg.set_header(addr_, addr_, addr_logid_array.at(i).get_addr()))) {
        TRANS_LOG(WARN, "ObRedoLogSyncRequestMsg set header error", KR(ret), K(*this));
      } else if (OB_FAIL(rpc->post_redo_log_sync_request(tenant_id_, addr_logid_array.at(i).get_addr(), msg))) {
        TRANS_LOG(WARN, "post redo log sync request error", KR(ret), K(*this), K_(self), K(addr_logid_array.at(i)));
      } else {
        // do nothing
      }
    }
  }

  return ret;
}

int ObPartTransCtx::handle_redo_log_sync_response(const ObRedoLogSyncResponseMsg& msg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (OB_UNLIKELY(for_replay_)) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "transaction is not master", KR(ret), "context", *this);
  } else if (OB_UNLIKELY(!is_dup_table_trans_)) {
    TRANS_LOG(INFO, "normal ctx resv redo log sync response msg, maybe happen when creat duplicate table", K(*this));
  } else if (ObRedoLogSyncResponseStatus::OB_REDO_LOG_SYNC_SUCC == msg.get_status() ||
             ObRedoLogSyncResponseStatus::OB_REDO_LOG_SYNC_LEASE_EXPIRED == msg.get_status()) {
    ObAddrLogId addr_logid(msg.get_addr(), msg.get_log_id());
    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = dup_table_msg_mask_set_.mask(addr_logid)))) {
      if (OB_MASK_SET_NO_NODE != tmp_ret) {
        TRANS_LOG(WARN, "dup_table_msg_mask_set mask error", K(tmp_ret), K(msg), K(*this));
      }
    }
  } else {
    // do nothing
  }

  return ret;
}

bool ObPartTransCtx::is_redo_log_sync_finish() const
{
  CtxLockGuard guard(lock_);
  // For dup table, there exits two scenerio which need callback txn immedidately:
  // 1. The redo sync task finished successfully
  // 2 The txn is in leader_revoke, and need callback immedidately in order to prevent blocking leader_revoke
  return !is_dup_table_trans_ || dup_table_msg_mask_set_.is_all_mask();
}

bool ObPartTransCtx::is_prepare_leader_revoke() const
{
  CtxLockGuard guard(lock_);
  return is_prepare_leader_revoke_;
}

bool ObPartTransCtx::need_to_post_redo_sync_task(const int64_t log_type) const
{
  CtxLockGuard guard(lock_);
  return is_dup_table_trans_ && (OB_LOG_TRANS_REDO == log_type || OB_LOG_SP_TRANS_REDO == log_type ||
                                    OB_LOG_SP_TRANS_COMMIT == log_type || OB_LOG_TRANS_COMMIT == log_type);
}

int ObPartTransCtx::retry_redo_sync_task(
    const uint64_t log_id, const int64_t log_type, const int64_t timestamp, const bool first_gen)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObDupTablePartitionMgr* mgr = NULL;
  const bool is_commit_log = ObStorageLogTypeChecker::is_trans_commit_log(log_type);
  CtxLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_dup_table_trans_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected trans type", K(ret), K(*this));
  } else if (first_gen) {
    update_durable_log_id_ts_(log_type, log_id, timestamp);
    (void)redo_sync_task_->reset();
    if (OB_FAIL(redo_sync_task_->make(
            ObTransRetryTaskType::REDO_SYNC_TASK, trans_id_, self_, log_id, log_type, timestamp))) {
      TRANS_LOG(WARN, "make ObDupTableRedoSyncTask error", K(ret), K(*this));
      // In terms of commit log, we only need to wait gts cache push over, and
      // we can ask for it
    } else if (OB_FAIL(redo_sync_task_->set_retry_interval_us(
                   is_commit_log ? 0 : REDO_SYNC_TASK_RETRY_INTERVAL_US, REDO_SYNC_TASK_RETRY_INTERVAL_US))) {
      TRANS_LOG(WARN, "set retry interval us error", K(ret), K(*this));
    } else if (OB_FAIL(trans_service_->push(redo_sync_task_))) {
      // TODO(): If the push fails, we need retry
      TRANS_LOG(WARN, "push task error", K(ret), K(*this), K(log_id));
    } else {
      // mark the redo log sync task has began
      dup_table_syncing_log_id_ = log_id;
      dup_table_syncing_log_ts_ = timestamp;
      REC_TRANS_TRACE_EXT(tlog_, alloc_redo_log_sync_task, OB_ID(arg1), dup_table_lease_addrs_.count());
    }
  } else {
    // do nothing
  }

  if (OB_SUCC(ret)) {
    // 1. Firstly, we need generate the duplicate replica array:
    // There exits two scenerio when needs to initialize the mask_set:
    // - Newly created task
    // - Task has retried more than one lease timeout.
    if (first_gen || !redo_sync_task_->is_mask_set_ready() ||
        ObTimeUtility::current_time() - redo_sync_task_->get_last_generate_mask_set_ts() >=
            ObTransService::DEFAULT_DUP_TABLE_LEASE_TIMEOUT_INTERVAL_US) {
      // NB: the order here is important, we need push up the cur_log_id on
      // dup_table_partition_mgr before obtaining the mask set. It is used to
      // guarantee if the newly onlined replica is not synced in the mask set it
      // must synced to the newest cur_log_id before provide service. Otherwise
      // newly onlined replica will miss some redo need be synced
      (void)redo_sync_task_->set_mask_set_ready(false);
      if (OB_ISNULL(mgr = partition_mgr_->get_dup_table_partition_mgr())) {
        tmp_ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "dup table partition mgr is null", K(tmp_ret), K(*this));
      } else if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = mgr->update_cur_log_id(log_id)))) {
        TRANS_LOG(WARN, "update cur log id error", K(tmp_ret), K(*this), K(log_id));
      } else if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = mgr->generate_redo_log_sync_set(
                                                dup_table_msg_mask_set_, dup_table_lease_addrs_, log_id)))) {
        TRANS_LOG(WARN, "generate redo log sync set error", K(tmp_ret), K(*this), K(log_id));
      } else {
        (void)redo_sync_task_->set_last_generate_mask_set_ts(ObTimeUtility::current_time());
        (void)redo_sync_task_->set_mask_set_ready(true);
      }
    }
    // 2. Then, send the redo log sync request
    if (OB_SUCCESS == tmp_ret) {
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = post_redo_log_sync_to_not_mask_addr_(log_id, timestamp, log_type)))) {
        TRANS_LOG(WARN, "post redo log sync to not mask addr error", K(tmp_ret), K(*this), K(log_id), K(log_type));
      } else {
        TRANS_LOG(DEBUG, "post redo log sync to not mask addr success", K_(trans_id));
      }
    }
  }

  REC_TRANS_TRACE_EXT(tlog_,
      retry_redo_log_sync_task,
      OB_ID(arg1),
      dup_table_lease_addrs_.count(),
      OB_ID(log_id),
      log_id,
      OB_ID(log_type),
      log_type,
      OB_ID(arg2),
      first_gen,
      OB_ID(ret),
      ret,
      OB_ID(arg3),
      tmp_ret);
  return ret;
}

bool ObPartTransCtx::is_redo_log_replayed(const uint64_t log_id)
{
  bool ret = false;
  CtxLockGuard guard(lock_);
  if (!for_replay_) {
    ret = true;
  } else if (prev_redo_log_ids_.count() > 0) {
    ret = prev_redo_log_ids_.at(prev_redo_log_ids_.count() - 1) >= log_id;
  } else {
    // do nothing
  }

  return ret;
}

int ObPartTransCtx::post_redo_log_sync_response_(const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  ObAddr dst;
  ObRedoLogSyncResponseMsg resp_msg;
  ObIDupTableRpc* dup_table_rpc = NULL;

  if (OB_ISNULL(dup_table_rpc = trans_service_->get_dup_table_rpc())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected dup table rpc", K(ret), K(*this));
  } else if (OB_FAIL(location_adapter_->nonblock_get_strong_leader(self_, dst))) {
    TRANS_LOG(WARN, "nonblock get leader error", K(ret), K_(self), K_(trans_id));
  } else if (OB_FAIL(
                 resp_msg.init(self_, log_id, trans_id_, addr_, ObRedoLogSyncResponseStatus::OB_REDO_LOG_SYNC_SUCC))) {
    TRANS_LOG(WARN, "ObRedoLogSyncResponseMsg init error", KR(ret), K(resp_msg), K(*this));
  } else if (OB_FAIL(resp_msg.set_header(addr_, addr_, dst))) {
    TRANS_LOG(WARN, "ObRedoLogSyncResponseMsg set header error", KR(ret), K(resp_msg));
  } else if (OB_FAIL(dup_table_rpc->post_redo_log_sync_response(self_.get_tenant_id(), dst, resp_msg))) {
    TRANS_LOG(WARN, "post redo log sync response error", KR(ret), K(resp_msg));
  } else {
    TRANS_LOG(DEBUG, "post redo log sync response success", K(resp_msg));
  }

  return ret;
}

void ObPartTransCtx::update_max_submitted_log_timestamp_(const int64_t cur_log_timestamp)
{
  if (cur_log_timestamp > max_submitted_log_timestamp_) {
    max_submitted_log_timestamp_ = cur_log_timestamp;
  }
}

int ObPartTransCtx::calculate_trans_ctx_cost(uint64_t& cost) const
{
  int ret = OB_SUCCESS;
  const ObIMemtableCtx* mem_ctx = static_cast<const ObIMemtableCtx*>(&mt_ctx_);
  const ObIMvccCtx* mvcc_ctx = static_cast<const ObIMvccCtx*>(mem_ctx);

  if (is_local_trans_) {
    cost = mvcc_ctx->get_callback_list_length();
  } else {
    cost = 2 * mvcc_ctx->get_callback_list_length();
  }

  return ret;
}

int ObPartTransCtx::handle_savepoint_rollback_request(
    const int64_t sql_no, const int64_t cur_sql_no, const bool need_response)
{
  int ret = OB_SUCCESS;
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (OB_UNLIKELY(for_replay_)) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "transaction is not master", K(ret), "context", *this);
  } else if (OB_FAIL(handle_savepoint_rollback_request_(sql_no, cur_sql_no, need_response))) {
    TRANS_LOG(WARN, "handle savepoint rollback request error", K(ret), K(sql_no), K(cur_sql_no), K(*this));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartTransCtx::handle_savepoint_rollback_request_(
    const int64_t sql_no, const int64_t cur_sql_no, const bool need_response)
{
  int ret = OB_SUCCESS;
  bool real_need_response = need_response;

  if (stmt_info_.stmt_expired(cur_sql_no, true)) {
    ret = OB_TRANS_SQL_SEQUENCE_ILLEGAL;
    TRANS_LOG(WARN, "sql sequence is illegal", "context", *this, K(cur_sql_no), K(sql_no));
  } else if (is_logging_()) {
    ret = OB_TRANS_STMT_NEED_RETRY;
    real_need_response = false;
    TRANS_LOG(INFO, "transaction is logging", K(ret), K(*this));
  } else if (is_changing_leader_) {
    ret = OB_NOT_MASTER;
    real_need_response = false;
    TRANS_LOG(WARN, "transaction is preparing changing leader", K(ret), K(*this));
  } else if (!stmt_info_.is_task_match(sql_no + 1)) {
    // Sql is not finished, we return OB_TRANS_STMT_NEED_RETRY and let scheduler retry
    real_need_response = false;
    ret = OB_TRANS_STMT_NEED_RETRY;
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      TRANS_LOG(WARN, "stmt info not match, need retry rollback", K(ret), "context", *this);
    }
  } else if (OB_FAIL(rollback_to_(sql_no))) {
    TRANS_LOG(WARN, "rollback to sql no error", K(ret), K(sql_no), K(*this));
  } else {
    stmt_info_.rollback_to(cur_sql_no, sql_no + 1);
    try_restore_read_snapshot();
  }

  if (real_need_response) {
    if (OB_FAIL(post_stmt_response_(OB_TRANS_SAVEPOINT_ROLLBACK_RESPONSE, sql_no, ret))) {
      TRANS_LOG(WARN, "post stmt response error", K(ret), K(*this));
    }
  }

  need_print_trace_log_ = true;
  REC_TRANS_TRACE_EXT(
      tlog_, rollback_savepoint, OB_ID(ret), ret, OB_ID(rollback_to), sql_no, OB_ID(rollback_from), cur_sql_no);
  return ret;
}

int ObPartTransCtx::half_stmt_commit()
{
  int ret = OB_SUCCESS;
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (is_inited_ && !for_replay_ && !is_exiting_) {
    mt_ctx_.half_stmt_commit();
  }

  return ret;
}

// Now only the dump transaction table will be called, use it to pay attention
int ObPartTransCtx::get_trans_table_status_info_(const int64_t log_ts, ObTransTableStatusInfo& info)
{
  int ret = OB_SUCCESS;
  uint64_t checksum = 0;

  if (OB_UNLIKELY(0 == log_ts)) {
    TRANS_LOG(WARN, "log ts is invalid");
  } else {
    ObTransTableStatusType status = mt_ctx_.get_trans_table_status();
    int64_t trans_version =
        (status == ObTransTableStatusType::COMMIT) ? mt_ctx_.get_commit_version() : mt_ctx_.get_trans_version();
    int64_t terminate_log_ts = INT64_MAX;
    if (ObTransTableStatusType::COMMIT == status || ObTransTableStatusType::ABORT == status) {
      terminate_log_ts = end_log_ts_;
    }

    if (OB_FAIL(update_and_get_trans_table_with_minor_freeze(log_ts, checksum))) {
      TRANS_LOG(WARN, "update and get trans table failed", K(log_ts), KP(this));
    } else if (OB_FAIL(info.set(
                   status, trans_version, undo_status_, terminate_log_ts, checksum, mt_ctx_.get_checksum_log_ts()))) {
      TRANS_LOG(WARN, "get trans table status info error", K(ret), K(*this));
    }
  }

  return ret;
}

int ObPartTransCtx::recover_from_trans_sstable_durable_ctx_info(ObTransSSTableDurableCtxInfo& ctx_info)
{
  int ret = OB_SUCCESS;
  CtxTransTableLockGuard guard(lock_, trans_table_seqlock_);

  if (OB_FAIL(participants_.assign(ctx_info.participants_))) {
    TRANS_LOG(WARN, "assign participants failed", K(ret), K(*this));
  } else if (OB_FAIL(prev_redo_log_ids_.assign(ctx_info.prev_redo_log_ids_))) {
    TRANS_LOG(WARN, "assign prev redo log ids failed", K(ret), K(*this));
  } else if (OB_FAIL(partition_log_info_arr_.assign(ctx_info.partition_log_info_arr_))) {
    TRANS_LOG(WARN, "assign partition log info arr failed", K(ret), K(*this));
  } else if (OB_FAIL(ctx_dependency_wrap_.prev_trans_arr_assign(ctx_info.prev_trans_arr_))) {
    TRANS_LOG(WARN, "assign prev trans arr failed", K(ret), K(*this));
  } else {
    set_for_replay(true);
    scheduler_ = ctx_info.scheduler_;
    coordinator_ = ctx_info.coordinator_;
    status_ = ctx_info.prepare_status_;
    set_app_trace_id_(ctx_info.app_trace_id_str_);
    max_durable_log_ts_ = ctx_info.max_durable_log_ts_;
    max_durable_sql_no_ = ctx_info.max_durable_sql_no_;
    global_trans_version_ = ctx_info.global_trans_version_;
    commit_log_checksum_ = ctx_info.commit_log_checksum_;
    state_.set_state(ctx_info.state_);
    state_.set_prepare_version(ctx_info.prepare_version_);
    trans_type_ = ctx_info.trans_type_;

    elr_prepared_state_ = ctx_info.elr_prepared_state_;
    is_dup_table_trans_ = ctx_info.is_dup_table_trans_;
    redo_log_no_ = ctx_info.redo_log_no_;
    mutator_log_no_ = ctx_info.mutator_log_no_;
    stmt_info_ = ctx_info.stmt_info_;
    min_log_ts_ = ctx_info.min_log_ts_;
    min_log_id_ = ctx_info.min_log_id_;
    sp_user_request_ = ctx_info.sp_user_request_;
    need_checksum_ = ctx_info.need_checksum_;
    prepare_log_id_ = ctx_info.prepare_log_id_;
    prepare_log_timestamp_ = ctx_info.prepare_log_timestamp_;
    clear_log_base_ts_ = ctx_info.clear_log_base_ts_;
    // for record log
    prev_checkpoint_id_ = ctx_info.prev_checkpoint_id_;

    (void)mark_dirty_trans();

    if (Ob2PCState::CLEAR == state_.get_state() || ObSpState::COMMIT == state_.get_state() ||
        ObSpState::ABORT == state_.get_state() || ObRunningState::ABORT == state_.get_state()) {
      set_exiting_();
    }

    set_trans_table_status_info_(ctx_info.trans_table_info_);
    REC_TRANS_TRACE_EXT(
        tlog_, reboot, OB_ID(ret), ret, OB_ID(arg1), is_dirty_, OB_ID(arg2), is_exiting_, OB_ID(state), get_state_());
  }

  return ret;
}

int64_t ObPartTransCtx::decide_sstable_trans_state_()
{
  int64_t ret_state;
  // Special treatment for memory state
  if (Ob2PCState::PRE_PREPARE == state_.get_state()) {
    ret_state = Ob2PCState::INIT;
  } else if (Ob2PCState::PRE_COMMIT == state_.get_state()) {
    ret_state = Ob2PCState::PREPARE;
  } else {
    ret_state = state_.get_state();
  }
  return ret_state;
}

int ObPartTransCtx::get_trans_sstable_durable_ctx_info(const int64_t log_ts, ObTransSSTableDurableCtxInfo& info)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  ObElrTransArrGuard prev_trans_guard;

  if (is_in_redo_with_prepare_) {
    ret = OB_EAGAIN;
    TRANS_LOG(WARN, "is_in_redo_with_preapre, cannot merge", K(ret), K(*this));
  } else if (OB_FAIL(get_trans_table_status_info_(log_ts, info.trans_table_info_))) {
    TRANS_LOG(WARN, "get trans table status", K(ret), K(*this));
  } else if (OB_FAIL(info.participants_.assign(participants_))) {
    TRANS_LOG(WARN, "assign participants failed", K(ret), K(*this));
  } else if (OB_FAIL(info.prev_redo_log_ids_.assign(prev_redo_log_ids_))) {
    TRANS_LOG(WARN, "assign prev redp log ids failed", K(ret), K(*this));
  } else if (OB_FAIL(info.partition_log_info_arr_.assign(partition_log_info_arr_))) {
    TRANS_LOG(WARN, "assign partition log info arr failed", K(ret), K(*this));
  } else if (OB_FAIL(ctx_dependency_wrap_.get_prev_trans_arr_guard(prev_trans_guard))) {
    TRANS_LOG(WARN, "get prev trans arr guard error", KR(ret), K(*this));
  } else if (OB_FAIL(info.prev_trans_arr_.assign(prev_trans_guard.get_elr_trans_arr()))) {
    TRANS_LOG(WARN, "assign prev trans arr failed", K(ret), K(*this));
  } else {
    info.partition_ = self_;
    info.trans_param_ = trans_param_;
    info.tenant_id_ = tenant_id_;
    info.trans_expired_time_ = trans_expired_time_;
    info.cluster_id_ = cluster_id_;
    info.scheduler_ = scheduler_;
    info.coordinator_ = coordinator_;
    info.prepare_status_ = status_;
    info.app_trace_id_str_ = trace_info_.get_app_trace_id();
    info.can_elr_ = can_elr_;
    info.max_durable_log_ts_ = max_durable_log_ts_;
    info.max_durable_sql_no_ = max_durable_sql_no_;
    info.global_trans_version_ = global_trans_version_;
    info.commit_log_checksum_ = commit_log_checksum_;
    info.state_ = decide_sstable_trans_state_();
    info.prepare_version_ = state_.get_prepare_version();
    info.trans_type_ = trans_type_;
    info.elr_prepared_state_ = elr_prepared_state_;
    info.is_dup_table_trans_ = is_dup_table_trans_;
    info.redo_log_no_ = redo_log_no_;
    info.mutator_log_no_ = mutator_log_no_;
    info.stmt_info_ = stmt_info_;
    info.min_log_ts_ = min_log_ts_;
    info.min_log_id_ = min_log_id_;
    info.sp_user_request_ = sp_user_request_;
    info.need_checksum_ = need_checksum_;
    info.prepare_log_id_ = prepare_log_id_;
    info.prepare_log_timestamp_ = prepare_log_timestamp_;
    info.clear_log_base_ts_ = clear_log_base_ts_;
    info.prev_checkpoint_id_ = prev_checkpoint_id_;
    TRANS_LOG(INFO, "trans table status when dump trans table", K(*this), K(info), K(log_ts));
  }

  return ret;
}

int ObPartTransCtx::get_trans_table_status_info(ObTransTableStatusInfo& trans_table_status_info)
{
  int ret = OB_SUCCESS;
  uint64_t seq = 0;

  do {
    seq = trans_table_seqlock_.read_begin();

    ObTransTableStatusType status = mt_ctx_.get_trans_table_status();
    int64_t trans_version =
        (status == ObTransTableStatusType::COMMIT) ? mt_ctx_.get_commit_version() : mt_ctx_.get_trans_version();
    int64_t terminate_log_ts = INT64_MAX;
    if (ObTransTableStatusType::COMMIT == status || ObTransTableStatusType::ABORT == status) {
      terminate_log_ts = end_log_ts_;
    }

    if (OB_FAIL(trans_table_status_info.set(status, trans_version, undo_status_, terminate_log_ts))) {
      TRANS_LOG(WARN, "get trans table status info error", K(ret), K(*this));
    }
  } while (trans_table_seqlock_.read_retry(seq));

  return ret;
}

int ObPartTransCtx::get_trans_state_and_version_without_lock(ObTransStatusInfo& trans_info)
{
  int ret = OB_SUCCESS;

  trans_info.status_ = mt_ctx_.get_trans_table_status();
  trans_info.trans_version_ = (trans_info.status_ == ObTransTableStatusType::COMMIT) ? mt_ctx_.get_commit_version()
                                                                                     : mt_ctx_.get_trans_version();
  if (trans_info.status_ != ObTransTableStatusType::ABORT && OB_FAIL(undo_status_.deep_copy(trans_info.undo_status_))) {
    TRANS_LOG(WARN, "deep copy undo status fail", K(*this), K(ret));
  }

  if (trans_info.status_ == ObTransTableStatusType::COMMIT || trans_info.status_ == ObTransTableStatusType::ABORT) {
    trans_info.end_log_ts_ = ATOMIC_LOAD(&end_log_ts_);
  }

  trans_info.start_log_ts_ = ATOMIC_LOAD(&min_log_ts_);
  return ret;
}

int ObPartTransCtx::set_trans_table_status_info_(const ObTransTableStatusInfo& trans_table_status_info)
{
  int ret = OB_SUCCESS;
  ObTransTableStatusType status;
  int64_t trans_version;
  trans_table_status_info.get_transaction_status(status, trans_version);

  mt_ctx_.set_trans_table_status(status);
  if (ObTransTableStatusType::COMMIT == status) {
    mt_ctx_.set_commit_version(trans_version);
  } else {
    mt_ctx_.set_trans_version(trans_version);
  }

  if (ObTransTableStatusType::COMMIT == status || ObTransTableStatusType::ABORT == status) {
    end_log_ts_ = trans_table_status_info.get_terminate_log_ts();
  }

  if (OB_FAIL(mt_ctx_.update_batch_checksum(
          trans_table_status_info.get_checksum(), trans_table_status_info.get_checksum_log_ts()))) {
    TRANS_LOG(ERROR, "failed to update batch checksum", K(*this));
  } else if (OB_FAIL(undo_status_.set(trans_table_status_info.get_undo_status()))) {
    TRANS_LOG(WARN, "failed to set undo status", K(*this));
  } else {
    uint64_t res = trans_table_status_info.get_checksum();

    FLOG_INFO("set trans table status info after reboot",
        K(res),
        K(trans_table_status_info.get_checksum_log_ts()),
        K(*this),
        K(trans_table_status_info));
  }

  return ret;
}

int ObPartTransCtx::remove_callback_for_uncommited_txn(ObMemtable* mt)
{
  int ret = OB_SUCCESS;
  // The lock in part_trans_ctx and memtable_ctx are all necessary
  CtxLockGuard guard(lock_);
  int64_t cnt = 0;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(mt)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "memtable is NULL", K(mt));
  } else if (OB_UNLIKELY(is_exiting_)) {
  } else if (OB_FAIL(mt_ctx_.remove_callback_for_uncommited_txn(mt, cnt))) {
    TRANS_LOG(WARN, "fail to remove callback for uncommitted txn", K(ret), K(mt_ctx_));
  } else {
    REC_TRANS_TRACE_EXT(tlog_,
        remove_callback_for_uncommitted_txn,
        OB_ID(ret),
        ret,
        OB_ID(total_count),
        cnt,
        OB_ID(memtable),
        (int64_t)mt);
  }

  return ret;
}

int ObPartTransCtx::remove_mem_ctx_for_trans_ctx(ObMemtable* mt)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(mt)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "memtable is NULL", K(mt));
  } else if (OB_UNLIKELY(is_exiting_)) {
  } else if (OB_FAIL(mt_ctx_.remove_mem_ctx_for_trans_ctx(mt))) {
    TRANS_LOG(WARN, "fail to remove mem for frozen mt", K(ret), K(mt_ctx_));
  }
  return ret;
}

int ObPartTransCtx::get_trans_mem_total_size(int64_t& size) const
{
  int ret = OB_SUCCESS;
  size = mt_ctx_.get_trans_mem_total_size();
  return ret;
}

int ObPartTransCtx::mark_frozen_data(
    const ObMemtable* const frozen_memtable, const ObMemtable* const active_memtable, int64_t& cb_cnt)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  bool marked = false;
  cb_cnt = 0;

  if (OB_FAIL(mt_ctx_.mark_frozen_data(frozen_memtable, active_memtable, marked, cb_cnt))) {
    TRANS_LOG(WARN, "mark frozen data failed", KR(ret), K(*this));
  }

  if (marked) {
    if (mark_dirty_trans()) {
      TRANS_LOG(INFO, "mark dirty", K(*this), K(cb_cnt), K(frozen_memtable), K(active_memtable));
      REC_TRANS_TRACE_EXT(tlog_,
          mark_frozen_data,
          OB_ID(ret),
          ret,
          OB_ID(total_count),
          cb_cnt,
          OB_ID(arg1),
          (int64_t)frozen_memtable,
          OB_ID(arg2),
          (int64_t)active_memtable);
    }
  } else {
    int freeze_ts = frozen_memtable->get_freeze_log_ts();
    if (min_log_ts_ <= freeze_ts && freeze_ts < end_log_ts_) {
      mark_dirty_trans();
      TRANS_LOG(INFO, "mark dirty crossed freeze", K(*this), K(*frozen_memtable));
    }
  }

  if (cb_cnt > 0 && !for_replay_ && !batch_commit_trans_) {
    int tmp_ret = OB_SUCCESS;
    bool need_state_log = false;
    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = submit_log_incrementally_(false, /*need state log*/
                                       need_state_log)))) {
      TRANS_LOG(WARN, "fail to submit log", KR(tmp_ret));
    }
  }

  return ret;
}

int ObPartTransCtx::submit_log_for_split(bool &log_finished)
{
  int ret = OB_SUCCESS;
  bool has_redo_log = false;
  log_finished = false;
  CtxLockGuard guard(lock_);

  if (batch_commit_trans_) {
    log_finished = true;
  } else {
    if (0 == submit_log_count_) {
      if (OB_FAIL(submit_log_incrementally_(false /*has state log*/, has_redo_log))) {
        TRANS_LOG(WARN, "fail to submit log", K(ret), K(*this));
      } else if (!has_redo_log) {
        log_finished = true;
      }
    }
  }

  return ret;
}

int ObPartTransCtx::submit_log_incrementally_(const bool need_state_log)
{
  bool unused = false;
  return submit_log_incrementally_(need_state_log, unused);
}

int ObPartTransCtx::submit_log_incrementally_(const bool need_state_log, bool& has_redo_log)
{
  int ret = OB_SUCCESS;
  int state = ObTransResultState::INVALID;
  ObStorageLogType log_type = ObStorageLogType::OB_LOG_UNKNOWN;

  if (OB_FAIL(ctx_dependency_wrap_.get_prev_trans_arr_result(state))) {
    TRANS_LOG(WARN, "get prev trans arr result error", K(ret), "context", *this);
  } else if (ObTransResultState::is_commit(state)) {
    ctx_dependency_wrap_.reset_prev_trans_arr();
  } else if (ObTransResultState::is_abort(state)) {
    set_status_(OB_TRANS_ROLLBACKED);
    need_print_trace_log_ = true;
    ctx_dependency_wrap_.reset_prev_trans_arr();
    TRANS_LOG(WARN, "current transaction need to rollback because of prev abort trans", "context", *this);
  } else {
    // do nothing
  }

  // If the predecessor fails, we should not write mutator again
  if (OB_SUCCESS == get_status_()) {
    if (need_state_log) {
      log_type = ObStorageLogType::OB_LOG_MUTATOR_WITH_STATE;
    } else {
      log_type = ObStorageLogType::OB_LOG_MUTATOR;
    }
    if (submit_log_count_ > 0) {
    } else if (OB_FAIL(submit_log_async_(log_type, has_redo_log))) {
      TRANS_LOG(WARN, "submit trans mutator log failed", KR(ret), "context", *this);
    }

    if (OB_FAIL(ret)) {
      TRANS_LOG(WARN, "fail to submit log incrementally", KR(ret), "context", *this);
    } else {
      TRANS_LOG(DEBUG, "succ to submit log incrementally", "context", *this);
    }
  }

  return ret;
}

int ObPartTransCtx::rollback_to_(const int32_t sql_no)
{
  int ret = OB_SUCCESS;

  int32_t curr_sql_no = stmt_info_.get_sql_no();
  bool need_write_log = max_durable_sql_no_ > sql_no;
  bool has_calc_checksum = false;
  uint64_t checksum = 0;
  int64_t checksum_log_ts = 0;

  if (OB_FAIL(mt_ctx_.rollback_to(sql_no,
          false, /*for replay*/
          need_write_log,
          max_durable_log_ts_,
          has_calc_checksum,
          checksum,
          checksum_log_ts))) {
    TRANS_LOG(WARN, "rollback to sql no error", K(ret), K(sql_no), K(need_write_log), K(max_durable_sql_no_), K(*this));
  } else if (need_write_log) {
    bool has_redo_log = false;
    if (OB_FAIL(submit_log_sync_(OB_LOG_MUTATOR, has_redo_log))) {
      TRANS_LOG(WARN, "submit rollback log failed", KR(ret), K_(trans_id));

      // In the case of log submission failure, fall back to the original behavior,
      //  which should rarely happen
      if (OB_FAIL(mt_ctx_.truncate_to(sql_no))) {
        TRANS_LOG(WARN, "rollback rest of callbacks error", K(ret), K(sql_no), K(*this));
      } else if (OB_FAIL(mt_ctx_.rollback_to(sql_no,
                     false, /*for_replay*/
                     need_write_log,
                     max_durable_log_ts_,
                     has_calc_checksum,
                     checksum,
                     checksum_log_ts))) {
        TRANS_LOG(
            WARN, "rollback to sql no error", K(ret), K(sql_no), K(need_write_log), K(max_durable_sql_no_), K(*this));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (!has_redo_log || !mt_ctx_.is_all_redo_submitted()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "rollback log has not been submitted", K(ret), K(has_redo_log), K(*this));
    } else if (OB_FAIL(mt_ctx_.truncate_to(sql_no))) {
      TRANS_LOG(WARN, "rollback rest of callbacks error", K(ret), K(sql_no), K(*this));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(calc_serialize_size_and_set_undo_(sql_no, curr_sql_no))) {
        TRANS_LOG(WARN, "record rollback action failed", K(ret), K(sql_no), K(curr_sql_no));
      }
    }
  }

  if (OB_SUCC(ret) && has_calc_checksum) {
    REC_TRANS_TRACE_EXT(tlog_,
        calc_checksum_by_rollback,
        OB_ID(ret),
        ret,
        OB_ID(checksum),
        checksum,
        OB_ID(checksum_log_ts),
        checksum_log_ts);
  }

  return ret;
}

int ObPartTransCtx::submit_log_sync_(const int64_t log_type, bool& has_redo_log)
{
  int ret = OB_SUCCESS;
  int64_t abs_timeout = std::max(mt_ctx_.get_abs_expired_time(),
      ObTimeUtility::current_time() + 100000);  // at least 100ms
  bool need_retry = false;
  int64_t retry_cnt = 0;

  common::ObTimeGuard timeguard("submit_log_sync", 80 * 1000);

  do {
    need_retry = false;
    if (OB_FAIL(submit_log_impl_(log_type, false, false, has_redo_log))) {
      if (ObClogAdapter::need_retry(ret) && ObTimeUtility::current_time() <= abs_timeout) {
        need_retry = true;
        retry_cnt++;
        PAUSE();
      }
    }

    if (need_retry && retry_cnt % 1000 == 0) {
      TRANS_LOG(INFO, "retry submit log", K(ret), K(retry_cnt), K(abs_timeout));
    }
  } while (need_retry);

  if (OB_FAIL(ret)) {
    TRANS_LOG(INFO, "try sbumit log sync failed", K(ret), K(retry_cnt), K(abs_timeout), K(*this));
  }

  return ret;
}

int ObPartTransCtx::replay_rollback_to(const int64_t sql_no, const int64_t log_ts)
{
  int ret = OB_SUCCESS;
  const int64_t max_sql_no = max_durable_sql_no_;
  bool has_calc_checksum = false;
  uint64_t checksum = 0;
  int64_t checksum_log_ts = 0;
  // TODO: There may be deadlock if hold the lock

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtx not inited");
    ret = OB_NOT_INIT;
  } else if (INT64_MAX == log_ts) {
    TRANS_LOG(WARN, "log ts is not invalid", K(log_ts));
    ret = OB_INVALID_ARGUMENT;
  } else if (log_ts > max_durable_log_ts_ && OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "transaction is exiting", "context", *this);
    ret = OB_TRANS_IS_EXITING;
  } else if (OB_UNLIKELY(!for_replay_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "transaction is master", K(ret), "context", *this);
  } else if (OB_FAIL(mt_ctx_.rollback_to(sql_no,
                 true,  /*for_replay*/
                 false, /*need_write_log*/
                 log_ts,
                 has_calc_checksum,
                 checksum,
                 checksum_log_ts))) {
    TRANS_LOG(WARN, "rollback to sql no error", K(ret), K(sql_no), K(*this));
  } else if (max_sql_no != 0 && OB_FAIL(undo_status_.undo(sql_no, max_sql_no))) {
    // If the callback list is empty, the max_sql no may is 0, and we donnot
    // need to record the undo array
    TRANS_LOG(WARN, "record rollback action error", K(ret), K(sql_no), K(max_sql_no), K(*this));
  } else {
    // do nothing
  }

  if (OB_SUCC(ret) && has_calc_checksum) {
    REC_TRANS_TRACE_EXT(tlog_,
        calc_checksum_by_rollback,
        OB_ID(ret),
        ret,
        OB_ID(checksum),
        checksum,
        OB_ID(checksum_log_ts),
        checksum_log_ts);
  }

  return ret;
}

int ObPartTransCtx::lock_for_read_(const ObTransStatusInfo& trans_info, const ObLockForReadArg& lock_for_read_arg,
    bool& can_read, int64_t& trans_version, bool& is_determined_state)
{
  int ret = OB_SUCCESS;
  can_read = false;
  trans_version = OB_INVALID_VERSION;
  is_determined_state = false;
  int64_t snapshot_version = lock_for_read_arg.snapshot_version_;
  const ObTransID& read_trans_id = lock_for_read_arg.read_trans_id_;
  const ObTransID& data_trans_id = lock_for_read_arg.data_trans_id_;
  int32_t read_sql_sequence = lock_for_read_arg.read_sql_sequence_;
  int32_t data_sql_sequence = lock_for_read_arg.data_sql_sequence_;
  bool read_latest = lock_for_read_arg.read_latest_;
  bool is_safe_read = lock_for_read_arg.read_ctx_.get_is_safe_read();

  switch (trans_info.status_) {
    case ObTransTableStatusType::COMMIT: {
      can_read = !trans_info.undo_status_.is_contain(data_sql_sequence);
      trans_version = trans_info.trans_version_;
      is_determined_state = true;
      break;
    }
    case ObTransTableStatusType::RUNNING: {
      if (read_trans_id == data_trans_id) {
        bool tmp_can_read = false;
        if (data_sql_sequence < read_sql_sequence) {
          tmp_can_read = true;
        } else if (read_latest) {
          // Data in the statement, unreadable when querying,
          // readable when checking exist
          tmp_can_read = true;
        } else {
          tmp_can_read = false;
        }
        can_read = tmp_can_read && !trans_info.undo_status_.is_contain(data_sql_sequence);
        trans_version = 0;
      } else {
        if (is_safe_read) {
          can_read = false;
          trans_version = 0;
        } else if (trans_info.trans_version_ > snapshot_version) {
          can_read = false;
          trans_version = 0;
        } else {
          ret = OB_ERR_SHARED_LOCK_CONFLICT;
          if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
            TRANS_LOG(WARN, "lock_for_read need retry", K(ret), K(trans_info), K(lock_for_read_arg));
          }
        }
      }
      is_determined_state = false;
      break;
    }
    case ObTransTableStatusType::ABORT: {
      can_read = false;
      trans_version = 0;
      is_determined_state = true;
      break;
    }
  }

  return ret;
}

int ObPartTransCtx::lock_for_read(
    const ObLockForReadArg& lock_for_read_arg, bool& can_read, int64_t& trans_version, bool& is_determined_state)
{
  int ret = OB_ERR_SHARED_LOCK_CONFLICT;
  ObTransStatusInfo trans_info;
  const int64_t MAX_SLEEP_US = 1000;
  const int64_t lock_wait_start_ts = ObTimeUtility::current_time();
  memtable::ObMemtableCtx &read_ctx = lock_for_read_arg.read_ctx_;

  for (int32_t i = 0; OB_ERR_SHARED_LOCK_CONFLICT == ret; i++) {
    // leave this check here to make sure no lock is held
    int64_t abs_stmt_timeout =
        read_ctx.get_trx_lock_timeout() < 0
            ? read_ctx.get_abs_expired_time()
            : MIN(lock_wait_start_ts + read_ctx.get_trx_lock_timeout(), read_ctx.get_abs_expired_time());

    if (OB_FAIL(get_trans_state_and_version_without_lock(trans_info))) {
      TRANS_LOG(WARN, "failed to get trans table status", K(ret));
    } else if (OB_FAIL(lock_for_read_(trans_info, lock_for_read_arg, can_read, trans_version, is_determined_state))) {
      TRANS_LOG(DEBUG, "trans status info is", K(trans_info));

      int64_t wait_lock_timeout = 2000;
      int64_t abs_lock_timeout = ::oceanbase::common::ObTimeUtility::current_time() + wait_lock_timeout;
      if (abs_stmt_timeout > 0) {
        abs_lock_timeout = min(abs_stmt_timeout, abs_lock_timeout);
      }
      // leave timeout checkout here to make sure try lock atleast once
      if (abs_lock_timeout >= abs_stmt_timeout) {
        ret = OB_ERR_SHARED_LOCK_CONFLICT;
        break;
      } else if (i < 10) {
        PAUSE();
      } else {
        usleep((i < MAX_SLEEP_US ? i : MAX_SLEEP_US));
      }
    }
  }

  return ret;
}

int ObPartTransCtx::get_transaction_status_with_log_ts(
    const int64_t log_ts, ObTransTableStatusType& status, int64_t& trans_version)
{
  int ret = OB_SUCCESS;
  ObTransStatusInfo trans_info;

  if (OB_FAIL(get_trans_state_and_version_without_lock(trans_info))) {
    TRANS_LOG(WARN, "failed to get trans table status", K(ret));
  } else {
    // return the transaction status according to the merge log ts.
    // the detailed document is available as follows.
    if (ObTransTableStatusType::RUNNING == trans_info.status_) {
      status = ObTransTableStatusType::RUNNING;
      trans_version = INT64_MAX;
      TRANS_LOG(TRACE, "get_trans_status in running", K(ret), K(log_ts), K(trans_info));
    } else if (log_ts < trans_info.end_log_ts_) {
      status = ObTransTableStatusType::RUNNING;
      trans_version = INT64_MAX;
    } else if (ObTransTableStatusType::COMMIT == trans_info.status_) {
      status = ObTransTableStatusType::COMMIT;
      trans_version = trans_info.trans_version_;
    } else if (ObTransTableStatusType::ABORT == trans_info.status_) {
      status = ObTransTableStatusType::ABORT;
      trans_version = 0;
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected transaction status", K(ret), K(trans_info));
    }
  }

  return ret;
}

int ObPartTransCtx::is_running(bool& is_running)
{
  int ret = OB_SUCCESS;
  ObTransStatusInfo trans_info;

  if (OB_FAIL(get_trans_state_and_version_without_lock(trans_info))) {
    TRANS_LOG(WARN, "failed to get trans table status", K(ret));
  } else {
    is_running = trans_info.status_ == ObTransTableStatusType::RUNNING;
  }

  return ret;
}

int ObPartTransCtx::check_sql_sequence_can_read_(
    const ObTransStatusInfo& trans_info, const int64_t sql_sequence, bool& can_read)
{
  int ret = OB_SUCCESS;
  if (ObTransTableStatusType::ABORT == trans_info.status_) {
    can_read = false;
  } else {  // RUNNING || COMMIT
    can_read = !trans_info.undo_status_.is_contain(sql_sequence);
  }
  return ret;
}

int ObPartTransCtx::check_sql_sequence_can_read(const int64_t sql_sequence, bool& can_read)
{
  int ret = OB_SUCCESS;
  ObTransStatusInfo trans_info;

  if (OB_FAIL(get_trans_state_and_version_without_lock(trans_info))) {
    TRANS_LOG(WARN, "failed to get trans table status", K(ret));
  } else if (OB_FAIL(check_sql_sequence_can_read_(trans_info, sql_sequence, can_read))) {
    TRANS_LOG(WARN, "failed to check sql sequence can read", K(ret), K(sql_sequence));
  }

  return ret;
}

int ObPartTransCtx::check_row_locked_(const ObTransStatusInfo& trans_info,
    const ObTransID& data_trans_id, const int64_t sql_sequence, ObStoreRowLockState& lock_state)
{
  int ret = OB_SUCCESS;

  // when lock_state.trans_version_ != 0 means cur trans is commit or abort
  switch (trans_info.status_) {
    case ObTransTableStatusType::COMMIT: {
      lock_state.is_locked_ = false;
      lock_state.trans_version_ = trans_info.trans_version_;
      break;
    }
    case ObTransTableStatusType::RUNNING: {
      lock_state.is_locked_ = !trans_info.undo_status_.is_contain(sql_sequence);
      lock_state.trans_version_ = 0;
      break;
    }
    case ObTransTableStatusType::ABORT: {
      lock_state.is_locked_ = false;
      lock_state.trans_version_ = OB_INVALID_VERSION;
      break;
    }
  }
  if (lock_state.is_locked_) {
    lock_state.lock_trans_id_ = data_trans_id;
  }

  return ret;
}

/*
 * return OB_SUCCESS if there is no lock or locked by myself.
 * return OB_ERR_EXCLUSIVE_LOCK_CONFLICT or OB_TRY_LOCK_ROW_CONFLICT if locked by others.
 *
 * is_locked, is the status of lock. true means locked by myself or others
 * lock_trans_id, is the trans id of who locked the row.
 * */
int ObPartTransCtx::check_row_locked(const ObStoreRowkey& key, ObIMvccCtx& ctx, const ObTransID& read_trans_id,
    const ObTransID& data_trans_id, const int64_t sql_sequence, ObStoreRowLockState& lock_state)
{
  int ret = OB_SUCCESS;
  lock_state.is_locked_ = false;
  ObTransStatusInfo trans_info;

  if (OB_FAIL(get_trans_state_and_version_without_lock(trans_info))) {
    TRANS_LOG(WARN, "failed to get trans table status", K(ret));
  } else if (OB_FAIL(check_row_locked_(trans_info, data_trans_id, sql_sequence, lock_state))) {
    TRANS_LOG(WARN, "failed to check transaction status", K(ret));
    // locked by other
  } else if (lock_state.is_locked_ && lock_state.lock_trans_id_ != read_trans_id) {
    int64_t lock_wait_start_ts =
        ctx.get_lock_wait_start_ts() > 0 ? ctx.get_lock_wait_start_ts() : ObTimeUtility::current_time();
    int64_t query_abs_lock_wait_timeout = ctx.get_query_abs_lock_wait_timeout(lock_wait_start_ts);
    int64_t cur_ts = ObTimeUtility::current_time();

    if (cur_ts >= query_abs_lock_wait_timeout) {
      ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
    } else {
      ret = OB_TRY_LOCK_ROW_CONFLICT;

      const uint32_t uid = ctx.get_ctx_descriptor();
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = get_global_lock_wait_mgr().post_lock(ret,
                             mt_ctx_,
                             self_.get_table_id(),
                             key,
                             query_abs_lock_wait_timeout,
                             -1 /*total_trans_node_cnt*/,
                             uid))) {
        TRANS_LOG(WARN, "post lock error", K(*this), K(key), K(uid));
      }

      ctx.set_lock_wait_start_ts(lock_wait_start_ts);
    }
  }

  TRANS_LOG(DEBUG, "trans status info is", K(trans_info), K(ret), K(lock_state), K(ctx));

  return ret;
}

int ObPartTransCtx::get_callback_type_(const int64_t sql_sequence, TransCallbackType& cb_type)
{
  int ret = OB_SUCCESS;

  ObTransStatusInfo trans_info;

  if (OB_FAIL(get_trans_state_and_version_without_lock(trans_info))) {
    TRANS_LOG(WARN, "failed to get trans table status", K(ret));
  } else if (trans_info.undo_status_.is_contain(sql_sequence)) {
    cb_type = TCB_STMT_ABORT;
  } else if (ObTransTableStatusType::COMMIT == trans_info.status_) {
    cb_type = TCB_TRANS_COMMIT;
  } else if (ObTransTableStatusType::ABORT == trans_info.status_) {
    cb_type = TCB_TRANS_ABORT;
  } else if (ObTransTableStatusType::RUNNING == trans_info.status_) {
    // still running
  } else {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unknown status", K(ret), K(trans_info));
  }

  TRANS_LOG(DEBUG, "trans status info is", K(trans_info));

  return ret;
}

int ObPartTransCtx::cleanout_transnode(ObMvccTransNode& tnode, ObMvccRow& value, bool& cleanout_finish)
{
  int ret = OB_SUCCESS;

  TransCallbackType cb_type = TCB_INVALID;
  cleanout_finish = false;

  if (OB_FAIL(get_callback_type_(tnode.get_sql_sequence(), cb_type))) {
    TRANS_LOG(ERROR, "fail to get callback type", K(ret), K(tnode));
  } else if (cb_type != TCB_INVALID) {
    ObMvccRowCallback cb(mt_ctx_, value, NULL);
    cb.set(NULL, &tnode, 0, NULL, false, false, tnode.get_sql_sequence());
    cb.set_is_link();

    if (OB_FAIL(cb.callback(cb_type, false /* for_replay */, false /* need_lock_for_write */))) {
      TRANS_LOG(WARN, "fail to cleanout transnode", K(ret), K(cb_type), K(tnode), K(value));
    } else {
      cleanout_finish = true;
    }
  } else {
    // still running
  }

  return ret;
}

void ObPartTransCtx::set_exiting_()
{
  if (is_dirty_) {
    partition_log_info_arr_.reset();
    participants_.reset();
  }
  return ObTransCtx::set_exiting_(is_dirty_);
}

int ObPartTransCtx::set_forbidden_sql_no(const int64_t sql_no, bool& forbid_succ)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  if (OB_UNLIKELY(sql_no < forbidden_sql_no_)) {
    ret = OB_TRANS_SQL_SEQUENCE_ILLEGAL;
    TRANS_LOG(WARN, "set forbidden sql no fail", K(ret), K(sql_no), K(*this));
  } else if (sql_no <= stmt_info_.get_sql_no()) {
    // After supporting the inter-partition parallelism, multiple task can
    // operate on the same partition at the same time So the check for whether
    // forbid or not is hard: After a task push up the sql_no, afterward forbid
    // operation of task on the same partition will failure. Because current
    // design only support partition level granularity.
    //
    // Current idea:
    // Implement the forbid based on the txn context, it is mainlu used to
    // reclaim the memory used for forbid mark. It is considered to save task
    // level information in the txn context in the sql layer. Absolutely, if the
    // sql layer can support automatic node resource reclaimation, we can no
    // longer need to rely on txn layer.
    forbid_succ = false;
  } else {
    forbidden_sql_no_ = sql_no;
    forbid_succ = true;
  }
  return ret;
}

int ObPartTransCtx::check_log_ts_and_get_trans_version(
    const int64_t log_ts, int64_t& trans_version, bool& is_related_trans, bool& is_rollback_trans)
{
  int ret = OB_SUCCESS;
  trans_version = 0;
  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "part_trans_ctx is not inited", K(ret));
  } else if (min_log_ts_ <= log_ts) {
    is_related_trans = true;
    is_rollback_trans = false;
    switch (mt_ctx_.get_trans_table_status()) {
      case ObTransTableStatusType::RUNNING:
        trans_version = INT64_MAX;
        break;
      case ObTransTableStatusType::COMMIT:
        trans_version = mt_ctx_.get_commit_version();
        break;
      case ObTransTableStatusType::ABORT:
        is_rollback_trans = true;
        break;
      default:
        break;
    }
  }
  return ret;
}

int ObPartTransCtx::check_if_terminated_in_given_log_range(
    const int64_t start_log_ts, const int64_t end_log_ts, bool& is_terminated)
{
  int ret = OB_SUCCESS;
  is_terminated = false;
  CtxLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "part_trans_ctx is not inited", K(ret));
  } else if (end_log_ts_ != INT64_MAX && is_dirty_ && end_log_ts_ > start_log_ts && end_log_ts_ <= end_log_ts) {
    is_terminated = true;
  }
  return ret;
}

// See comments in submit_log_impl to understand the log submission state
// machine
int ObPartTransCtx::decide_log_type_for_mutator_and_record_(ObStorageLogType& log_type)
{
  int ret = OB_SUCCESS;
  if (is_changing_leader_ && prepare_changing_leader_state_ == CHANGING_LEADER_STATE::LOGGING_NOT_FINISH) {
    log_type = OB_LOG_MUTATOR_WITH_STATE;
    is_trans_state_sync_finished_ = false;
  } else if (part_trans_action_ == ObPartTransAction::COMMIT) {
    if (is_sp_trans_() || is_mini_sp_trans_()) {
      if (OB_FAIL(generate_sp_commit_log_type_(log_type))) {
        TRANS_LOG(WARN, "generate sp commit log failed", K(*this));
      }
    } else if (is_xa_local_trans()) {
      log_type = OB_LOG_TRANS_REDO;
    } else {
      log_type = OB_LOG_TRANS_REDO_WITH_PREPARE;
    }
  } else if (part_trans_action_ == ObPartTransAction::ABORT) {
    if (is_sp_trans_() || is_mini_sp_trans_()) {
      log_type = OB_LOG_SP_TRANS_ABORT;
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "decide next log type is wrong", K(*this));
    }
  } else if (part_trans_action_ == ObPartTransAction::DIED) {
    log_type = OB_LOG_MUTATOR_ABORT;
  } else {
    log_type = OB_LOG_MUTATOR;
  }
  return ret;
}

int ObPartTransCtx::decide_and_submit_next_log_(storage::ObStorageLogType& log_type, 
                                                bool &has_redo_log,
                                                bool has_pending_cb)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(decide_log_type_for_mutator_and_record_(log_type))) {
    TRANS_LOG(ERROR, "decide log type for mutator and record fail", KR(ret), "context", *this);
  } else if (OB_LOG_MUTATOR == log_type &&
             0 != GCONF._private_buffer_size &&
             !mt_ctx_.pending_log_size_too_large() &&
             !has_pending_cb) {
  // If the log is decided as OB_LOG_MUTATOR, we neednot continue to
  // logging if all the followings are true
  // 1. _private_redo_buffer is opened 
  // 2. pending log size is too small for continuing logging
  // 3. there are not pending callbacks for minor merge
  } else if ((OB_LOG_TRANS_REDO_WITH_PREPARE == log_type
              || OB_LOG_TRANS_REDO == log_type)
              && OB_FAIL(alloc_local_trans_version_(log_type))) {
    if (OB_EAGAIN != ret) {
      TRANS_LOG(WARN, "alloc log id and timestamp error", KR(ret), "context", *this,
                K(log_type));
    } else {
      ret = OB_SUCCESS;
      if (submit_log_count_ > 0) {
        TRANS_LOG(ERROR, "unexpected submit log count or pending count", "context", *this,
                  K(log_type));
        need_print_trace_log_ = true;
      }
      inc_submit_log_pending_count_();
      inc_submit_log_count_();
    }
  } else if (OB_FAIL(submit_log_task_(log_type, has_redo_log))) {
    TRANS_LOG(WARN, "submit log task error", KR(ret), "context", *this,
              K(log_type), K(is_sp_trans_()), K(is_mini_sp_trans_()));
  } else {
    TRANS_LOG(DEBUG, "submit log task success", "context", *this,
              K(log_type), K(log_type), K(has_redo_log));
  }

  return ret;
}

void ObPartTransCtx::debug_slow_on_sync_log_success_(const int64_t log_type)
{
  if (OB_LOG_MUTATOR == log_type && !is_changing_leader_) {
    DEBUG_SYNC(BEFORE_SYNC_LOG_SUCCESS);
  }
}

int ObPartTransCtx::update_and_get_trans_table_with_minor_freeze(const int64_t last_replay_log_ts, uint64_t& checksum)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(mt_ctx_.update_and_get_trans_table_with_minor_freeze(last_replay_log_ts, checksum))) {
    TRANS_LOG(WARN, "update trans table with minor freeze fail", K(last_replay_log_ts), K(*this));
  } else {
    REC_TRANS_TRACE_EXT(tlog_,
        calc_checksum_by_minor_freeze,
        OB_ID(ret),
        ret,
        OB_ID(checksum),
        checksum,
        OB_ID(checksum_log_ts),
        mt_ctx_.get_checksum_log_ts());
  }

  return ret;
}

// We use the function to check whether the log submission is valid:
// 1. For mutator and redo log, we can submit it concurrently, while the order
//    in clog should be guaranteed. Otherwise the follower will be confused
// 2. The other logs depends on each other, so no concurrency can not be permit
bool ObPartTransCtx::can_submit_log_(const int64_t log_type)
{
  bool can_submit_log = false;
  if (OB_LOG_MUTATOR == log_type || OB_LOG_TRANS_REDO == log_type || OB_LOG_SP_TRANS_REDO == log_type) {
    if (submit_log_pending_count_ > 0) {
      can_submit_log = false;
    } else {
      can_submit_log = true;
    }
  } else {
    if (submit_log_count_ > 0 || submit_log_pending_count_ > 0) {
      can_submit_log = false;
    } else {
      can_submit_log = true;
    }
  }

  return can_submit_log;
}

void ObPartTransCtx::update_durable_log_id_ts_(const int64_t log_type, const uint64_t log_id, const int64_t log_ts)
{
  // 1. update min_log_id_ and min_log_ts_
  if (INT64_MAX == min_log_ts_ || log_ts < min_log_ts_) {
    min_log_id_ = log_id;
    min_log_ts_ = log_ts;
  }

  // 2. update max_durable_log_ts_
  if (max_durable_log_ts_ < log_ts) {
    max_durable_log_ts_ = log_ts;
  }

  // 3. update end_log_ts_
  if (ObStorageLogTypeChecker::is_trans_commit_log(log_type) || ObStorageLogTypeChecker::is_trans_abort_log(log_type)) {
    end_log_ts_ = log_ts;
  }
}

int ObPartTransCtx::submit_log_if_neccessary()
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  if (0 == submit_log_count_ && mt_ctx_.pending_log_size_too_large() && !batch_commit_trans_) {
    ret = submit_log_incrementally_(false /*need_state_log*/);
  }

  return ret;
}

bool ObPartTransCtx::is_trans_valid_for_replay_(const ObStorageLogType log_type, const int64_t log_ts)
{
  bool ret = true;

  if (!for_replay_) {
    ret = false;
  } else if (log_ts <= max_durable_log_ts_) {
    // newer transaction state, don't need check
  } else if (is_exiting_) {
    // transaction is exiting, should not be check for valid
    ret = false;
  } else {
    int64_t state = state_.get_state();
    if (log_type == OB_LOG_SP_TRANS_REDO) {
      if (ObSpState::INIT != state) {
        ret = false;
      }
    } else if (log_type == OB_LOG_SP_TRANS_COMMIT) {
      if (ObSpState::INIT != state) {
        ret = false;
      }
    } else if (log_type == OB_LOG_SP_TRANS_ABORT) {
      if (ObSpState::INIT != state && ObSpState::PREPARE != state) {
        ret = false;
      }
    } else if (log_type == OB_LOG_MUTATOR) {
      if (Ob2PCState::INIT != state && ObSpState::INIT != state) {
        ret = false;
      }
    } else if (log_type == OB_LOG_TRANS_STATE) {
      if (Ob2PCState::INIT != state) {
        ret = false;
      }
    } else if (log_type == OB_LOG_MUTATOR_ABORT) {
      if (Ob2PCState::INIT != state && ObSpState::INIT != state) {
        ret = false;
      }
    } else if (log_type == OB_LOG_TRANS_REDO) {
      if (Ob2PCState::INIT != state && Ob2PCState::PRE_PREPARE != state) {
        ret = false;
      }
    } else if (log_type == OB_LOG_TRANS_PREPARE) {
      if (Ob2PCState::INIT != state && Ob2PCState::PRE_PREPARE != state) {
        ret = false;
      }
    } else if (log_type == OB_LOG_TRANS_COMMIT) {
      if (Ob2PCState::PREPARE != state) {
        ret = false;
      }
    } else if (log_type == OB_LOG_TRANS_ABORT) {
      if (Ob2PCState::PREPARE != state) {
        ret = false;
      }
    } else if (log_type == OB_LOG_TRANS_CLEAR) {
      if (Ob2PCState::COMMIT != state && Ob2PCState::ABORT != state) {
        ret = false;
      }
    } else if (log_type == OB_LOG_START_MEMBERSHIP_STORAGE) {
      // needn't check
    } else if (log_type == OB_LOG_TRANS_RECORD) {
      if (Ob2PCState::INIT != state) {
        ret = false;
      }
    } else {
      ret = false;
      TRANS_LOG(ERROR, "unknown log type", K(log_type), K(*this));
    }
  }

  return ret;
}

int ObPartTransCtx::check_need_notify_listener_(bool& need_notify_listener)
{
  int ret = OB_SUCCESS;
  need_notify_listener = false;
  // TODO: check whether the target partition exits in the participants array, if not exits, we need notify the listener
  return ret;
}

int ObPartTransCtx::try_respond_coordinator_(const ObTransMsgType msg_type, const ListenerAction action)
{
  int ret = OB_SUCCESS;
  bool is_split_partition = false;
  bool need_notify_listener = false;

  if (OB_FAIL(check_cur_partition_split_(is_split_partition))) {
    TRANS_LOG(WARN, "check current partition split error", KR(ret), K(*this));
  } else if (!is_split_partition) {
    // No split happenes, we can reply to coordinator
    if (OB_FAIL(post_2pc_response_(coordinator_, msg_type))) {
      TRANS_LOG(WARN, "submit 2pc commit response error", "ret", ret, "context", *this);
    }
  } else if (OB_FAIL(check_need_notify_listener_(need_notify_listener))) {
    TRANS_LOG(WARN, "check_need_notify_listener_ failed", KR(ret), K(*this));
  } else if (!need_notify_listener) {
    // Participants array contains the target partition, so we can response to the coordinator
    if (OB_FAIL(post_2pc_response_(coordinator_, msg_type))) {
      TRANS_LOG(WARN, "submit 2pc commit response error", "ret", ret, "context", *this);
    }
  } else if (OB_ISNULL(listener_handler_) && OB_FAIL(init_listener_handler_())) {
    TRANS_LOG(WARN, "failed to init listener handler", KR(ret));
  } else if (listener_handler_->is_listener_ready()) {
    // We can response to the coordinator if the downstream listener is ready
    if (OB_FAIL(post_2pc_response_(coordinator_, msg_type))) {
      TRANS_LOG(WARN, "submit 2pc commit response error", "ret", ret, "context", *this);
    }
  } else if (OB_FAIL(listener_handler_->notify_listener(action))) {
    TRANS_LOG(WARN, "failed to notify listener", KR(ret), "context", *this);
  }

  return ret;
}

int ObPartTransCtx::submit_log_when_preparing_changing_leader_(const int64_t sql_no)
{
  int ret = OB_SUCCESS;

  if (mt_ctx_.is_sql_need_be_submit(sql_no) && 0 == submit_log_count_) {
    ret = submit_log_incrementally_(false /*need state log*/);
    TRANS_LOG(INFO, "submit log when changing leader", KR(ret), K(sql_no), K(*this));
  }

  return ret;
}

int ObPartTransCtx::init_listener_handler_()
{
  int ret = OB_SUCCESS;
  // TODO
  return ret;
}

int ObPartTransCtx::handle_listener_message(const ObTrxMsgBase& msg, const int64_t msg_type)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  ObTrxMsgBase& message = const_cast<ObTrxMsgBase&>(msg);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "listener not inited");
    ret = OB_NOT_INIT;
  } else if (!msg.is_valid()) {
    TRANS_LOG(WARN, "invalid listener message", "context", *this, K(msg));
    ret = OB_TRANS_INVALID_MESSAGE;
  } else if (OB_UNLIKELY(is_exiting_)) {
    TRANS_LOG(WARN, "listener context is exiting", "context", *this, K(msg));
    ret = OB_TRANS_IS_EXITING;
  } else if (for_replay_) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "invalid state, transaction is replaying", K(ret), "context", *this);
  } else if (OB_ISNULL(listener_handler_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "listener handler is NULL", KR(ret), K(*this));
  } else {
    switch (msg_type) {
      case OB_TRX_LISTENER_COMMIT_REQUEST: {
        ObTrxListenerCommitRequest& req = static_cast<ObTrxListenerCommitRequest&>(message);
        if (OB_FAIL(listener_handler_->handle_listener_commit_request(req.split_info_))) {
          TRANS_LOG(WARN, "failed to handle listener commit request", KR(ret), K(*this));
        }
        break;
      }
      case OB_TRX_LISTENER_COMMIT_RESPONSE: {
        ObTrxListenerCommitResponse& res = static_cast<ObTrxListenerCommitResponse&>(message);
        if (OB_FAIL(listener_handler_->handle_listener_commit_response(res))) {
          TRANS_LOG(WARN, "failed to handle listener commit response", KR(ret), K(*this));
        }
        break;
      }
      case OB_TRX_LISTENER_ABORT_REQUEST: {
        ObTrxListenerAbortRequest& req = static_cast<ObTrxListenerAbortRequest&>(message);
        if (OB_FAIL(listener_handler_->handle_listener_abort_request(req.split_info_))) {
          TRANS_LOG(WARN, "failed to handle listener abort request", KR(ret), K(*this));
        }
        break;
      }
      case OB_TRX_LISTENER_ABORT_RESPONSE: {
        ObTrxListenerAbortResponse& res = static_cast<ObTrxListenerAbortResponse&>(message);
        if (OB_FAIL(listener_handler_->handle_listener_abort_response(res))) {
          TRANS_LOG(WARN, "failed to handle listener abort response", KR(ret), K(*this));
        }
        break;
      }
      case OB_TRX_LISTENER_CLEAR_REQUEST: {
        ObTrxListenerClearRequest& req = static_cast<ObTrxListenerClearRequest&>(message);
        if (OB_FAIL(listener_handler_->handle_listener_clear_request(req))) {
          TRANS_LOG(WARN, "failed to handle listener clear request", KR(ret), K(*this));
        }
        break;
      }
      case OB_TRX_LISTENER_CLEAR_RESPONSE: {
        ObTrxListenerClearResponse& res = static_cast<ObTrxListenerClearResponse&>(message);
        if (OB_FAIL(listener_handler_->handle_listener_clear_response(res))) {
          TRANS_LOG(WARN, "failed to handle listener clear response", KR(ret), K(*this));
        }
        break;
      }
      default: {
        ret = OB_TRANS_INVALID_MESSAGE_TYPE;
        TRANS_LOG(ERROR, "wrong listener message type", KR(ret), K(msg_type));
        break;
      }
    }
  }
  return ret;
}

int ObPartTransCtx::construct_listener_context(const ObTrxMsgBase& msg)
{
  int ret = OB_SUCCESS;
  UNUSED(msg);
  CtxLockGuard guard(lock_);
  is_listener_ = true;
  /*
  TODO, init, init listener handler
  */
  return ret;
}

int ObPartTransCtx::replay_listener_commit_log(
    const ObTransCommitLog& log, const int64_t timestamp, const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  UNUSED(log);
  UNUSED(timestamp);
  UNUSED(log_id);
  // TODO
  return ret;
}

int ObPartTransCtx::replay_listener_abort_log(
    const ObTransAbortLog& log, const int64_t timestamp, const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  UNUSED(log);
  UNUSED(timestamp);
  UNUSED(log_id);
  // TODO
  return ret;
}

void ObPartTransCtx::update_clear_log_base_ts_(const int64_t log_ts)
{
  if (log_ts > clear_log_base_ts_) {
    clear_log_base_ts_ = log_ts;
  }
}

int ObPartTransCtx::set_tmp_scheduler_(const ObAddr& scheduler)
{
  int ret = OB_SUCCESS;

  if (!scheduler.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    tmp_scheduler_ = scheduler;
  }

  return ret;
}

void ObPartTransCtx::remove_trans_table()
{
  remove_trans_table_();
}

bool ObPartTransCtx::mark_dirty_trans()
{
  bool ret = false;

  if (!is_dirty_) {
    is_dirty_ = true;
    ret = true;
  }

  return ret;
}

void ObPartTransCtx::get_audit_info(int64_t& lock_for_read_elapse) const
{
  lock_for_read_elapse = mt_ctx_.get_lock_for_read_elapse();
}

// the function is called after the physical restore is successfully finished.
// It kills the uncommitted transactions and promotes the commit action for elr
// and 1PC.

// The restore_version and last_restore_log_ts is respectively the version below
// which transactions should be kept and the log ts who is the last log ts during
// restore phase. fake_terminate_log_ts is mocked as terminate_log_ts of
// aborted dirty transaction. (See details in ObPartTransCtx::fake_kill_).
// For pg restored from 2.x, last_restore_log_id is used instead of last_restore_log_ts

// NB: We should also take dirty txn into account. Of course, Dirty txns are
// more complicated. For example, the transaction status of dirty txn may be
// newer while the log corresponding to the newer status may be filtered and is
// not neccessary to the physical recovery. So we should mark the transaction
// dead even the transaction is committed in transaction table without commit
// log to explicitly suicide. (See details in ObPartTransCtx::fake_kill_).
int ObPartTransCtx::clear_trans_after_restore(const int64_t restore_version, const uint64_t last_restore_log_id,
    const int64_t last_restore_log_ts, const int64_t fake_terminate_log_ts)
{
  int ret = OB_SUCCESS;
  bool need_clear = false;
  CtxLockGuard guard(lock_);
  const int64_t state = get_state_();
  {
    // For pg restored from 2.x, last_restore_log_ts is invalid and last_restore_log_id is valid
    if (OB_INVALID_TIMESTAMP == last_restore_log_ts) {
      if (cluster_version_ < CLUSTER_VERSION_3000 && OB_INVALID_ID != last_restore_log_id) {
        need_clear = min_log_id_ <= last_restore_log_id;
      }
    } else {
      need_clear = min_log_ts_ <= last_restore_log_ts;
    }
  }
  if (IS_NOT_INIT) {
    // skip the uninitialized transactions
    ret = OB_SUCCESS;
    TRANS_LOG(INFO,
        "transaction is not initted",
        K(*this),
        K(restore_version),
        K(last_restore_log_id),
        K(last_restore_log_ts),
        K(fake_terminate_log_ts));
  } else if (!need_clear) {
    // skip new transactions after restore completes
    ret = OB_SUCCESS;
    TRANS_LOG(INFO,
        "new transactions after restore completes",
        K(*this),
        K(restore_version),
        K(last_restore_log_id),
        K(last_restore_log_ts),
        K(fake_terminate_log_ts));
  } else {
    ObTransTableStatusType status = mt_ctx_.get_trans_table_status();
    if (ObTransTableStatusType::COMMIT == status) {
      int64_t commit_version = mt_ctx_.get_commit_version();
      if (commit_version > restore_version) {
        // Case 1.1: the transaction commit version is bigger than the restore
        // version. NB: The case only happens when the trans table contains
        // newer status and the log corresponding the newer status is filtered
        // by the recovery phase.

        // We should fake the transaction be killed with fake_terminate_log_ts.
        // (See details in ObPartTransCtx::fake_kill_)
        TRANS_LOG(INFO,
            "transaction in recover case1.1",
            K(*this),
            K(restore_version),
            K(last_restore_log_id),
            K(last_restore_log_id),
            K(fake_terminate_log_ts));
        ret = fake_kill_(fake_terminate_log_ts);
      } else {
        // Case 1.2: the transaction commit version is bigger than the restore
        // version. The transaction may not have clear log, so we should
        // eliminate the dependency of 2pc by set_exiting_(). And the dirty txns
        // will maintain the trans table and the non-dirty ones will release the
        // ctx quickly
        TRANS_LOG(INFO,
            "transaction in recover case1.2",
            K(*this),
            K(restore_version),
            K(last_restore_log_id),
            K(last_restore_log_ts),
            K(fake_terminate_log_ts));
        (void)set_exiting_();
      }
    } else if (ObTransTableStatusType::ABORT == status) {
      // Case 2: the transaction is aborted. The transaction may not have clear
      // log, so we should eliminate the dependency of 2pc by set_exiting_().
      // And the dirty txns will maintain the trans table and the non-dirty ones
      // will release the ctx quickly
      TRANS_LOG(INFO,
          "transaction in recover case2",
          K(*this),
          K(restore_version),
          K(last_restore_log_id),
          K(last_restore_log_ts),
          K(fake_terminate_log_ts));
      (void)set_exiting_();
    } else if (ObTransTableStatusType::RUNNING == status) {
      int64_t prepare_version = mt_ctx_.get_trans_version();
      if (prepare_version > restore_version) {
        // case 3.1: the transaction is in running status or prepare status with
        // prepare version bigger than the restore version. We should kill it
        // with fake_terminate_log_ts.(See details in ObPartTransCtx::kill_v2_)
        TRANS_LOG(INFO,
            "transaction in recover case3.1",
            K(*this),
            K(restore_version),
            K(last_restore_log_id),
            K(last_restore_log_ts),
            K(fake_terminate_log_ts));
        ret = kill_v2_(fake_terminate_log_ts);
      } else {
        if (ObSpState::PREPARE == state || Ob2PCState::PREPARE == state) {
          // case 3.2: the transaction is in prepare status with prepare version
          // smaller than the restore version. The recovery phase guarantees
          // that all prepared transaction with prepare version smaller than
          // checkpoint ts is decided. NB: So all prepared transactions not
          // decided must be elr or 1pc
          TRANS_LOG(INFO,
              "transaction in recover case3.2",
              K(*this),
              K(restore_version),
              K(last_restore_log_id),
              K(last_restore_log_ts),
              K(fake_terminate_log_ts));

          // must be elr or 1pc, if not, we should report the error
          if (!(is_sp_trans_() && ObSpState::PREPARE == get_state_()) && !(batch_commit_trans_ && is_prepared())) {
            TRANS_LOG(ERROR,
                "unexpected transaction status",
                K(*this),
                K(restore_version),
                K(last_restore_log_id),
                K(last_restore_log_ts),
                K(fake_terminate_log_ts));
          }

          int64_t unused_safe_slave_read_timestamp = -1;
          bool checkpoint_succ = false;

          if (OB_FAIL(checkpoint_(restore_version + 1, unused_safe_slave_read_timestamp, checkpoint_succ))) {
            TRANS_LOG(WARN, "checkpoint the transaction failed", K(*this));
          } else if (!checkpoint_succ) {
            TRANS_LOG(ERROR, "checkpoint failed", K(*this));
          }
        } else {
          // case 3.3: the transaction is in running status. We should kill it
          // with fake_terminate_log_ts.(See details in ObPartTransCtx::kill_v2_)
          TRANS_LOG(INFO,
              "transaction in recover case3.3",
              K(*this),
              K(restore_version),
              K(last_restore_log_id),
              K(last_restore_log_ts),
              K(fake_terminate_log_ts));
          ret = kill_v2_(fake_terminate_log_ts);
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR,
          "unknown state",
          K(*this),
          K(restore_version),
          K(last_restore_log_id),
          K(last_restore_log_ts),
          K(fake_terminate_log_ts),
          K(status));
    }
  }

  return ret;
}

// fake kill is used to maintain the dirty txn information with trans table.
// The trans table state is determined by four parameters(trans_status,
// trans_version, undo_status, terminate_log_id). So when we need to fake a
// aborted trans table, we need set trans_status to be ABORT, terminate_log_ts
// to be a faked terminate log ts which guarantees no data can exist after the
// faked terminate log ts. The trans_version and undo_status is not necessary.
int ObPartTransCtx::fake_kill_(const int64_t terminate_log_ts)
{
  int ret = OB_SUCCESS;
  need_print_trace_log_ = true;

  if (OB_FAIL(mt_ctx_.fake_kill())) {
    TRANS_LOG(WARN, "fake kill transaction failed", K(terminate_log_ts), K(*this));
  } else if (OB_FAIL(unregister_timeout_task_())) {
    TRANS_LOG(WARN, "unregister timer task error", KR(ret), "context", *this);
  } else {
    // TODO(): try to think how to connect the terminate log ts and
    // fake kill interface
    end_log_ts_ = terminate_log_ts;
    // TODO(): the interface is currently not necessary, remove it
    set_state_(Ob2PCState::CLEAR);
    (void)trans_clear_();
    set_exiting_();
  }

  return ret;
}

// TODO(): adapt the kill_v2_ into kill interface
int ObPartTransCtx::kill_v2_(const int64_t terminate_log_ts)
{
  int ret = OB_SUCCESS;
  need_print_trace_log_ = true;

  if (OB_FAIL(trans_kill_())) {
    TRANS_LOG(WARN, "fake kill transaction failed", K(terminate_log_ts), K(*this));
  } else if (OB_FAIL(unregister_timeout_task_())) {
    TRANS_LOG(WARN, "unregister timer task error", KR(ret), "context", *this);
  } else {
    end_log_ts_ = terminate_log_ts;
    // TODO(): the interface is currently not necessary, remove it
    set_state_(Ob2PCState::CLEAR);
    (void)trans_clear_();
    set_exiting_();
  }

  return ret;
}

bool ObPartTransCtx::is_in_trans_table_state()
{
  bool bret = false;
  CtxLockGuard guard(lock_);

  if (is_exiting_ && is_dirty_) {
    bret = true;
  }

  return bret;
}

int64_t ObPartTransCtx::get_part_trans_action() const
{
  int64_t action = part_trans_action_;

  if (ObPartTransAction::UNKNOWN == action) {
    if (stmt_info_.get_sql_no() > 0) {
      int ret = OB_SUCCESS;
      if (OB_FAIL(lock_.try_lock())) {
        TRANS_LOG(INFO, "lock fail to get part_trans_action", K(ret), K_(trans_id), K_(self), KP(this));
      } else {
        action = stmt_info_.is_task_match() ? ObPartTransAction::END_TASK : ObPartTransAction::START_TASK;
        lock_.unlock();
      }
    }
  }

  return action;
}

void ObPartTransCtx::DEBUG_SYNC_slow_txn_before_handle_message_(const int64_t msg_type)
{
  (void)DEBUG_SYNC_slow_txn_during_2pc_prepare_phase_for_physical_backup_1055_(msg_type);
}

void ObPartTransCtx::DEBUG_SYNC_slow_txn_during_2pc_prepare_phase_for_physical_backup_1055_(const int64_t msg_type)
{
  ObPartitionKey mock_key;
  mock_key.init(1100611139453783, 1, 0);

  if (mock_key == self_ && (OB_TRX_2PC_PREPARE_REQUEST == msg_type || OB_TRANS_2PC_PREPARE_REQUEST == msg_type)) {
    DEBUG_SYNC(SLOW_TXN_DURING_2PC_COMMIT_PHASE_FOR_PHYSICAL_BACKUP_1055);
  }
}

int ObPartTransCtx::calc_serialize_size_and_set_participants_(const ObPartitionArray &participants)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  bool enable_trans_ctx_size_limit = false;
  if (tenant_config.is_valid()) {
    enable_trans_ctx_size_limit = tenant_config->_enable_trans_ctx_size_limit;
  }

  if (enable_trans_ctx_size_limit) {
    if (OB_FAIL(do_calc_and_set_participants_(participants))) {
      TRANS_LOG(WARN,
          "do calc and set participants failed. set tenant config _enable_trans_ctx_size_limit=false may be able to "
          "handle this.",
          KR(ret), K(enable_trans_ctx_size_limit));
    }
  } else if (OB_FAIL(set_participants_(participants))) {
    TRANS_LOG(WARN, "set participants failed.", KR(ret), K(enable_trans_ctx_size_limit));
  }

  return ret;
}

#ifdef ERRSIM
#define INJECT_CALC_AND_SET_PARTICIPANTS_ERRSIM                                                       \
  do {                                                                                                \
    if (OB_FAIL(E(EventTable::EN_PARTICIPANTS_SIZE_OVERFLOW) OB_SUCCESS)) {                           \
      OB_MAX_TRANS_SERIALIZE_SIZE = 1500;                                                             \
      OB_MIN_REDO_LOG_SERIALIZE_SIZE = 500;                                                           \
    } else if (OB_FAIL(E(EventTable::EN_PART_PLUS_UNDO_OVERFLOW) OB_SUCCESS)) {                       \
      OB_MAX_TRANS_SERIALIZE_SIZE = 7500;                                                             \
      OB_MIN_REDO_LOG_SERIALIZE_SIZE = 500;                                                           \
    } else if (participants_serialize_size > 4096 &&                                                  \
               OB_FAIL(E(EventTable::EN_HANDLE_PREPARE_MESSAGE_EAGAIN) OB_SUCCESS)) {                 \
      TRANS_LOG(INFO, "ERRSIM set participants once", KR(ret), K(participants_serialize_size));       \
    }                                                                                                 \
                                                                                                      \
    OB_MAX_UNDO_ACTION_SERIALIZE_SIZE = OB_MAX_TRANS_SERIALIZE_SIZE - OB_MIN_REDO_LOG_SERIALIZE_SIZE; \
                                                                                                      \
    TRANS_LOG(INFO,                                                                                   \
        "ERRSIM modify trans ctx serialize size ",                                                    \
        K(OB_MAX_TRANS_SERIALIZE_SIZE),                                                               \
        K(OB_MIN_REDO_LOG_SERIALIZE_SIZE),                                                            \
        K(OB_MAX_UNDO_ACTION_SERIALIZE_SIZE));                                                        \
                                                                                                      \
    ret = OB_SUCCESS;                                                                                 \
  } while (false);
#else
#define INJECT_CALC_AND_SET_PARTICIPANTS_ERRSIM
#endif

/*
 * There are three kinds of cases when set participants. We calculate serialize size and do
 * something to avoid dumping trans state table fail.
 *
 *          ┌─────────────────────────────────────────────────┐
 * CASE 1 : │                   participants                  │
 *          └─────────────────────────────────────────────────┘
 *
 *          │          OB_MAX_TRANS_SERIALIZE_SIZE       │
 *          └────────────────────────────────────────────┘
 * Participants are too large. This situation should be handled by upper layer
 *
 *          ┌────────────────────────────┬────────────────────┐
 * CASE 2 : │         participants       │     undo status    │
 *          └────────────────────────────┴────────────────────┘
 *
 *          │          OB_MAX_TRANS_SERIALIZE_SIZE       │
 *          └────────────────────────────────────────────┘
 * Participants plus undo status are too large. Trans state table can not be dumped by flushing
 * record log.Rollback it without returning error code because the follower cannot response to the
 * leader if return error code here.
 *
 *          ┌─────────────────┬───────────────┬────────────────┐
 * CASE 3 : │   participants  │  undo status  │    redo log    │
 *          └─────────────────┴───────────────┴────────────────┘
 *
 *          │          OB_MAX_TRANS_SERIALIZE_SIZE       │
 *          └────────────────────────────────────────────┘
 * Flush record log can make trans state table be successfully dumped.
 */
int ObPartTransCtx::do_calc_and_set_participants_(const ObPartitionArray &participants)
{
  int ret = OB_SUCCESS;
  int64_t participants_serialize_size = participants_.get_serialize_size() + participants.get_serialize_size();
  int64_t undo_serialize_size = undo_status_.get_serialize_size();
  int64_t redo_log_id_serialize_size = prev_redo_log_ids_.get_serialize_size();
  bool has_redo_log = false;
  bool need_submit_record_log = false;

  INJECT_CALC_AND_SET_PARTICIPANTS_ERRSIM

  if (OB_UNLIKELY(participants_serialize_size > OB_MAX_TRANS_SERIALIZE_SIZE)) {
    // case 1
    ret = OB_ERR_UNEXPECTED;
    int64_t participants_count = participants.count();
    TRANS_LOG(ERROR,
        "participants is unexpected too large.",
        KR(ret),
        K(participants_count),
        K(participants_serialize_size),
        KPC(this));
  } else if (OB_UNLIKELY(participants_serialize_size + undo_serialize_size > OB_MAX_TRANS_SERIALIZE_SIZE)) {
    // case 2
    TRANS_LOG(WARN,
        "transaction is too large. flush record log can not handle it",
        K(participants_serialize_size),
        K(undo_serialize_size),
        K(OB_MAX_TRANS_SERIALIZE_SIZE),
        K(trans_id_),
        KPC(this));
    set_status_(OB_TRANS_NEED_ROLLBACK);

    // Reset undo status to make trans state table can be dumped.
    undo_status_.reset();
    undo_serialize_size = 0;
  } else {
    // normal case, set participants directly
  }

  if (OB_SUCC(ret) && OB_UNLIKELY(participants_serialize_size + undo_serialize_size + redo_log_id_serialize_size >
                                  OB_MAX_TRANS_SERIALIZE_SIZE)) {
    // case 3
    need_submit_record_log = true;
    int64_t total_size = participants_serialize_size + redo_log_id_serialize_size + undo_serialize_size;
    TRANS_LOG(INFO,
        "flush record log to reserve space for participants",
        K(participants_serialize_size),
        K(undo_serialize_size),
        K(redo_log_id_serialize_size),
        K(total_size),
        K(OB_MAX_TRANS_SERIALIZE_SIZE),
        KPC(this));
  }

  if (OB_FAIL(ret)) {
    // participants is too large, this function can not handle it
  } else if (OB_UNLIKELY(need_submit_record_log) && OB_FAIL(submit_log_async_(OB_LOG_TRANS_RECORD, has_redo_log))) {
    TRANS_LOG(WARN, "submit record log failed", KR(ret), KPC(this));
  } else if (OB_FAIL(set_participants_(participants))) {
    TRANS_LOG(WARN, "set participants error", KR(ret), KPC(this), K(participants));
  }

  return ret;
}

#ifdef ERRSIM
#define INJECT_CALC_AND_SET_UNDO_ERRSIM                                                               \
  do {                                                                                                \
    if (OB_FAIL(E(EventTable::EN_UNDO_ACTIONS_SIZE_OVERFLOW) OB_SUCCESS)) {                           \
      OB_MAX_TRANS_SERIALIZE_SIZE = 7500;                                                             \
      OB_MIN_REDO_LOG_SERIALIZE_SIZE = 4900;                                                          \
    }                                                                                                 \
                                                                                                      \
    OB_MAX_UNDO_ACTION_SERIALIZE_SIZE = OB_MAX_TRANS_SERIALIZE_SIZE - OB_MIN_REDO_LOG_SERIALIZE_SIZE; \
                                                                                                      \
    TRANS_LOG(INFO,                                                                                   \
        "ERRSIM modify trans ctx serialize size ",                                                    \
        K(OB_MAX_TRANS_SERIALIZE_SIZE),                                                               \
        K(OB_MIN_REDO_LOG_SERIALIZE_SIZE),                                                            \
        K(OB_MAX_UNDO_ACTION_SERIALIZE_SIZE));                                                        \
                                                                                                      \
    ret = OB_SUCCESS;                                                                                 \
  } while (false);
#else
#define INJECT_CALC_AND_SET_UNDO_ERRSIM
#endif

/*
 * There are two cases when set undo status. We calculate serialize size and do something to avoid
 * dumping trans state table fail.
 *
 * We increase undo_serialize_size_ every time we set undo. But if the undo_serialize_size is too
 * large and the transaction need rollback, we update this variable to get a real size.
 *
 *          ┌────────────────────────────────────────┐
 * CASE 4 : │              undo status               │
 *          └────────────────────────────────────────┘
 *
 *          │ OB_MAX_UNDO_ACTION_SERIALIZE_SIZE  │
 *          └────────────────────────────────────┘
 *
 *          │          OB_MAX_TRANS_SERIALIZE_SIZE       │
 *          └────────────────────────────────────────────┘
 * Undo status are too large, rollback this transaction.
 *
 *          ┌───────────────────────────┬───────────────────────┐
 * CASE 5 : │         undo status       │        redo log       │
 *          └───────────────────────────┴───────────────────────┘
 *
 *          │ OB_MAX_UNDO_ACTION_SERIALIZE_SIZE  │
 *          └────────────────────────────────────┘
 *
 *          │          OB_MAX_TRANS_SERIALIZE_SIZE       │
 *          └────────────────────────────────────────────┘
 * Flush record log can make trans state table be successfully dumped.
 */
int ObPartTransCtx::calc_serialize_size_and_set_undo_(const int64_t undo_to, const int64_t undo_from)
{
  int ret = OB_SUCCESS;
  ObUndoAction undo_action(undo_to, undo_from);
  int64_t undo_serialize_size = undo_status_.get_serialize_size();
  int64_t redo_log_id_serialize_size = prev_redo_log_ids_.get_serialize_size();
  bool has_redo_log = false;
  bool updated_size = false;

  INJECT_CALC_AND_SET_UNDO_ERRSIM

  if (undo_serialize_size > OB_MAX_UNDO_ACTION_SERIALIZE_SIZE) {
    // case 4
    set_status_(OB_TRANS_NEED_ROLLBACK);
    ret = OB_SIZE_OVERFLOW;
    TRANS_LOG(WARN,
        "size overflow when set undo action",
        KR(ret),
        K(undo_serialize_size),
        K(redo_log_id_serialize_size),
        K(OB_MAX_UNDO_ACTION_SERIALIZE_SIZE),
        KPC(this));
  } else if (OB_UNLIKELY(undo_serialize_size + redo_log_id_serialize_size > OB_MAX_TRANS_SERIALIZE_SIZE)) {
    // case 5
    TRANS_LOG(INFO,
        "flush record log to reserve space for undo",
        K(undo_serialize_size),
        K(redo_log_id_serialize_size),
        K(OB_MAX_TRANS_SERIALIZE_SIZE));
    if (OB_FAIL(submit_log_async_(OB_LOG_TRANS_RECORD, has_redo_log))) {
      TRANS_LOG(WARN,
          "submit record log failed",
          KR(ret),
          K(undo_serialize_size),
          K(redo_log_id_serialize_size),
          K(OB_MAX_TRANS_SERIALIZE_SIZE),
          KPC(this));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(undo_status_.undo(undo_to, undo_from))) {
    TRANS_LOG(WARN, "record rollback action failed", KR(ret), K(undo_action), KPC(this));
  }

  return ret;
}

}  // namespace transaction
}  // namespace oceanbase
