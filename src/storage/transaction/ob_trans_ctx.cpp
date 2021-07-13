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
#include "ob_trans_ctx.h"
#include "ob_trans_ctx_mgr.h"
#include "ob_trans_service.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "share/ob_cluster_version.h"
#include "share/rc/ob_context.h"
#include "ob_gc_partition_adapter.h"

namespace oceanbase {
using namespace common;
using namespace memtable;
using namespace share;

namespace transaction {

void ObTransState::set_state(const int64_t state)
{
  ObByteLockGuard guard(lock_);
  state_ = state;
}

void ObTransState::set_prepare_version(const int64_t prepare_version)
{
  ObByteLockGuard guard(lock_);
  prepare_version_ = prepare_version;
}

void ObTransState::get_state_and_version(int64_t& state, int64_t& prepare_version)
{
  ObByteLockGuard guard(lock_);
  state = state_;
  prepare_version = prepare_version_;
}

int ObTransCtx::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t trans_expired_time,
    const ObPartitionKey& self, ObITransCtxMgr* ctx_mgr, const ObStartTransParam& trans_param,
    const uint64_t cluster_version, ObTransService* trans_service)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    TRANS_LOG(WARN, "ObTransCtx inited twice", K(self), K(trans_id));
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(ctx_type_str_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "transaction context not constructed", KR(ret), K(trans_id));
  } else if (!is_valid_tenant_id(tenant_id) || !trans_id.is_valid() || !self.is_valid() || trans_expired_time <= 0 ||
             OB_ISNULL(ctx_mgr) || !trans_param.is_valid() || cluster_version <= 0 || OB_ISNULL(trans_service)) {
    TRANS_LOG(WARN,
        "invalid argument",
        K(tenant_id),
        K(trans_id),
        K(self),
        KP(ctx_mgr),
        K(trans_param),
        K(cluster_version),
        KP(trans_service));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(lock_.init(this))) {
    TRANS_LOG(WARN, "init lock error", KR(ret));
  } else {
    const ObAddr& addr = trans_service->get_server();
    if (!addr.is_valid()) {
      TRANS_LOG(ERROR, "unexepcted error", KP(trans_service), "addr", addr);
    } else {
      tenant_id_ = tenant_id;
      self_ = self;
      addr_ = addr;
      trans_id_ = trans_id;
      ctx_mgr_ = ctx_mgr;
      trans_param_ = trans_param;
      trans_expired_time_ = trans_expired_time;
      ctx_create_time_ = ObClockGenerator::getClock();
      trans_service_ = trans_service;
      cluster_version_ = cluster_version;
      last_check_gc_ts_ = ObClockGenerator::getClock();
      part_trans_action_ = ObPartTransAction::UNKNOWN;

      if (NULL == partition_mgr_) {
        TRANS_LOG(ERROR, "partition mgr is null, unexpected error", KP_(partition_mgr), "context", *this);
        ret = OB_ERR_UNEXPECTED;
      } else if (is_readonly_) {
        partition_mgr_->inc_read_only_count();
      } else {
        partition_mgr_->inc_active_read_write_count();
      }

      if (OB_SUCC(ret)) {
        ObTransTraceLog* tmp_tlog = NULL;
        if (OB_FAIL(alloc_audit_rec_and_trace_log_(trans_service, tmp_tlog))) {
          TRANS_LOG(ERROR, "alloc ObTransTraceLog failed", KR(ret));
        } else {
          tlog_ = tmp_tlog;
          is_inited_ = true;

          if (trans_id.get_inc_num() < 32) {
            TRANS_LOG(INFO, "transaction context init success", "context", *this);
          }

          REC_TRANS_TRACE_EXT(tlog_,
              init,
              OB_ID(arg1),
              (int64_t) & (*this),
              OB_ID(ctx_type),
              ctx_type_str_,
              OB_ID(trans_type),
              trans_type_,
              OB_ID(trans_id),
              trans_id,
              OB_ID(pkey),
              self,
              OB_ID(arg1),
              trans_param_.is_bounded_staleness_read(),
              OB_ID(uref),
              get_uref());
        }
      }
    }
  }
  return ret;
}

void ObTransCtx::destroy()
{
  if (is_inited_) {
    if (end_trans_cb_.need_callback()) {
      end_trans_cb_.callback(OB_TRANS_UNKNOWN);
    }
    print_trace_log_if_necessary_();
    magic_number_ = UNKNOWN_FREE_CTX_MAGIC_NUM;
    if (NULL == partition_mgr_) {
      TRANS_LOG(ERROR, "partition mgr is null, unexpected error", KP_(partition_mgr), "context", *this);
    } else {
      partition_mgr_->desc_total_ctx_count();
    }
    if (NULL != trans_audit_record_) {
      int ret = OB_SUCCESS;
      // backfill audit buffer
      (void)trans_audit_record_->set_trans_audit_data(tenant_id_,
          addr_,
          trans_id_,
          self_,
          session_id_,
          proxy_session_id_,
          trans_type_,
          get_uref(),
          ctx_create_time_,
          trans_expired_time_,
          trans_param_,
          get_type(),
          get_status_(),
          is_for_replay());
      ObTransAuditRecordMgr* record_mgr = NULL;
      if (OB_ISNULL(record_mgr = record_mgr_guard_.get_trans_audit_record_mgr())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "failed to get trans audit record manager");
      } else if (OB_FAIL(record_mgr->revert_record(trans_audit_record_))) {
        TRANS_LOG(ERROR, "revert trans audit record failed, unexpected error", KR(ret));
      } else {
        // do nothing
      }
      trans_audit_record_ = NULL;
    } else if (NULL != tlog_) {
      ObTransTraceLogFactory::release(tlog_);
      tlog_ = NULL;
    }
    record_mgr_guard_.destroy();
    is_inited_ = false;
  }
}

int ObTransCtx::reset_trans_audit_record()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(trans_audit_record_)) {
    ObTransAuditRecordMgr* record_mgr = NULL;
    if (OB_ISNULL(record_mgr = record_mgr_guard_.get_trans_audit_record_mgr())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "failed to get trans audit record manager");
    } else if (OB_FAIL(record_mgr->revert_record(trans_audit_record_))) {
      TRANS_LOG(ERROR, "revert trans audit record failed, unexpected error", KR(ret));
    }
    trans_audit_record_ = NULL;
    tlog_ = NULL;
  }
  return ret;
}

int ObTransCtx::TransAuditRecordMgrGuard::set_tenant_id(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(NULL == with_tenant_ctx_)) {
    with_tenant_ctx_ = new (buf_) ObTenantSpaceFetcher(tenant_id);
    if (OB_FAIL(with_tenant_ctx_->get_ret())) {
      TRANS_LOG(WARN, "failed to switch tenant context", K(tenant_id), KR(ret));
    }
  }
  return ret;
}

ObTransAuditRecordMgr* ObTransCtx::TransAuditRecordMgrGuard::get_trans_audit_record_mgr()
{
  ObTransAuditRecordMgr* mgr = NULL;
  if (OB_NOT_NULL(with_tenant_ctx_)) {
    mgr = with_tenant_ctx_->entity().get_tenant()->get<ObTransAuditRecordMgr*>();
  }
  return mgr;
}

void ObTransCtx::TransAuditRecordMgrGuard::destroy()
{
  if (OB_NOT_NULL(with_tenant_ctx_)) {
    with_tenant_ctx_->~ObTenantSpaceFetcher();
    with_tenant_ctx_ = NULL;
  }
}

void ObTransCtx::get_ctx_guard(CtxLockGuard& guard)
{
  guard.set(lock_);
}

void ObTransCtx::print_trace_log()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(lock_.try_lock())) {
    TRANS_LOG(WARN, "print trace log trylock error", K(ret));
  } else {
    print_trace_log_();
    lock_.unlock();
  }
  UNUSED(ret);
}

void ObTransCtx::print_trace_log_()
{
  FORCE_PRINT_TRACE(tlog_, "[force print]");
}

void ObTransCtx::reset()
{
  // unknown
  magic_number_ = UNKNOWN_RESET_CTX_MAGIC_NUM;
  is_inited_ = false;
  cluster_version_ = CLUSTER_VERSION_140;
  self_.reset();
  trans_id_.reset();
  ctx_mgr_ = NULL;
  trans_param_.reset();
  addr_.reset();
  trans_expired_time_ = 0;
  ctx_create_time_ = 0;
  trans_service_ = NULL;
  lock_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  task_list_.reset();
  state_.reset();
  status_ = OB_SUCCESS;
  partition_mgr_ = NULL;
  trans_type_ = TransType::DIST_TRANS;
  session_id_ = 0;
  proxy_session_id_ = 0;
  stc_.reset();
  tlog_ = NULL;
  part_trans_action_ = ObPartTransAction::UNKNOWN;
  end_trans_cb_.reset();
  pending_callback_param_ = OB_SUCCESS;
  trans_need_wait_wrap_.reset();
  replay_clear_clog_ts_ = 0;
  is_dup_table_trans_ = false;
  is_exiting_ = false;
  is_readonly_ = false;
  for_replay_ = false;
  need_print_trace_log_ = false;
  is_bounded_staleness_read_ = false;
  has_pending_callback_ = false;
  need_record_rollback_trans_log_ = false;
  elr_prepared_state_ = ELR_INIT;
  p_mt_ctx_ = NULL;
  can_elr_ = false;
}

int ObTransCtx::set_partition(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;

  CtxLockGuard guard(lock_);

  if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else {
    self_ = partition;
  }

  return ret;
}

int ObTransCtx::set_partition_trans_ctx_mgr(ObPartitionTransCtxMgr* partition_mgr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(partition_mgr)) {
    TRANS_LOG(WARN, "invalid argument", K(partition_mgr));
    ret = OB_INVALID_ARGUMENT;
  } else {
    partition_mgr_ = partition_mgr;
  }

  return ret;
}

int ObTransCtx::alloc_audit_rec_and_trace_log_(ObTransService* trans_service, ObTransTraceLog*& trace_log)
{
  int ret = OB_SUCCESS;
  // 1. sql_audit is on. If fail to get recod but revert successfully,
  //    it needs to be allocated dynamically. Return error if fail to revert.
  // 2. sql_audit is off. Trace log is not required to be recorded
  bool need_alloc = true;
  ObTransTraceLog* tmp_tlog = NULL;

  if (OB_ISNULL(trans_service)) {
    TRANS_LOG(WARN, "invalid argument", K(trans_service));
    ret = OB_INVALID_ARGUMENT;
  } else if (!GCONF.enable_sql_audit) {
    need_alloc = false;
    ret = OB_SUCCESS;
  } else {
    int32_t retry_times = 3;
    ObTransAuditRecordMgr* record_mgr = NULL;
    if (OB_FAIL(record_mgr_guard_.set_tenant_id(tenant_id_))) {
      TRANS_LOG(WARN, "record_mgr_guard set tenant id error", KR(ret), K_(tenant_id));
    } else if (OB_ISNULL(record_mgr = record_mgr_guard_.get_trans_audit_record_mgr())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "failed to get trans audit record manager");
    } else {
      ObTransAuditRecord* tmp_rec = NULL;
      while (retry_times--) {
        if (OB_FAIL(record_mgr->get_empty_record(tmp_rec))) {
          if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
            TRANS_LOG(WARN, "get empty audit record failed", KR(ret));
          }
        } else if (OB_FAIL(tmp_rec->init(this))) {
          TRANS_LOG(WARN, "set trans ctx in audit record failed", KR(ret));
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = record_mgr->revert_record(tmp_rec))) {
            TRANS_LOG(WARN, "record_mgr revert audit record failed", K(tmp_ret));
            ret = tmp_ret;
            // if fail to revert, error is returned and the trans starts unsuccessfully
            need_alloc = false;
            break;
          }
        } else {
          trans_audit_record_ = tmp_rec;
          tmp_tlog = trans_audit_record_->get_trace_log();
          break;  // success
        }
      }
    }
  }
  // If fail to get recod but revert successfully, trace log needs to be allocated dynamically
  if (OB_FAIL(ret) && need_alloc) {
    tmp_tlog = ObTransTraceLogFactory::alloc();
    if (OB_ISNULL(tmp_tlog)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "alloc ObTransTraceLog error", KR(ret));
    } else {
      ret = OB_SUCCESS;  // rewrite ret
    }
  }

  trace_log = tmp_tlog;
  return ret;
}

void ObTransCtx::before_unlock(CtxLockArg& arg)
{
  arg.task_list_.push_range(task_list_);

  if (has_pending_callback_) {
    arg.end_trans_cb_ = end_trans_cb_;
    arg.has_pending_callback_ = has_pending_callback_;
    arg.pending_callback_param_ = pending_callback_param_;
    end_trans_cb_.reset();
    has_pending_callback_ = false;
    pending_callback_param_ = OB_SUCCESS;
  }

  if (NULL != p_mt_ctx_ && is_elr_prepared_()) {
    arg.p_mt_ctx_ = p_mt_ctx_;
    p_mt_ctx_ = NULL;
  }
}

void ObTransCtx::after_unlock(CtxLockArg& arg)
{
  int ret = OB_SUCCESS;
  obrpc::ObTransRpcResult result;
  DLIST_FOREACH_REMOVESAFE_NORET(curr, arg.task_list_)
  {
    if (NULL != trans_service_) {
      // ignore ret
      const int64_t msg_type = curr->get_msg().msg_type_;
      if (ObTransMsgType2Checker::is_valid_msg_type(msg_type)) {
        trans_service_->handle_batch_msg(msg_type + OB_TRX_NEW_MSG_TYPE_BASE, curr->get_msg().data_.trx_msg_base);
      } else {
        trans_service_->handle_trans_msg(curr->get_msg().data_.trans_msg, result);
      }
    }
    LocalTaskFactory::release(const_cast<LocalTask*>(curr));
    curr = NULL;
  }
  if (NULL != arg.p_mt_ctx_) {
    arg.p_mt_ctx_->elr_trans_preparing();
    // Subtract ref to avoid the core dump caused by memory collection
    // in the ending transaction context in the subsequent process after unlocking
    (void)partition_mgr_->release_ctx_ref(this);
  }
  if (arg.has_pending_callback_) {
    int64_t remaining_wait_interval_us = get_remaining_wait_interval_us_();
    if (0 == remaining_wait_interval_us) {
      if (OB_FAIL(arg.end_trans_cb_.callback(arg.pending_callback_param_))) {
        need_print_trace_log_ = true;
        TRANS_LOG(WARN, "end transaction callback failed", KR(ret), "context", *this);
      }
      REC_TRANS_TRACE_EXT(tlog_, end_trans_cb, Y(ret), OB_ID(param), arg.pending_callback_param_, OB_ID(param), true);
    } else {
      // register asynchronous callback task
      EndTransCallbackTask* task = NULL;
      if (OB_ISNULL(task = EndTransCallbackTaskFactory::alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG(WARN, "alloc memory failed", KR(ret), K(*this));
      } else if (OB_FAIL(task->make(ObTransRetryTaskType::END_TRANS_CB_TASK,
                     arg.end_trans_cb_,
                     arg.pending_callback_param_,
                     trans_need_wait_wrap_.get_receive_gts_ts(),
                     trans_need_wait_wrap_.get_need_wait_interval_us()))) {
        TRANS_LOG(WARN, "make ObEndTransCallbackTask error", KR(ret), K(*task), K(*this));
      } else if (OB_ISNULL(trans_service_)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "trans service is NULL", K(ret));
      } else if (OB_FAIL(trans_service_->push(task))) {
        TRANS_LOG(WARN, "push task error", KR(ret), K(*task), K(*this));
      } else {
        // do nothing
      }
      if (OB_FAIL(ret)) {
        if (NULL != task) {
          EndTransCallbackTaskFactory::release(task);
        }
        // if fail to register asynchronous callback, continue to use synchronous callback
        while (get_remaining_wait_interval_us_() > 0) {
          usleep(get_remaining_wait_interval_us_());
        }
        if (OB_FAIL(arg.end_trans_cb_.callback(arg.pending_callback_param_))) {
          TRANS_LOG(WARN, "end transaction callback failed", KR(ret), "context", *this);
        }
      }
      REC_TRANS_TRACE_EXT(tlog_, end_trans_cb, Y(ret), OB_ID(param), arg.pending_callback_param_, OB_ID(param), false);
    }
  }
}

int ObTransCtx::set_trans_param_(const ObStartTransParam& trans_param)
{
  int ret = OB_SUCCESS;
  trans_param_ = trans_param;
  return ret;
}

void ObTransCtx::print_trace_log_if_necessary_()
{
  // freectx
  if (!is_exiting_ && !is_readonly_) {
    TRANS_LOG(ERROR, "ObPartTransCtx not exiting", "context", *this, K(lbt()));
    FORCE_PRINT_TRACE(tlog_, "[trans debug] ");
  }

  if (is_slow_query_()) {
    static ObMiniStat::ObStatItem item("slow trans statistics", 60 * 1000 * 1000);
    ObMiniStat::stat(item);
    FORCE_PRINT_TRACE(tlog_, "[slow trans] ");
  } else if (need_print_trace_log_) {
    FORCE_PRINT_TRACE(tlog_, "[trans warn] ");
  } else if (OB_UNLIKELY(trans_id_.is_sampling())) {
    FORCE_PRINT_TRACE(tlog_, "[trans sampling] ");
  } else {
    PRINT_TRACE(tlog_);
  }
}

void ObTransCtx::set_exiting_(bool is_dirty)
{
  int tmp_ret = OB_SUCCESS;

  if (!is_exiting_) {
    is_exiting_ = true;
    if (is_dirty) {
      print_trace_log_if_necessary_();
    }

    const int64_t uref = get_uref();
    if (NULL == partition_mgr_) {
      TRANS_LOG(ERROR, "partition mgr is null, unexpected error", KP_(partition_mgr), "context", *this);
    } else {
      TRANS_LOG(DEBUG, "transaction exiting", "context", *this);
      REC_TRANS_TRACE_EXT(tlog_, exiting, OB_ID(uref), uref, OB_ID(arg1), session_id_, OB_ID(arg2), proxy_session_id_);
      // The following operations should be placed before erase_trans_ctx.
      // This is used to avoid accessing the pointer of partition_mgr after memory release of ctx.
      if (OB_UNLIKELY(0 != xa_ref_count_)) {
        TRANS_LOG(WARN, "wrong xa ref count", K(*this), K_(xa_ref_count), "lbt", lbt());
      }
      if (is_readonly_) {
        partition_mgr_->desc_read_only_count();
      } else {
        partition_mgr_->desc_active_read_write_count();
      }
      if (!is_dirty && OB_SUCCESS != (tmp_ret = partition_mgr_->erase_trans_ctx_(trans_id_))) {
        TRANS_LOG(WARN, "erase transaction context error", "ret", tmp_ret, "context", *this);
      }
    }
  }
}

void ObTransCtx::set_exiting_()
{
  set_exiting_(false /*is_dirty*/);
}

void ObTransCtx::remove_trans_table_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(partition_mgr_->erase_trans_ctx_(trans_id_))) {
    TRANS_LOG(WARN, "erase transaction context error", K(ret), "context", *this);
  } else {
    const int64_t uref = get_uref();
    FLOG_INFO("remove unused trans table", K(trans_id_), K(uref), K(*this));
  }
}

bool ObTransCtx::is_slow_query_() const
{
  return ObClockGenerator::getClock() >=
         (ctx_create_time_ + ObServerConfig::get_instance().trace_log_slow_query_watermark);
}

void ObTransCtx::set_stc_(const MonotonicTs stc)
{
  if (0 == stc_.mts_) {
    stc_ = stc;
    REC_TRANS_TRACE_EXT(tlog_, set_stc, OB_ID(stc), stc_.mts_);
  }
}

void ObTransCtx::set_stc_by_now_()
{
  if (0 == stc_.mts_) {
    stc_ = MonotonicTs::current_time();
    REC_TRANS_TRACE_EXT(tlog_, set_stc, OB_ID(stc), stc_.mts_);
  }
}

void ObTransCtx::check_partition_exist_(const ObPartitionKey& pkey, bool& partition_exist)
{
  if (pkey.is_valid()) {
    (void)GC_PARTITION_ADAPTER.check_partition_exist(pkey, partition_exist);
  }
}

ObITsMgr* ObTransCtx::get_ts_mgr_()
{
  return trans_service_->get_ts_mgr();
}

void ObTransCtx::end_trans_callback_(const int cb_param)
{
  if (end_trans_cb_.need_callback()) {
    if (OB_SUCCESS != cb_param) {
      TRANS_LOG(WARN, "end transaction callback", K(cb_param), "context", *this);
      need_print_trace_log_ = true;
    }
    has_pending_callback_ = true;
    pending_callback_param_ = cb_param;
  }
}

void ObTransCtx::end_trans_callback_(const bool is_rollback, const int retcode)
{
  int cb_param = retcode;
  if (is_rollback && (OB_TRANS_ROLLBACKED == retcode || OB_TRANS_UNKNOWN == retcode || OB_TRANS_KILLED == retcode)) {
    // rewrite cb_param
    cb_param = OB_SUCCESS;
  }

  end_trans_callback_(cb_param);
}

int ObDistTransCtx::init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t trans_expired_time,
    const ObPartitionKey& self, ObITransCtxMgr* ctx_mgr, const ObStartTransParam& trans_param,
    const uint64_t cluster_version, ObTransService* trans_service)
{
  int ret = OB_SUCCESS;
  const int64_t PART_MAX_PRINT_COUNT = 32;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_UNLIKELY(!trans_id.is_valid()) ||
      OB_UNLIKELY(!self.is_valid()) || OB_UNLIKELY(trans_expired_time <= 0) || OB_ISNULL(ctx_mgr) ||
      OB_UNLIKELY(!trans_param.is_valid()) || OB_UNLIKELY(cluster_version <= 0) || OB_ISNULL(trans_service)) {
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
  } else if (OB_FAIL(participants_.set_max_print_count(PART_MAX_PRINT_COUNT))) {
    TRANS_LOG(WARN, "set participant max print count error", KR(ret));
  } else if (OB_ISNULL(rpc_ = trans_service->get_trans_rpc()) ||
             OB_ISNULL(location_adapter_ = trans_service->get_location_adapter()) ||
             OB_ISNULL(timer_ = &(trans_service->get_trans_timer()))) {
    TRANS_LOG(ERROR, "ObTransService is invalid, unexpected error", KP(rpc_), KP(location_adapter_));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(timeout_task_.init(this))) {
    TRANS_LOG(WARN, "timeout task init error", KR(ret));
  } else if (OB_FAIL(trace_info_.init())) {
    TRANS_LOG(WARN, "trace info init error", K(ret), K(self), K(trans_id), K_(trace_info));
  } else if (OB_FAIL(ObTransCtx::init(tenant_id,
                 trans_id,
                 trans_expired_time,
                 self,
                 ctx_mgr,
                 trans_param,
                 cluster_version,
                 trans_service))) {
    TRANS_LOG(WARN, "ObTransCtx inited error", KR(ret));
  } else {
    set_state_(Ob2PCState::INIT);
    trans_2pc_timeout_ = ObServerConfig::get_instance().trx_2pc_retry_interval;
  }

  return ret;
}

void ObDistTransCtx::reset()
{
  // destroy();
  ObTransCtx::reset();
  scheduler_.reset();
  coordinator_.reset();
  participants_.reset();
  request_id_ = OB_INVALID_TIMESTAMP;
  rpc_ = NULL;
  location_adapter_ = NULL;
  commit_start_time_ = -1;
  trans_start_time_ = -1;
  need_refresh_location_ = false;
  trans_2pc_timeout_ = 0;
  // timeout_task_.destroy();
  timeout_task_.reset();
  timer_ = NULL;
  trace_info_.reset();
  xid_.reset();
}

void ObDistTransCtx::destroy()
{
  trans_2pc_timeout_ = -1;
  timeout_task_.destroy();
  if (timeout_task_.is_registered()) {
    // during the observer exiting caused by kill -15, when the ctx memory is released,
    // the scheduled task may not have been canceled
    TRANS_LOG(WARN, "timeout task is not unregistered", "context", *this);
  }
  ObTransCtx::destroy();
}

int ObDistTransCtx::set_xid(const ObString& gtrid_str, const ObString& bqual_str, const int64_t format_id)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  if (gtrid_str.empty() || gtrid_str.length() < 0 || bqual_str.length() < 0 ||
      gtrid_str.length() > ObXATransID::MAX_GTRID_LENGTH || bqual_str.length() > ObXATransID::MAX_BQUAL_LENGTH) {
    TRANS_LOG(WARN, "invalid xid arguments", K(gtrid_str), K(bqual_str), K(format_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (xid_.set(gtrid_str, bqual_str, format_id)) {
    TRANS_LOG(WARN, "set xid error", K(ret), K(gtrid_str), K(bqual_str), K(format_id));
  } else {
    TRANS_LOG(DEBUG, "set xid success", K(gtrid_str), K(bqual_str), K(format_id));
  }
  return ret;
}

int ObDistTransCtx::set_xid(const ObXATransID& xid)
{
  CtxLockGuard guard(lock_);
  return set_xid_(xid);
}

int ObDistTransCtx::set_scheduler(const ObAddr& scheduler)
{
  int ret = OB_SUCCESS;

  CtxLockGuard guard(lock_);

  if (OB_UNLIKELY(!scheduler.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(scheduler));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = set_scheduler_(scheduler);
  }

  return ret;
}

int ObDistTransCtx::set_coordinator(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;

  CtxLockGuard guard(lock_);

  if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = set_coordinator_(partition);
  }

  return ret;
}

int ObDistTransCtx::set_participants(const ObPartitionArray& participants)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);
  ret = set_participants_(participants);
  return ret;
}

int ObDistTransCtx::set_participant(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  CtxLockGuard guard(lock_);

  ObPartitionArray participants;
  if (OB_FAIL(participants.push_back(pkey))) {
    TRANS_LOG(WARN, "push participant error", KR(ret), K(pkey));
  } else {
    ret = set_participants_(participants);
  }

  return ret;
}

int ObDistTransCtx::get_participants_copy(ObPartitionArray& copy_participants)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(lock_.try_lock())) {
    TRANS_LOG(INFO, "get participants copy fail", K_(trans_id));
    // rewrite
    ret = OB_SUCCESS;
  } else {
    if (OB_SUCCESS != (ret = copy_participants.assign(participants_))) {
      TRANS_LOG(WARN, "ObDistTransCtx get participants copy error", K(ret), K(*this));
    }
    lock_.unlock();
  }

  return ret;
}

int ObDistTransCtx::set_scheduler_(const ObAddr& scheduler)
{
  int ret = OB_SUCCESS;
  scheduler_ = scheduler;
  return ret;
}

int ObDistTransCtx::set_coordinator_(const ObPartitionKey& coordinator)
{
  int ret = OB_SUCCESS;
  coordinator_ = coordinator;
  return ret;
}

int ObDistTransCtx::set_participants_(const ObPartitionArray& participants)
{
  int ret = OB_SUCCESS;
  ret = participants_.assign(participants);
  return ret;
}

int ObDistTransCtx::get_trans_location_leader_(
    const ObPartitionKey& partition, const bool nonblock, ObAddr& server, bool& partition_exist)
{
  int ret = OB_SUCCESS;
#ifdef TRANS_ERROR
  // random failure of sending message
  const int64_t random = ObRandom::rand(1, 100);
  if (0 == random % 20) {
    ret = OB_LOCATION_NOT_EXIST;
    // TRANS_LOG(WARN, "get location leader for random error",KR(ret), K(partition), "context", *this);
  }
#endif

  partition_exist = true;
  if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else {
    // If fail to get the partition leader of a transaction,
    // it will be retrieved from the global location cache again for any reason
    if (nonblock) {
      if (OB_FAIL(location_adapter_->nonblock_get_strong_leader(partition, server))) {
        TRANS_LOG(DEBUG, "nonblock get leader error", K(ret), K(partition));
        const int64_t cur_ts = ObTimeUtility::current_time();
        if (cur_ts - last_check_gc_ts_ > CHECK_GC_PARTITION_INTERVAL) {
          check_partition_exist_(partition, partition_exist);
          last_check_gc_ts_ = cur_ts;
        }
      }
    } else {
      if (OB_FAIL(location_adapter_->get_strong_leader(partition, server))) {
        TRANS_LOG(DEBUG, "get leader error", K(ret), K(partition));
        const int64_t cur_ts = ObTimeUtility::current_time();
        if (cur_ts - last_check_gc_ts_ > CHECK_GC_PARTITION_INTERVAL) {
          check_partition_exist_(partition, partition_exist);
          last_check_gc_ts_ = cur_ts;
        }
      }
    }
  }

  return ret;
}

void ObDistTransCtx::generate_request_id_()
{
  const int64_t request_id = ObClockGenerator::getClock();
  if (OB_INVALID_TIMESTAMP == request_id_ || request_id > request_id_) {
    request_id_ = request_id;
  } else {
    ++request_id_;
  }
}

int ObDistTransCtx::post_trans_msg_(const ObPartitionKey& partition, const ObTransMsg& msg, const int64_t type,
    const bool nonblock, bool& partition_exist)
{
  int ret = OB_SUCCESS;
  const bool need_refresh = need_refresh_location_;
  ObAddr server;

  partition_exist = true;
  if (trans_param_.is_bounded_staleness_read()) {
    ret = OB_ERR_UNEXPECTED;
    if (EXECUTE_COUNT_PER_SEC(10)) {
      TRANS_LOG(ERROR, "post msg for bounded staleness read, unexpected error", K(ret), K(msg));
    }
  } else {
    if (OB_FAIL(get_trans_location_leader_(partition, nonblock, server, partition_exist))) {
      if (EXECUTE_COUNT_PER_SEC(1)) {
        TRANS_LOG(WARN, "get partition leader error", KR(ret), K(partition), K(server));
        if (!partition_exist) {
          TRANS_LOG(WARN, "partition is not exist", K(partition), "context", *this);
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    uint64_t tenant_id = 0;
    if (is_inner_table(partition.get_table_id())) {
      tenant_id = OB_SYS_TENANT_ID;
    } else {
      tenant_id = partition.get_tenant_id();
    }
    if (!server.is_valid()) {
      TRANS_LOG(WARN, "invalid server", K(partition), K(server));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(post_trans_msg_(tenant_id, server, msg, type))) {
      TRANS_LOG(WARN, "post transaction message error", KR(ret), K(partition), K(server), K(msg));
    } else {
      //_FILL_TRACE_BUF(tlog_, "post trans msg, server=%s, type=%ld", to_cstring(server), type);
    }
  }

  // force refresh location cache
  if (need_refresh) {
    int tmp_ret = OB_SUCCESS;
    // The location cache is not forced to refresh, keep the old cache
    const int64_t expire_renew_time = 0;
    // Update the leader information of the partition
    if (OB_SUCCESS != (tmp_ret = location_adapter_->nonblock_renew(partition, expire_renew_time))) {
      TRANS_LOG(WARN, "nonblock renew error", "ret", tmp_ret, K(partition));
    } else {
      TRANS_LOG(DEBUG, "nonblock renew success", K(partition));
    }
  }
  return ret;
}

int ObDistTransCtx::post_trans_msg_(const ObPartitionKey& partition, const ObTrxMsgBase& msg, const int64_t type,
    const bool nonblock, bool& partition_exist)
{
  int ret = OB_SUCCESS;
  const bool need_refresh = need_refresh_location_;
  ObAddr server;

  partition_exist = true;
  if (trans_param_.is_bounded_staleness_read()) {
    ret = OB_ERR_UNEXPECTED;
    if (EXECUTE_COUNT_PER_SEC(10)) {
      TRANS_LOG(ERROR, "post msg for bounded staleness read, unexpected error", K(ret), K(msg));
    }
  } else {
    if (OB_FAIL(get_trans_location_leader_(partition, nonblock, server, partition_exist))) {
      if (EXECUTE_COUNT_PER_SEC(1)) {
        TRANS_LOG(WARN, "get partition leader error", K(ret), K(partition), K(server));
        if (!partition_exist) {
          TRANS_LOG(WARN, "partition is not exist", K(partition), "context", *this);
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    uint64_t tenant_id = 0;
    if (is_inner_table(partition.get_table_id())) {
      tenant_id = OB_SYS_TENANT_ID;
    } else {
      tenant_id = partition.get_tenant_id();
    }
    if (!server.is_valid()) {
      TRANS_LOG(WARN, "invalid server", K(partition), K(server));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(post_trans_msg_(tenant_id, server, msg, type))) {
      TRANS_LOG(WARN, "post transaction message error", K(ret), K(partition), K(server), K(msg));
    } else {
      //_FILL_TRACE_BUF(tlog_, "post trans msg, server=%s, type=%ld", to_cstring(server), type);
    }
  }

  // force refresh location cache
  if (need_refresh) {
    int tmp_ret = OB_SUCCESS;
    // The location cache is not forced to refresh, keep the old cache
    const int64_t expire_renew_time = 0;
    // Update the leader information of the partition
    if (OB_SUCCESS != (tmp_ret = location_adapter_->nonblock_renew(partition, expire_renew_time))) {
      TRANS_LOG(WARN, "nonblock renew error", "ret", tmp_ret, K(partition));
    } else {
      TRANS_LOG(DEBUG, "nonblock renew success", K(partition));
    }
  }
  return ret;
}

int ObDistTransCtx::post_trans_msg_(
    const uint64_t tenant_id, const ObAddr& server, const ObTransMsg& msg, const int64_t type)
{
  int ret = OB_SUCCESS;
  LocalTask* task = NULL;

  if (server == addr_ && !is_xa_local_trans()) {
    if (NULL == (task = LocalTaskFactory::alloc(type))) {
      TRANS_LOG(WARN, "alloc local task error");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      task->set_msg(msg);
      task_list_.add_last(task);
    }
  } else {
    ret = rpc_->post_trans_msg(tenant_id, server, msg, type);
  }

  return ret;
}

int ObDistTransCtx::post_trans_msg_(
    const uint64_t tenant_id, const ObAddr& server, const ObTrxMsgBase& msg, const int64_t type)
{
  int ret = OB_SUCCESS;
  LocalTask* task = NULL;

  if (server == addr_ && !is_xa_local_trans()) {
    if (NULL == (task = LocalTaskFactory::alloc(type))) {
      TRANS_LOG(WARN, "alloc local task error");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      task->set_msg(msg);
      task_list_.add_last(task);
    }
  } else {
    ret = rpc_->post_trans_msg(tenant_id, server, msg, type);
  }

  return ret;
}

int ObDistTransCtx::register_timeout_task_(const int64_t interval_us)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(interval_us < 0)) {
    TRANS_LOG(WARN, "invalid argument", K(interval_us));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(timer_)) {
    TRANS_LOG(ERROR, "transaction timer is null", K_(trans_id));
    ret = OB_ERR_UNEXPECTED;
    // add ref first, and then register the time task
  } else if (OB_ISNULL(partition_mgr_)) {
    TRANS_LOG(ERROR, "partition mgr is null, unexpected error", KP_(partition_mgr), K_(trans_id));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(partition_mgr_->acquire_ctx_ref(trans_id_))) {
    TRANS_LOG(WARN, "get transaction ctx for inc ref error", KR(ret), K_(trans_id));
  } else {
    if (OB_FAIL(timer_->register_timeout_task(timeout_task_, interval_us))) {
      TRANS_LOG(WARN, "register timeout task error", KR(ret), K(interval_us), K_(trans_id));
      // in case of registration failure, you need to cancel ref
      (void)partition_mgr_->release_ctx_ref(this);
    }
  }

  return ret;
}

int ObDistTransCtx::unregister_timeout_task_()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(timer_)) {
    TRANS_LOG(ERROR, "transaction timer is null", K_(trans_id));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(timer_->unregister_timeout_task(timeout_task_))) {
    // rewrite ret
    if (OB_TIMER_TASK_HAS_NOT_SCHEDULED == ret) {
      ret = OB_SUCCESS;
    }
  } else {
    // just dec ctx ref
    if (NULL == partition_mgr_) {
      TRANS_LOG(ERROR, "partition mgr is null, unexpected error", KP_(partition_mgr), K_(trans_id));
      ret = OB_ERR_UNEXPECTED;
    } else {
      (void)partition_mgr_->release_ctx_ref(this);
    }
  }

  return ret;
}

void ObDistTransCtx::update_trans_2pc_timeout_()
{
  const int64_t timeout_new = 2 * trans_2pc_timeout_;

  if (MAX_TRANS_2PC_TIMEOUT_US > timeout_new) {
    trans_2pc_timeout_ = timeout_new;
  } else {
    trans_2pc_timeout_ = MAX_TRANS_2PC_TIMEOUT_US;
  }
}

int ObDistTransCtx::set_app_trace_info_(const ObString& app_trace_info)
{
  int ret = OB_SUCCESS;
  const int64_t len = app_trace_info.length();

  if (OB_UNLIKELY(len < 0) || OB_UNLIKELY(len > OB_MAX_TRACE_ID_BUFFER_SIZE)) {
    TRANS_LOG(WARN, "invalid argument", K(app_trace_info), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (0 == trace_info_.get_app_trace_info().length()) {
    // set for the first time
    if (OB_FAIL(trace_info_.set_app_trace_info(app_trace_info))) {
      TRANS_LOG(WARN, "set app trace info error", K(ret), K(app_trace_info), K(*this));
    }
  } else if (trace_info_.get_app_trace_info().length() != app_trace_info.length()) {
    // in big trans case, leader may change if redo log is not persisted successfully
    TRANS_LOG(WARN, "different app trace info", K(ret), K(app_trace_info), "context", *this, K(lbt()));
  } else {
    // do nothing
  }
  return ret;
}

// this func is used to switch the old message type to new type
int64_t ObDistTransCtx::msg_type_switch_(const int64_t msg_type)
{
  int64_t ret = msg_type;

  if (!cluster_version_after_2250_()) {
    // do nothing
  } else if (OB_TRANS_2PC_CLEAR_REQUEST == msg_type) {
    ret = OB_TRX_2PC_CLEAR_REQUEST;
  } else if (OB_TRANS_2PC_ABORT_REQUEST == msg_type) {
    ret = OB_TRX_2PC_ABORT_REQUEST;
  } else if (OB_TRANS_2PC_COMMIT_REQUEST == msg_type) {
    ret = OB_TRX_2PC_COMMIT_REQUEST;
  } else if (OB_TRANS_2PC_CLEAR_RESPONSE == msg_type) {
    ret = OB_TRX_2PC_CLEAR_RESPONSE;
  } else if (OB_TRANS_2PC_ABORT_RESPONSE == msg_type) {
    ret = OB_TRX_2PC_ABORT_RESPONSE;
  } else if (OB_TRANS_2PC_COMMIT_RESPONSE == msg_type) {
    ret = OB_TRX_2PC_COMMIT_RESPONSE;
  } else if (OB_TRANS_2PC_PRE_COMMIT_REQUEST == msg_type) {
    ret = OB_TRX_2PC_PRE_COMMIT_REQUEST;
  } else if (OB_TRANS_2PC_PRE_COMMIT_RESPONSE == msg_type) {
    ret = OB_TRX_2PC_PRE_COMMIT_RESPONSE;
  } else if (OB_TRANS_2PC_PREPARE_REQUEST == msg_type) {
    ret = OB_TRX_2PC_PREPARE_REQUEST;
  } else if (OB_TRANS_2PC_PREPARE_RESPONSE == msg_type) {
    ret = OB_TRX_2PC_PREPARE_RESPONSE;
  }

  return ret;
}

int ObDistTransCtx::set_app_trace_id_(const ObString& app_trace_id)
{
  int ret = OB_SUCCESS;
  const int64_t len = app_trace_id.length();

  if (OB_UNLIKELY(len < 0) || OB_UNLIKELY(len > OB_MAX_TRACE_ID_BUFFER_SIZE)) {
    TRANS_LOG(WARN, "invalid argument", K(app_trace_id), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (0 == trace_info_.get_app_trace_id().length()) {
    // set for the first time
    if (OB_FAIL(trace_info_.set_app_trace_id(app_trace_id))) {
      TRANS_LOG(WARN, "set app trace id error", K(ret), K(app_trace_id), K(*this));
    }
  } else if (trace_info_.get_app_trace_id().length() != app_trace_id.length()) {
    // in big trans case, leader may change if redo log is not persisted successfully
    TRANS_LOG(WARN, "different app trace id", K(ret), K(app_trace_id), "context", *this);
  } else {
    // do nothing
  }
  return ret;
}

int ObDistTransCtx::set_xid_(const ObXATransID& xid)
{
  int ret = OB_SUCCESS;
  if (!xid.is_valid()) {
    TRANS_LOG(WARN, "invalid xid", K(xid));
    ret = OB_INVALID_ARGUMENT;
  } else {
    xid_ = xid;
  }
  return ret;
}

}  // namespace transaction
}  // namespace oceanbase
