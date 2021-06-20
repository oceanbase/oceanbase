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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_CTX_
#define OCEANBASE_TRANSACTION_OB_TRANS_CTX_

#include "share/ob_define.h"
#include "lib/lock/ob_small_spin_lock.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/oblog/ob_trace_log.h"
#include "lib/list/ob_dlist.h"
#include "lib/list/ob_dlink_node.h"
#include "share/ob_cluster_version.h"
#include "ob_location_adapter.h"
#include "ob_trans_rpc.h"
#include "ob_clog_adapter.h"
#include "ob_trans_define.h"
#include "ob_trans_timer.h"
#include "ob_trans_result.h"
#include "ob_trans_log.h"
#include "ob_trans_msg.h"
#include "ob_trans_msg2.h"
#include "ob_trans_submit_log_cb.h"
#include "ob_mask_set.h"
#include "ob_trans_version_mgr.h"
#include "ob_trans_end_trans_callback.h"
#include "ob_trans_ctx_lock.h"
#include "ob_trans_audit_record_mgr.h"
#include "storage/memtable/ob_memtable_context.h"

namespace oceanbase {

namespace common {
class ObPartitionKey;
class ObAddr;
class ObMaskSet;
}  // namespace common

namespace storage {
class LeaderActiveArg;
}

namespace memtable {
class ObIMemtableCtxFactory;
class ObIMemtableCtx;
};  // namespace memtable

namespace transaction {
class ObITransCtxMgr;
class ObTransMsg;
class ObTransService;
class ObITsMgr;
class KillTransArg;
}  // namespace transaction

namespace transaction {
#define DISCARD_MESSAGE(msg) discard_message((msg))

#define REC_TRANS_TRACE(recorder_ptr, trace_event) \
  do {                                             \
    if (NULL != recorder_ptr) {                    \
      REC_TRACE(*recorder_ptr, trace_event);       \
    }                                              \
  } while (0)

#define REC_TRANS_TRACE_EXT(recorder_ptr, trace_event, pairs...) \
  do {                                                           \
    if (NULL != recorder_ptr) {                                  \
      REC_TRACE_EXT(*recorder_ptr, trace_event, ##pairs);        \
    }                                                            \
  } while (0)

static inline void discard_message(const ObTransMsg& msg)
{
  if (!msg.is_valid()) {
    TRANS_LOG(WARN, "invalid argumet", K(msg));
  } else {
    TRANS_LOG(WARN, "discard message", K(msg));
  }
}

static inline void protocol_error(const int64_t state, const int64_t msg_type)
{
  if (!ObTransMsgTypeChecker::is_valid_msg_type(msg_type)) {
    TRANS_LOG(WARN, "invalid argumet", K(state), K(msg_type));
  } else {
    TRANS_LOG(ERROR, "protocol error", K(state), K(msg_type), "lbt", lbt());
  }
}

typedef common::LinkHashNode<ObTransKey> TransCtxHashNode;
typedef common::LinkHashValue<ObTransKey> TransCtxHashValue;

class ObTransState {
public:
  ObTransState()
  {
    reset();
  }
  ~ObTransState()
  {
    reset();
  }
  void reset()
  {
    prepare_version_ = ObTransVersion::INVALID_TRANS_VERSION;
    state_ = Ob2PCState::UNKNOWN;
  }
  void set_state(const int64_t state);
  void set_prepare_version(const int64_t prepare_version);
  void get_state_and_version(int64_t& state, int64_t& prepare_version);
  int64_t get_state() const
  {
    return state_;
  }
  int64_t get_prepare_version() const
  {
    return prepare_version_;
  }
  VIRTUAL_TO_STRING_KV(K_(prepare_version), K_(state));

public:
  common::ObByteLock lock_;
  int64_t prepare_version_;
  int64_t state_;
};

// NOTICE: You should **CHANGE** the signature of all inherit class once you
// change one of the signature of `ObTransCtx`.
// For Example: If you change the signature of the function `commit` in
// `ObTransCtx`, you should also modify the signature of function `commit` in
// `ObPartTransCtx`, `ObSlaveTransCtx`, `ObScheTransCtx` and `ObCoordTransCtx`
class ObTransCtx : public TransCtxHashValue {
  friend class CtxLock;

protected:
  class TransAuditRecordMgrGuard {
  public:
    TransAuditRecordMgrGuard() : with_tenant_ctx_(NULL){};
    ~TransAuditRecordMgrGuard()
    {
      destroy();
    }
    int set_tenant_id(uint64_t tenant_id);
    void destroy();
    ObTransAuditRecordMgr* get_trans_audit_record_mgr();

  private:
    share::ObTenantSpaceFetcher* with_tenant_ctx_;
    char buf_[sizeof(share::ObTenantSpaceFetcher)];
  };

public:
  ObTransCtx(const char* ctx_type_str = "unknow", const int64_t ctx_type = ObTransCtxType::UNKNOWN)
      : magic_number_(UNKNOWN_RESET_CTX_MAGIC_NUM),
        ctx_type_str_(ctx_type_str),
        ctx_type_(ctx_type),
        is_inited_(false),
        tenant_id_(OB_INVALID_TENANT_ID),
        ctx_mgr_(NULL),
        trans_expired_time_(0),
        ctx_create_time_(0),
        trans_service_(NULL),
        tlog_(NULL),
        state_(),
        status_(OB_SUCCESS),
        cluster_version_(0),
        partition_mgr_(NULL),
        trans_type_(TransType::DIST_TRANS),
        session_id_(UINT32_MAX),
        proxy_session_id_(UINT32_MAX),
        stc_(0),
        part_trans_action_(ObPartTransAction::UNKNOWN),
        trans_audit_record_(NULL),
        pending_callback_param_(common::OB_SUCCESS),
        p_mt_ctx_(NULL),
        replay_clear_clog_ts_(0),
        is_dup_table_trans_(false),
        is_exiting_(false),
        is_readonly_(false),
        for_replay_(false),
        need_print_trace_log_(false),
        is_bounded_staleness_read_(false),
        has_pending_callback_(false),
        need_record_rollback_trans_log_(false),
        can_elr_(false),
        xa_ref_count_(0)
  {}
  virtual ~ObTransCtx()
  {
    destroy();
  }
  // rpc. send transaction message by rpc interface
  // location adapter. the adapter between transaction engine and location cache
  // ctx_mgr. the transaction context manager
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t trans_expired_time,
      const common::ObPartitionKey& self, ObITransCtxMgr* ctx_mgr, const ObStartTransParam& trans_param,
      const uint64_t cluster_version, ObTransService* trans_service);
  virtual void destroy();
  void reset();

public:
  void get_ctx_guard(CtxLockGuard& guard);
  void print_trace_log();
  // ATTENTION! There is no lock protect
  void set_need_print_trace_log()
  {
    need_print_trace_log_ = true;
  }
  bool is_too_slow_transaction() const
  {
    return ObClockGenerator::getRealClock() >= ctx_create_time_ + OB_TRANS_WARN_USE_TIME;
  }
  void set_readonly(const bool is_readonly)
  {
    is_readonly_ = is_readonly;
  }
  bool is_readonly() const
  {
    return is_readonly_;
  }
  void set_bounded_staleness_read(const bool is_bounded_staleness_read)
  {
    is_bounded_staleness_read_ = is_bounded_staleness_read;
  }
  bool is_bounded_staleness_read() const
  {
    return is_bounded_staleness_read_;
  }
  void set_for_replay(const bool for_replay)
  {
    for_replay_ = for_replay;
  }
  bool is_for_replay() const
  {
    return for_replay_;
  }
  const common::ObPartitionKey& get_partition() const
  {
    return self_;
  }
  const char* get_ctx_type() const
  {
    return ctx_type_str_;
  }
  int set_partition_trans_ctx_mgr(ObPartitionTransCtxMgr* partition_mgr);
  ObPartitionTransCtxMgr* get_partition_mgr()
  {
    return partition_mgr_;
  }
  ObTransService* get_trans_service()
  {
    return trans_service_;
  }
  void set_trans_type(const int32_t trans_type)
  {
    trans_type_ = trans_type;
  }
  int get_trans_type() const
  {
    return trans_type_;
  }
  bool is_mini_sp_trans() const
  {
    return is_mini_sp_trans_();
  }
  int set_partition(const ObPartitionKey& partition);
  int set_session_id(const uint32_t session_id)
  {
    session_id_ = session_id;
    return common::OB_SUCCESS;
  }
  uint32_t get_session_id() const
  {
    return session_id_;
  }
  int set_proxy_session_id(const uint64_t proxy_session_id)
  {
    proxy_session_id_ = proxy_session_id;
    return common::OB_SUCCESS;
  }
  uint64_t get_proxy_session_id() const
  {
    return proxy_session_id_;
  }
  int get_status() const
  {
    return status_;
  }
  void before_unlock(CtxLockArg& arg);
  void after_unlock(CtxLockArg& arg);
  void set_dup_table_trans(bool is_dup_table_trans)
  {
    is_dup_table_trans_ = is_dup_table_trans;
  }
  bool is_dup_table_trans() const
  {
    return is_dup_table_trans_;
  }
  bool is_sp_trans() const
  {
    return is_sp_trans_();
  }
  bool is_can_elr() const
  {
    return can_elr_;
  }
  bool is_elr_prepared() const
  {
    return is_elr_prepared_();
  }

public:
  bool is_exiting() const
  {
    return is_exiting_;
  }
  int64_t get_trans_expired_time() const
  {
    return trans_expired_time_;
  }
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  const ObTransID& get_trans_id() const
  {
    return trans_id_;
  }
  uint64_t get_cluster_version() const
  {
    return cluster_version_;
  }
  int64_t get_ctx_create_time() const
  {
    return ctx_create_time_;
  }
  const ObAddr& get_addr() const
  {
    return addr_;
  }
  virtual int64_t get_part_trans_action() const
  {
    return part_trans_action_;
  }
  const ObStartTransParam& get_trans_param() const
  {
    return trans_param_;
  }
  int64_t get_type()
  {
    return ctx_type_;
  }
  TransAuditRecordMgrGuard& get_record_mgr_guard()
  {
    return record_mgr_guard_;
  }
  int reset_trans_audit_record();

public:
  virtual bool is_inited() const = 0;
  virtual int handle_timeout(const int64_t delay) = 0;
  virtual int kill(const KillTransArg& arg, ObEndTransCallbackArray& cb_array) = 0;
  virtual int leader_takeover(const int64_t checkpoint) = 0;
  virtual int leader_active(const storage::LeaderActiveArg& arg) = 0;
  virtual int leader_revoke(const bool first_check, bool& need_release, ObEndTransCallbackArray& cb_array) = 0;
  virtual bool can_be_freezed() const = 0;
  virtual int commit(const bool is_rollback, sql::ObIEndTransCallback* cb, bool is_readonly,
      const MonotonicTs commit_time, const int64_t stmt_expired_time, const ObStmtRollbackInfo& stmt_rollback_info,
      const common::ObString& app_trace_info, bool& need_convert_to_dist_trans) = 0;
  virtual void set_exiting_();
  virtual bool is_dirty() const
  {
    return false;
  }

public:
  VIRTUAL_TO_STRING_KV(KP(this), K_(ctx_type), K_(trans_id), K_(tenant_id), K_(is_exiting), K_(trans_type),
      K_(is_readonly), K_(trans_expired_time), K_(self), K_(state), K_(cluster_version), K_(trans_need_wait_wrap),
      K_(trans_param), K_(can_elr), "uref", get_uref(), K_(ctx_create_time));

protected:
  int set_trans_param_(const ObStartTransParam& trans_param);
  // There was a problem in the online pre delivery environment.
  // After timeout of transaction that did not enter 2pc, the time task did not clean it up,
  // resulting in ctx remaining.
  // Reason: there is time drift (5.7s). Therefore, false is retuned for checking trans timeout,
  //         ctx is not released. If we use system clock, the problem can be avoided.
  //         This is because clog generator use system clock for time task.
  void set_exiting_(bool is_dirty);
  void print_trace_log_();
  void print_trace_log_if_necessary_();
  bool is_trans_expired_() const
  {
    return ObClockGenerator::getRealClock() >= trans_expired_time_;
  }
  bool is_slow_query_() const;
  void set_state_(const int64_t state)
  {
    state_.set_state(state);
  }
  int64_t get_state_() const
  {
    return state_.get_state();
  }
  void set_status_(const int status)
  {
    status_ = status;
  }
  int get_status_() const
  {
    return status_;
  }
  bool cluster_version_after_2200_() const
  {
    return cluster_version_ >= CLUSTER_VERSION_2200;
  }
  bool cluster_version_after_2230_() const
  {
    return cluster_version_ >= CLUSTER_VERSION_2230;
  }
  bool cluster_version_after_2250_() const
  {
    return cluster_version_ >= CLUSTER_VERSION_2250;
  }
  bool cluster_version_before_2271_() const
  {
    return cluster_version_ < CLUSTER_VERSION_2271;
  }
  bool is_sp_trans_() const
  {
    return trans_type_ == TransType::SP_TRANS;
  }
  bool is_mini_sp_trans_() const
  {
    return trans_type_ == TransType::MINI_SP_TRANS;
  }
  void set_stc_(const MonotonicTs stc);
  void set_stc_by_now_();
  MonotonicTs get_stc() const
  {
    return stc_;
  }
  void check_partition_exist_(const common::ObPartitionKey& pkey, bool& partition_exist);
  ObITsMgr* get_ts_mgr_();
  void end_trans_callback_(const bool is_rollback, const int retcode);
  void end_trans_callback_(const int cb_param);
  int64_t get_remaining_wait_interval_us_()
  {
    return trans_need_wait_wrap_.get_remaining_wait_interval_us();
  }
  void set_trans_need_wait_wrap_(const MonotonicTs receive_gts_ts, const int64_t need_wait_interval_us)
  {
    trans_need_wait_wrap_.set_trans_need_wait_wrap(receive_gts_ts, need_wait_interval_us);
  }
  void set_elr_preparing_()
  {
    ATOMIC_STORE(&elr_prepared_state_, ELRState::ELR_PREPARING);
  }
  bool is_elr_preparing_() const
  {
    return ELRState::ELR_PREPARING == ATOMIC_LOAD(&elr_prepared_state_);
  }
  void set_elr_prepared_()
  {
    ATOMIC_STORE(&elr_prepared_state_, ELRState::ELR_PREPARED);
  }
  bool is_elr_prepared_() const
  {
    return ELRState::ELR_PREPARED == ATOMIC_LOAD(&elr_prepared_state_);
  }
  void reset_elr_state_()
  {
    ATOMIC_STORE(&elr_prepared_state_, ELRState::ELR_INIT);
  }
  void remove_trans_table_();

public:
  // for resource pool
  // assume the size of transaction context(Scheduler/Coordinator/Participant) is about 200B,
  // then the total size of trans_ctx preallocated by resource pool is 200B * 100 * 1000 = 20MB.
  // Taking the concurrency num into consideration, obviously, it is appropriate.
  static const int64_t RP_TOTAL_NUM = 100 * 1000;

protected:
  static const int64_t MAX_TRANS_2PC_TIMEOUT_US = 3 * 1000 * 1000;  // 3s
  // if 600 seconds after trans timeout, warn is required
  static const int64_t OB_TRANS_WARN_USE_TIME = 600 * 1000 * 1000;
  // 0x0078746365657266 means freectx
  static const int64_t UNKNOWN_FREE_CTX_MAGIC_NUM = 0x0078746365657266;
  // 0x006e776f6e6b6e75 means resetctx
  static const int64_t UNKNOWN_RESET_CTX_MAGIC_NUM = 0x006e776f6e6b6e75;
  static const int64_t CHECK_GC_PARTITION_INTERVAL = 10 * 1000 * 1000;
  // the time interval of asking scheduler for participant
  static const int64_t CHECK_SCHEDULER_STATUS_INTERVAL = 10 * 1000 * 1000;
  // the time interval of asking scheduler for rs
  static const int64_t CHECK_RS_SCHEDULER_STATUS_INTERVAL = 60 * 1000 * 1000;

private:
  int alloc_audit_rec_and_trace_log_(ObTransService* trans_service, ObTransTraceLog*& trace_log);

private:
  DISALLOW_COPY_AND_ASSIGN(ObTransCtx);

protected:
  TransAuditRecordMgrGuard record_mgr_guard_;
  int64_t magic_number_;
  const char* const ctx_type_str_;
  int64_t ctx_type_;
  bool is_inited_;
  uint64_t tenant_id_;
  common::ObPartitionKey self_;
  ObTransID trans_id_;
  ObITransCtxMgr* ctx_mgr_;
  ObStartTransParam trans_param_;
  common::ObAddr addr_;
  // the expired time of a transaction for checking transaction timeout
  int64_t trans_expired_time_;
  int64_t ctx_create_time_;
  ObTransService* trans_service_;
  mutable CtxLock lock_;
  ObTransTraceLog* tlog_;
  LocalTaskList task_list_;
  ObTransState state_;
  int status_;
  uint64_t cluster_version_;
  ObPartitionTransCtxMgr* partition_mgr_;
  int32_t trans_type_;
  uint32_t session_id_;
  uint64_t proxy_session_id_;
  MonotonicTs stc_;
  int64_t last_check_gc_ts_;
  // the variable is used to record the action of the current transaction in the stmt execution
  int64_t part_trans_action_;
  ObTransAuditRecord* trans_audit_record_;
  ObEndTransCallback end_trans_cb_;
  ObTransNeedWaitWrap trans_need_wait_wrap_;
  int pending_callback_param_;
  // whether it is ready for elr
  int32_t elr_prepared_state_;
  // it is used to wake up the lock queue after submitting the log of elr trans
  memtable::ObMemtableCtx* p_mt_ctx_;
  int64_t replay_clear_clog_ts_;
  bool is_dup_table_trans_;
  bool is_exiting_;
  bool is_readonly_;
  bool for_replay_;
  bool need_print_trace_log_;
  bool is_bounded_staleness_read_;
  bool has_pending_callback_;
  // the variable is used to indicate whether the rollback transaction log needs to be recorded
  bool need_record_rollback_trans_log_;
  // whether the trans can release locks early
  bool can_elr_;
  int64_t xa_ref_count_;
};

class TransCtxAlloc {
public:
  ObTransCtx* alloc_value()
  {
    return NULL;
  }
  void free_value(ObTransCtx* ctx)
  {
    if (NULL != ctx) {
      ObTransCtxFactory::release(ctx);
    }
  }
  TransCtxHashNode* alloc_node(ObTransCtx* ctx)
  {
    UNUSED(ctx);
    return op_alloc(TransCtxHashNode);
  }
  void free_node(TransCtxHashNode* node)
  {
    if (NULL != node) {
      op_free(node);
      node = NULL;
    }
  }
};

class ObDistTransCtx : public ObTransCtx {
  friend class CtxLock;
  friend class IterateTransStatFunctor;

public:
  explicit ObDistTransCtx(const char* ctx_type_str, const int64_t ctx_type)
      : ObTransCtx(ctx_type_str, ctx_type),
        participants_(ObModIds::OB_TRANS_PARTITION_ARRAY, OB_MALLOC_NORMAL_BLOCK_SIZE),
        request_id_(0),
        rpc_(NULL),
        location_adapter_(NULL),
        commit_start_time_(0),
        trans_start_time_(0),
        need_refresh_location_(false),
        trans_2pc_timeout_(0),
        timer_(NULL)
  {}
  virtual ~ObDistTransCtx()
  {
    destroy();
  }
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t trans_expired_time,
      const common::ObPartitionKey& self, ObITransCtxMgr* ctx_mgr, const ObStartTransParam& trans_param,
      const uint64_t cluster_version, ObTransService* trans_service);
  virtual void destroy();
  void reset();

public:
  int set_scheduler(const common::ObAddr& scheduler);
  int set_coordinator(const common::ObPartitionKey& coordinator);
  int set_participants(const common::ObPartitionArray& participants);
  int set_participant(const common::ObPartitionKey& pkey);
  int get_participants_copy(common::ObPartitionArray& copy_participants);
  const ObPartitionKey& get_coordinator() const
  {
    return coordinator_;
  }
  ObITransRpc* get_trans_rpc() const
  {
    return rpc_;
  }
  const common::ObAddr& get_scheduler() const
  {
    return scheduler_;
  }
  int set_xid(const common::ObString& gtrid_str, const common::ObString& bqual_str, const int64_t format_id);
  int set_xid(const ObXATransID& xid);
  const ObXATransID& get_xid() const
  {
    return xid_;
  }
  bool is_xa_local_trans() const
  {
    return !xid_.empty();
  }

public:
  INHERIT_TO_STRING_KV("ObTransCtx", ObTransCtx, K_(scheduler), K_(coordinator), K_(participants), K_(request_id),
      K_(timeout_task), K_(xid));

protected:
  int set_scheduler_(const common::ObAddr& scheduler);
  int set_coordinator_(const common::ObPartitionKey& coordinator);
  int set_participants_(const common::ObPartitionArray& participants);
  bool is_single_partition_() const
  {
    return 1 == participants_.count();
  }
  int post_trans_msg_(const common::ObPartitionKey& partition, const ObTransMsg& msg, const int64_t type,
      const bool nonblock, bool& partition_exist);
  int post_trans_msg_(
      const uint64_t tenant_id, const common::ObAddr& server, const ObTransMsg& msg, const int64_t type);
  int post_trans_msg_(const common::ObPartitionKey& partition, const ObTrxMsgBase& msg, const int64_t type,
      const bool nonblock, bool& partition_exist);
  int post_trans_msg_(
      const uint64_t tenant_id, const common::ObAddr& server, const ObTrxMsgBase& msg, const int64_t type);
  // get the partition leader information from cache in the transaction module
  int get_trans_location_leader_(
      const ObPartitionKey& partition, const bool nonblock, ObAddr& addr, bool& partition_exist);
  int register_timeout_task_(const int64_t interval_us);
  int unregister_timeout_task_();
  void update_trans_2pc_timeout_();
  void generate_request_id_();
  int set_app_trace_info_(const ObString& app_trace_info);
  int64_t msg_type_switch_(const int64_t msg_type);
  int set_app_trace_id_(const ObString& app_trace_id);
  int set_xid_(const ObXATransID& xid);

private:
  DISALLOW_COPY_AND_ASSIGN(ObDistTransCtx);

protected:
  common::ObAddr scheduler_;
  common::ObPartitionKey coordinator_;
  common::ObPartitionArray participants_;
  int64_t request_id_;
  ObITransRpc* rpc_;
  ObILocationAdapter* location_adapter_;
  int64_t commit_start_time_;
  // the variable is used to get the time between the trans start and response to client
  int64_t trans_start_time_;
  bool need_refresh_location_;
  int64_t trans_2pc_timeout_;
  ObTransTimeoutTask timeout_task_;
  ObITransTimer* timer_;
  ObTraceInfo trace_info_;
  // char xid_buffer_[ObTransDesc::MAX_XID_LENGTH];
  ObXATransID xid_;
};

}  // namespace transaction
}  // namespace oceanbase

#endif  // OCEANBASE_TRANSACTION_OB_TRANS_CTX_
