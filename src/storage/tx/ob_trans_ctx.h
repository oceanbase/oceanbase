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
#include "ob_trans_define.h"
#include "ob_trans_timer.h"
#include "ob_trans_result.h"
#include "ob_trans_submit_log_cb.h"
#include "ob_trans_version_mgr.h"
#include "ob_trans_end_trans_callback.h"
#include "ob_trans_ctx_lock.h"
#include "storage/memtable/ob_memtable_context.h"
#include "ob_xa_define.h"
#include "share/rc/ob_context.h"
#include "share/ob_light_hashmap.h"
#include "ob_tx_elr_handler.h"
#include "storage/tx/ob_tx_on_demand_print.h"

namespace oceanbase
{

namespace common
{
class ObAddr;
}

namespace storage
{
class LeaderActiveArg;
}

namespace memtable
{
class ObIMemtableCtxFactory;
class ObIMemtableCtx;
};

namespace transaction
{
class ObITransCtxMgr;
class ObTransService;
class ObITsMgr;
class KillTransArg;
class ObLSTxCtxMgr;
}

namespace transaction
{
static inline void protocol_error(const int64_t state, const int64_t msg_type)
{
  // if (!ObTransMsgTypeChecker::is_valid_msg_type(msg_type)) {
  //   TRANS_LOG(WARN, "invalid argumet", K(state), K(msg_type));
  // } else {
  //   TRANS_LOG(ERROR, "protocol error", K(state), K(msg_type), "lbt", lbt());
  // }
}

// NOTICE: You should **CHANGE** the signature of all inherit class once you
// change one of the signature of `ObTransCtx`.
// For Example: If you change the signature of the function `commit` in
// `ObTransCtx`, you should also modify the signatore of function `commit` in
// `ObPartTransCtx`, `ObScheTransCtx`
class ObTransCtx: public share::ObLightHashLink<ObTransCtx>
{
  friend class CtxLock;
public:
  ObTransCtx(const char *ctx_type_str = "unknow", const int64_t ctx_type = ObTransCtxType::UNKNOWN)
      : tenant_id_(OB_INVALID_TENANT_ID),
      trans_expired_time_(0), ctx_create_time_(0),
      trans_service_(NULL), tlog_(NULL),
      cluster_version_(0), ls_tx_ctx_mgr_(NULL),
      session_id_(UINT32_MAX),
      associated_session_id_(UINT32_MAX),
      stc_(0), part_trans_action_(ObPartTransAction::UNKNOWN),
      callback_scheduler_on_clear_(false),
      pending_callback_param_(common::OB_SUCCESS), p_mt_ctx_(NULL),
      is_exiting_(false), for_replay_(false),
      need_retry_redo_sync_by_task_(false),
      has_pending_callback_(false),
      can_elr_(false),
      opid_(0) {}
  virtual ~ObTransCtx() { }
  void reset() { }
public:
  void get_ctx_guard(CtxLockGuard &guard, uint8_t mode = CtxLockGuard::MODE::ALL);
  void print_trace_log();
  // ATTENTION! There is no lock protect
  bool is_too_long_transaction() const
  { return ObClockGenerator::getRealClock() >= ctx_create_time_ + OB_TRANS_WARN_USE_TIME; }
  bool is_readonly() const { return false; }
  void set_for_replay(const bool for_replay) { for_replay_ = for_replay; }
  bool is_for_replay() const { return for_replay_; }
  void set_need_retry_redo_sync_by_task_() { need_retry_redo_sync_by_task_ = true; }
  const share::ObLSID &get_ls_id() const { return ls_id_; }
  int set_ls_tx_ctx_mgr(ObLSTxCtxMgr *ls_tx_ctx_mgr)
  {
    ls_tx_ctx_mgr_ = ls_tx_ctx_mgr;
    return common::OB_SUCCESS;
  }
  ObLSTxCtxMgr *get_ls_tx_ctx_mgr() { return ls_tx_ctx_mgr_; }
  ObTransService *get_trans_service() { return trans_service_; }
  int set_ls_id(const share::ObLSID &ls_id)
  {
    ls_id_ = ls_id;
    return common::OB_SUCCESS;
  }
  int set_trans_id(const ObTransID &trans_id)
  {
    int ret = OB_SUCCESS;
    if (!trans_id.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      trans_id_ = trans_id;
    }
    return ret;
  }
  bool contain(const ObTransID trans_id) { return trans_id_ == trans_id; }
  int set_session_id(const uint32_t session_id) { session_id_ = session_id; return common::OB_SUCCESS; }
  uint32_t get_session_id() const { return session_id_; }
  uint32_t get_associated_session_id() const { return associated_session_id_; }
  void before_unlock(CtxLockArg &arg);
  void after_unlock(CtxLockArg &arg);
public:
  void set_exiting() { is_exiting_ = true; }
  bool is_exiting() const { return is_exiting_; }
  int64_t get_trans_expired_time() const { return trans_expired_time_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  const ObTransID &get_trans_id() const { return trans_id_; }
  uint64_t get_cluster_version() const { return cluster_version_; }
  int64_t get_ctx_create_time() const { return ctx_create_time_; }
  int64_t get_trans_start_time() const { return ctx_create_time_; }// TODO: return real trans start time
  const ObAddr &get_addr() const { return addr_; }
  virtual int64_t get_part_trans_action() const { return part_trans_action_; }
  int acquire_ctx_ref() { return acquire_ctx_ref_(); }
  void release_ctx_ref();
  ObITransRpc *get_trans_rpc() const { return rpc_; }
public:
  virtual bool is_inited() const = 0;
  virtual int handle_timeout(const int64_t delay) = 0;
  virtual int kill(const KillTransArg &arg, ObTxCommitCallback *&cb_list) = 0;
  // thread unsafe
  VIRTUAL_TO_STRING_KV(KP(this), K_(ref),
                       K_(trans_id),
                       K_(tenant_id),
                       K_(is_exiting),
                       K_(trans_expired_time),
                       K_(cluster_version),
                       K_(trans_need_wait_wrap),
                       K_(stc),
                       K_(ctx_create_time));
protected:
  void set_exiting_();
  void print_trace_log_();
  void print_trace_log_if_necessary_();
  bool is_trans_expired_() const { return ObClockGenerator::getRealClock() >= trans_expired_time_; }
  bool is_slow_query_() const;
  void set_stc_(const MonotonicTs stc);
  void set_stc_by_now_();
  MonotonicTs get_stc_();
  ObITsMgr *get_ts_mgr_();
  bool has_callback_scheduler_();
  int defer_callback_scheduler_(const int ret, const share::SCN &commit_version);
  int prepare_commit_cb_for_role_change_(const int cb_ret, ObTxCommitCallback *&cb_list);
  int64_t get_remaining_wait_interval_us_()
  {
    return trans_need_wait_wrap_.get_remaining_wait_interval_us();
  }
  void set_trans_need_wait_wrap_(const MonotonicTs receive_gts_ts,
                                 const int64_t need_wait_interval_us)
  {
    trans_need_wait_wrap_.set_trans_need_wait_wrap(receive_gts_ts,
                                                   need_wait_interval_us);
  }
  int acquire_ctx_ref_()
  {
    ObLightHashLink::inc_ref(1);
    TRANS_LOG(DEBUG, "inc tx ctx ref", KPC(this));
    return OB_SUCCESS;
  }
  void release_ctx_ref_()
  {
    ObLightHashLink::dec_ref(1);
    TRANS_LOG(DEBUG, "dec tx ctx ref", KPC(this));
  }
  virtual int register_timeout_task_(const int64_t interval_us);
  virtual int unregister_timeout_task_();
  void generate_request_id_();
  void update_trans_2pc_timeout_();
  int set_app_trace_info_(const ObString &app_trace_info);
  int set_app_trace_id_(const ObString &app_trace_id);
public:
  // for resource pool
  // assume the size of transaction context(Scheduler/Coordinator/Participant) is about 200B,
  // then the total size of trans_ctx preallocated by resource pool is 200B * 100 * 1000 = 20MB.
  // Taking the concurrency num into consideration, obviously, it is appropriate.
  static const int64_t RP_TOTAL_NUM = 100 * 1000;
   // if 600 seconds after trans timeout, warn is required
  static const int64_t OB_TRANS_WARN_USE_TIME = 600 * 1000 * 1000;
  static const int64_t MAX_TRANS_2PC_TIMEOUT_US = 3 * 1000 * 1000; // 3s
protected:
  //0x0078746365657266 means freectx
  static const int64_t UNKNOWN_FREE_CTX_MAGIC_NUM = 0x0078746365657266;
  //0x006e776f6e6b6e75 means resetctx
  static const int64_t UNKNOWN_RESET_CTX_MAGIC_NUM = 0x006e776f6e6b6e75;
  static const int64_t CHECK_GC_PARTITION_INTERVAL = 10 * 1000 * 1000;
  // the time interval of asking scheduler for participant
  static const int64_t CHECK_SCHEDULER_STATUS_INTERVAL = 10 * 1000 * 1000;
  // the time interval of asking scheduler for rs
  static const int64_t CHECK_RS_SCHEDULER_STATUS_INTERVAL = 60 * 1000 * 1000;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTransCtx);
protected:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObTransID trans_id_;

  common::ObAddr addr_;
  // the expired time of a transaction for checking transaction timeout
  int64_t trans_expired_time_;
  int64_t ctx_create_time_;
  ObTransService *trans_service_;
  mutable CtxLock lock_;
  ObTransTraceLog trace_log_;
  ObTransTraceLog *tlog_;
  uint64_t cluster_version_;
  ObLSTxCtxMgr *ls_tx_ctx_mgr_;
  uint32_t session_id_;
  uint32_t associated_session_id_;// associated session id in distributed scenario
  // set stc only by set_stc_xxx, and
  // get stc only by get_stc_()
  MonotonicTs stc_;
  // the variable is used to record the action of the current transaction in the stmt execution
  int64_t part_trans_action_;
  ObTxCommitCallback commit_cb_;
  // [only used by mysqltest]: will callback scheduler when clear log is persistented
  bool callback_scheduler_on_clear_;
  ObTransNeedWaitWrap trans_need_wait_wrap_;
  int pending_callback_param_;
  // it is used to wake up the lock queue after submitting the log of elr trans
  memtable::ObMemtableCtx *p_mt_ctx_;
  bool is_exiting_;
  bool for_replay_;
  bool need_retry_redo_sync_by_task_;
  bool has_pending_callback_;
  // whether the trans can release locks early
  bool can_elr_;
  // inc opid before ctx unlocked
  int64_t opid_;

  int64_t request_id_;
  ObITransRpc *rpc_;
  int64_t trans_2pc_timeout_;
  ObTransTimeoutTask timeout_task_;
  ObITransTimer *timer_;
  ObTraceInfo trace_info_;
  // feature handler
  ObTxELRHandler elr_handler_;
};

class TransCtxAlloc
{
public:
  static const int64_t OP_LOCAL_NUM = 128;
  ObTransCtx* alloc_value() { return NULL; }
  void free_value(ObTransCtx* ctx)
  {
    if (NULL != ctx) {
      ObTransCtxFactory::release(ctx);
    }
  }
};


} // transaction
} // oceanbase

#endif // OCEANBASE_TRANSACTION_OB_TRANS_CTX_
