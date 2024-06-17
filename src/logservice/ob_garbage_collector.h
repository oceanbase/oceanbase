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

#ifndef OCEANBASE_LOGSERVICE_OB_GARBAGE_COLLECTOR_
#define OCEANBASE_LOGSERVICE_OB_GARBAGE_COLLECTOR_

#include "common/ob_role.h"
#include "lib/hash/ob_array_hash_map.h"
#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "lib/net/ob_addr.h"
#include "share/ob_thread_pool.h"
#include "share/ob_ls_id.h"
#include "share/ls/ob_ls_status_operator.h"
#include "storage/tx_storage/ob_safe_destroy_handler.h"
#include "logservice/ob_log_base_header.h"
#include "logservice/ob_append_callback.h"
#include "logservice/logrpc/ob_log_rpc_proxy.h"

namespace oceanbase
{
namespace obrpc
{
class ObSrvRpcProxy;
}
namespace transaction
{
class ObTransService;
}
namespace common
{
class ObMySQLProxy;
}
namespace storage
{
class ObLSService;
class ObLS;
}
namespace share
{
class SCN;
}
namespace logservice
{
enum ObGCLSLOGType
{
  UNKNOWN_TYPE = 0,
  BLOCK_TABLET_TRANSFER_IN = 1,
  OFFLINE_LS = 2,
  GC_LS_LOG_TYPE_MAX_TYPE = 3,
};

// 记录LS当前GC状态,状态不能回退,增删状态需要注意对应的bool接口的正确性
enum LSGCState
{
  INVALID_LS_GC_STATE = 0,
  NORMAL = 1,
  LS_BLOCKED = 2,
  WAIT_OFFLINE = 3, //deprecated after version 4.0.0
  LS_OFFLINE = 4,
  WAIT_GC = 5,
  MAX_LS_GC_STATE = 6,
};

static inline
int gc_state_to_string(const LSGCState gc_state,
                       char *str,
                       const int64_t str_len)
{
  int ret = OB_SUCCESS;
  if (gc_state == INVALID_LS_GC_STATE) {
    strncpy(str ,"INVALID_STATE", str_len);
  } else if (gc_state == NORMAL) {
    strncpy(str ,"NORMAL", str_len);
  } else if (gc_state == LS_BLOCKED) {
    strncpy(str ,"LS_BLOCKED", str_len);
  } else if (gc_state == WAIT_OFFLINE) {
    // only for version 4.0.0
    strncpy(str ,"WAIT_OFFLINE", str_len);
  } else if (gc_state == LS_OFFLINE) {
    strncpy(str ,"LS_OFFLINE", str_len);
  } else if (gc_state == WAIT_GC) {
    strncpy(str ,"WAIT_GC", str_len);
  } else {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

struct GCDiagnoseInfo
{
  GCDiagnoseInfo() { reset(); }
  ~GCDiagnoseInfo() { reset(); }
  LSGCState gc_state_;
  int64_t gc_start_ts_;
  int64_t block_tx_ts_;
  TO_STRING_KV(K(gc_state_),
               K(gc_start_ts_),
               K(block_tx_ts_));
  void reset() {
    gc_state_ = LSGCState::INVALID_LS_GC_STATE;
    gc_start_ts_ = OB_INVALID_TIMESTAMP;
    block_tx_ts_ = OB_INVALID_TIMESTAMP;
  }
};

class ObGCLSLog
{
public:
  ObGCLSLog();
  ObGCLSLog(const int16_t log_type);
  ~ObGCLSLog();
public:
  void reset();
  int16_t get_log_type();
  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(K(header_), K(version_), K(log_type_));
private:
  static const int16_t GC_LOG_VERSION = 1;
  ObLogBaseHeader header_;
  int16_t version_;
  int16_t log_type_;
};

class ObGarbageCollector : public share::ObThreadPool
{
public:
  ObGarbageCollector();
  ~ObGarbageCollector();
public:
  static int mtl_init(ObGarbageCollector* &gc_service);
  int init(storage::ObLSService *ls_service,
           obrpc::ObSrvRpcProxy *rpc_proxy,
           common::ObMySQLProxy *sql_proxy,
           obrpc::ObLogServiceRpcProxy *log_rpc_proxy,
           const common::ObAddr &self_addr);
  int start();
  void stop();
  void wait();
  void destroy();
  void run1();
public:
  //当前轮次GC查询LS的状态,状态不能回退,增删状态需要注意对应的bool接口的正确性
  enum LSStatus
  {
    LS_INVALID_STATUS = 0,
    LS_NORMAL = 1,
    LS_DROPPING = 2,
    LS_TENANT_DROPPING = 3,
    LS_WAIT_OFFLINE = 4,
    LS_NEED_DELETE_ENTRY = 5,
    LS_NEED_GC = 6,
    LS_MAX_STATUS = 7,
  };

  enum GCReason
  {
    INVALID_GC_REASON = 0,          //不需要GC
    NOT_IN_LEADER_MEMBER_LIST = 1,  //不在成员列表中
    LS_STATUS_ENTRY_NOT_EXIST = 2,  //日志流状态表已删除该日志流
    MAX_GC_REASON = 3,
  };

  struct GCCandidate
  {
    share::ObLSID ls_id_;
    LSStatus ls_status_;
    GCReason gc_reason_;
  public:
    void set_ls_status(const share::ObLSStatus &ls_status);
    TO_STRING_KV(K(ls_id_) ,K(ls_status_), K(gc_reason_));
  };

  typedef common::ObSEArray<GCCandidate, 16> ObGCCandidateArray;
  typedef common::ObLinearHashMap<common::ObAddr, share::ObLSArray> ServerLSMap;
  static const int64_t GC_INTERVAL = 10 * 1000 * 1000; // 10 seconds
public:
  static bool is_ls_dropping_ls_status(const LSStatus &status);
  static bool is_tenant_dropping_ls_status(const LSStatus &status);
  int get_ls_status_from_table(const share::ObLSID &ls_id,
                               share::ObLSStatus &ls_status);
  int add_safe_destroy_task(storage::ObSafeDestroyTask &task);
  template <typename Function>
  int safe_destroy_task_for_each(Function &fn)
  {
    return safe_destroy_handler_.for_each(fn);
  }
private:
  bool is_valid_ls_status_(const LSStatus &status);
  bool is_need_gc_ls_status_(const LSStatus &status);
  bool is_normal_ls_status_(const LSStatus &status);
  bool is_need_delete_entry_ls_status_(const LSStatus &status);
  //member list相关
  int gc_check_member_list_(ObGCCandidateArray &gc_candidates);
  int construct_server_ls_map_for_member_list_(ServerLSMap &server_ls_map) const;
  int handle_each_ls_for_member_list_(ServerLSMap &server_ls_map,
                                      ObGCCandidateArray &gc_candidates);
  int construct_server_ls_map_(ServerLSMap &server_ls_map,
                               const common::ObAddr &server,
                               const share::ObLSID &id) const;
  //日志流状态表相关
  void gc_check_ls_status_(ObGCCandidateArray &gc_candidates);
  int gc_check_ls_status_(storage::ObLS &ls,
                          ObGCCandidateArray &gc_candidates);
  int check_if_tenant_has_been_dropped_(const uint64_t tenant_id,
                                        bool &has_dropped);
  int delete_ls_status_(const share::ObLSID &id);
  void execute_gc_(ObGCCandidateArray &gc_candidates);

private:
  class InsertLSFunctor;
  class QueryLSIsValidMemberFunctor;
private:
  bool is_inited_;
  storage::ObLSService *ls_service_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  common::ObMySQLProxy *sql_proxy_;
  obrpc::ObLogServiceRpcProxy *log_rpc_proxy_;
  common::ObAddr self_addr_;
  int64_t seq_;
  storage::ObSafeDestroyHandler safe_destroy_handler_;
  // stop push task, but will process the left task.
  bool stop_create_new_gc_task_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObGarbageCollector);
};

class ObGCHandler : public ObIReplaySubHandler,
                    public ObIRoleChangeSubHandler,
                    public ObICheckpointSubHandler
{
public:
  ObGCHandler();
  ~ObGCHandler();

public:
  class ObGCLSLogCb : public AppendCb
  {
  public:
    enum CbState : uint8_t
    {
      STATE_INIT = 0,
      STATE_SUCCESS = 1,
      STATE_FAILED = 2,
    };
    ObGCLSLogCb()
      : state_(CbState::STATE_INIT), handler_(NULL)
    {
    }
    explicit ObGCLSLogCb(ObGCHandler *handler)
      : state_(CbState::STATE_INIT), handler_(handler)
    {
    }
    virtual ~ObGCLSLogCb() {}
    virtual int on_success() override;
    virtual int on_failure() override {
      ATOMIC_STORE(&state_, CbState::STATE_FAILED);
      return OB_SUCCESS;
    }
    inline bool is_succeed() const { return CbState::STATE_SUCCESS == ATOMIC_LOAD(&state_); }
    inline bool is_failed() const { return CbState::STATE_FAILED == ATOMIC_LOAD(&state_); }
    TO_STRING_KV(K(state_), K(scn_), KP(handler_));
  public:
    CbState state_;
    share::SCN scn_;
    ObGCHandler *handler_;
  };
public:
  int init(storage::ObLS *ls);
  void reset();
  void execute_pre_gc_process(ObGarbageCollector::LSStatus &ls_status);
  int execute_pre_remove();
  int check_ls_can_offline(const share::ObLSStatus &ls_status);
  int gc_check_invalid_member_seq(const int64_t gc_seq, bool &need_gc);
  static bool is_valid_ls_gc_state(const LSGCState &state);
  static bool is_ls_offline_gc_state(const LSGCState &state);
  void set_log_sync_stopped();
  bool is_log_sync_stopped() const {return ATOMIC_LOAD(&log_sync_stopped_);}

  int handle_on_success_cb(const ObGCHandler::ObGCLSLogCb &cb);
  int diagnose(GCDiagnoseInfo &diagnose_info) const;

  // for replay
  virtual int replay(const void *buffer,
                     const int64_t nbytes,
                     const palf::LSN &lsn,
                     const share::SCN &scn) override;

  // for role change
  virtual void switch_to_follower_forcedly() override;
  virtual int switch_to_leader() override;
  virtual int switch_to_follower_gracefully() override;
  virtual int resume_leader() override;

  // for checkpoint
  virtual share::SCN get_rec_scn() override;
  virtual int flush(share::SCN &scn) override;

  TO_STRING_KV(K(is_inited_),
               K(gc_seq_invalid_member_),
               K(gc_start_ts_),
               K(block_tx_ts_),
               K(block_log_debug_time_),
               K(log_sync_stopped_),
               K(rec_scn_));

private:
  typedef common::SpinRWLock RWLock;
  typedef common::SpinRLockGuard RLockGuard;
  typedef common::SpinWLockGuard WLockGuard;



private:
  const int64_t MAX_WAIT_TIME_US_FOR_READONLY_TX = 10 * 60 * 1000 * 1000L;//10 min
  const int64_t LS_CLOG_ALIVE_TIMEOUT_US = 100 * 1000; //100ms
  const int64_t GET_GTS_TIMEOUT_US = 10L * 1000 * 1000; //10s
  int get_gts_(const int64_t timeout_us, share::SCN &gts_scn);
  bool is_ls_blocked_state_(const LSGCState &state);
  bool is_ls_wait_gc_state_(const LSGCState &state);
  bool is_ls_blocked_finished_(const LSGCState &state);
  bool is_ls_offline_finished_(const LSGCState &state);

  bool is_tablet_clear_(const ObGarbageCollector::LSStatus &ls_status);
  void try_check_and_set_wait_gc_(ObGarbageCollector::LSStatus &ls_status);
  int try_check_and_set_wait_gc_when_log_archive_is_off_(
      const LSGCState &gc_state,
      const share::SCN &readable_scn,
      const share::SCN &offline_scn,
      ObGarbageCollector::LSStatus &ls_status);
  int check_if_tenant_is_dropping_or_dropped_(const uint64_t tenant_id,
      bool &is_tenant_dropping_or_dropped);
  int get_tenant_readable_scn_(share::SCN &readable_scn);
  int check_if_tenant_in_archive_(bool &in_archive);
  int submit_log_(const ObGCLSLOGType log_type, bool &is_success);
  int update_ls_gc_state_after_submit_log_(const ObGCLSLOGType log_type,
                                           const share::SCN &scn);
  int block_ls_transfer_in_(const share::SCN &block_scn);
  int offline_ls_(const share::SCN &offline_scn);
  int get_palf_role_(common::ObRole &role);
  void handle_gc_ls_dropping_(const ObGarbageCollector::LSStatus &ls_status);
  void handle_gc_ls_offline_(ObGarbageCollector::LSStatus &ls_status);
  void set_block_tx_if_necessary_();
  int overwrite_set_gc_state_retcode_(const int ret_code,
                                      const LSGCState gc_state,
                                      const ObLSID &ls_id);
private:
  bool is_inited_;
  mutable RWLock rwlock_; //for leader revoke/takeover submit log
  storage::ObLS *ls_;
  int64_t gc_seq_invalid_member_; //缓存gc检查当前ls不在成员列表时的轮次
  int64_t gc_start_ts_;
  int64_t block_tx_ts_;
  int64_t block_log_debug_time_;
  bool log_sync_stopped_;//used for trans_service to kill trx, True means this replica may not be able to fully synchronize the logs.
  share::SCN rec_scn_;
  common::ObSpinLock rec_scn_lock_;//protect  rec_scn_, which guarantees that cb.scn_ is assigned valid value before on_success callback
  int64_t last_print_dba_log_ts_;
};

} // namespace logservice
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_OB_GARBAGE_COLLECTOR_
