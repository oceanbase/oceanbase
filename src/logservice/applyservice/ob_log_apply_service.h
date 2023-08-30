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

#ifndef OCEANBASE_LOGSERVICE_LOG_APPLY_SERVICE_
#define OCEANBASE_LOGSERVICE_LOG_APPLY_SERVICE_
#include "common/ob_role.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/thread/ob_thread_lease.h"
#include "logservice/palf/palf_callback.h"
#include "logservice/palf/palf_handle.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
namespace share
{
class SCN;
}
namespace palf
{
class PalfEnv;
}
namespace logservice
{
class ObLSAdapter;
class AppendCb;
class AppendCbTask;
class ObApplyStatus;
class ObLogApplyService;
class ObApplyStatusGuard;

enum class ObApplyServiceTaskType
{
  INVALID_LOG_TASK = 0,
  SUBMIT_LOG_TASK = 1,
  APPLY_LOG_TASK = 2,
};

//虚拟表统计
struct LSApplyStat
{
  int64_t ls_id_;
  common::ObRole role_;
  int64_t proposal_id_;
  palf::LSN end_lsn_;
  int64_t pending_cnt_;

  TO_STRING_KV(K(ls_id_),
               K(role_),
               K(proposal_id_),
               K(end_lsn_),
               K(pending_cnt_));
};

struct ApplyDiagnoseInfo
{
  ApplyDiagnoseInfo() { reset(); }
  ~ApplyDiagnoseInfo() { reset(); }
  share::SCN max_applied_scn_;
  TO_STRING_KV(K(max_applied_scn_));
  void reset() {
    max_applied_scn_.reset();
  }
};

class ObApplyFsCb : public palf::PalfFSCb
{
public:
  ObApplyFsCb();
  ObApplyFsCb(ObApplyStatus *apply_status);
  ~ObApplyFsCb();
  void destroy();
  int update_end_lsn(int64_t id,
                     const palf::LSN &end_lsn,
                     const int64_t proposal_id);
private:
  ObApplyStatus *apply_status_;
};

class ObApplyServiceTask
{
public:
  ObApplyServiceTask();
  virtual ~ObApplyServiceTask();
  virtual void reset();
  ObApplyStatus *get_apply_status();
  bool acquire_lease();
  bool revoke_lease();
  ObApplyServiceTaskType get_type() const;
  VIRTUAL_TO_STRING_KV(K(type_));
protected:
  ObApplyServiceTaskType type_;
  ObApplyStatus *apply_status_;
  common::ObThreadLease lease_;
};

class ObApplyServiceSubmitTask : public ObApplyServiceTask
{
public:
  ObApplyServiceSubmitTask();
  ~ObApplyServiceSubmitTask();
  int init(ObApplyStatus *apply_status);
  void reset() override;
};

class ObApplyServiceQueueTask : public ObApplyServiceTask
{
public:
  typedef common::ObLink Link;
public:
  ObApplyServiceQueueTask();
  ~ObApplyServiceQueueTask();
  int init(ObApplyStatus *apply_status,
           const int64_t idx);
  void reset() override;
public:
  Link *top();
  int pop();
  int push(Link *p);
  void inc_total_submit_cb_cnt();
  void inc_total_apply_cb_cnt();
  int64_t get_total_submit_cb_cnt() const;
  int64_t get_total_apply_cb_cnt() const;
  void set_snapshot_check_submit_cb_cnt();
  int is_snapshot_apply_done(bool &is_done);
  int is_apply_done(bool &is_done);
  int64_t idx() const;
  INHERIT_TO_STRING_KV("ObApplyServiceQueueTask", ObApplyServiceTask,
                       K(total_submit_cb_cnt_),
                       K(last_check_submit_cb_cnt_),
                       K(total_apply_cb_cnt_),
                       K(idx_));
private:
  int64_t total_submit_cb_cnt_;
  int64_t last_check_submit_cb_cnt_;
  int64_t total_apply_cb_cnt_;
  common::ObSpLinkQueue queue_;
  int64_t idx_;
};

class ObApplyStatus
{
public:
  ObApplyStatus();
  ~ObApplyStatus();
public:
  int init(const share::ObLSID &id,
           palf::PalfEnv *palf_env,
           ObLogApplyService *ap_sv);
  void destroy();
  int stop();
  void inc_ref();
  int64_t dec_ref();
  //任务相关
  int push_append_cb(AppendCb *cb);
  int try_submit_cb_queues();
  int try_handle_cb_queue(ObApplyServiceQueueTask *cb_queue, bool &is_timeslice_run_out);
  int is_apply_done(bool &is_done,
                    palf::LSN &end_lsn);
  //主备切换相关
  //int can_switch_to_follower(bool &can_revoke); //非最大保护模式不需要
  int switch_to_leader(const int64_t new_proposal_id);
  int switch_to_follower();
  //palf相关
  int update_palf_committed_end_lsn(const palf::LSN &end_lsn, const int64_t proposal_id);
  int unregister_file_size_cb();
  void close_palf_handle();
  //最大连续回调位点
  int get_max_applied_scn(share::SCN &scn);
  int stat(LSApplyStat &stat) const;
  int handle_drop_cb();
  int diagnose(ApplyDiagnoseInfo &diagnose_info);
  // offline相关
  //
  // The constraint between palf and apply:
  //
  // Palf guarantee that switch apply to follower only when there is not
  // any uncommitted logs in previous LEADER, therefore, apply only update
  // 'palf_committed_end_lsn_' when 'proposal_id_' is as same as current
  // proposal_id of palf.
  //
  // To increase robustness, apply assums that update 'palf_committed_end_lsn_'
  // when the role of apply is LEADER execpet above constraints. otherwise,
  // apply consider it as unexpected error.
  //
  // However, in rebuild scenario, apply will be reset to FOLLOWER even if there
  // are logs to be committed when 'proposal_id_' is as same as current proposal_id
  // of palf.
  //
  // To solve above problem, add an interface which used to reset 'proposal_id_' of
  // apply.
  //
  // NB: this interface only can be used in 'ObLogHandler::offline'.
  void reset_proposal_id();
  // NB: this interface only can be used in 'ObLogHandler::online'.
  void reset_meta();
  TO_STRING_KV(K(ls_id_),
               K(role_),
               K(proposal_id_),
               K(palf_committed_end_lsn_),
               K(last_check_scn_),
               K(max_applied_cb_scn_));
private:
  int submit_task_to_apply_service_(ObApplyServiceTask &task);
  int check_and_update_max_applied_scn_(const share::SCN scn);
  int update_last_check_scn_();
  int handle_drop_cb_queue_(ObApplyServiceQueueTask &cb_queue);
  int switch_to_follower_();
  //从cb中获取打点信息
  void get_cb_trace_(AppendCb *cb,
                     int64_t &append_start_time,
                     int64_t &append_finish_time,
                     int64_t &cb_first_handle_time,
                     int64_t &cb_start_time);
  void statistics_cb_cost_(const palf::LSN &lsn,
                           const share::SCN &scn,
                           const int64_t append_start_time,
                           const int64_t append_finish_time,
                           const int64_t cb_first_handle_time,
                           const int64_t cb_start_time,
                           const int64_t idx);
private:
  typedef common::RWLock RWLock;
  typedef RWLock::RLockGuard RLockGuard;
  typedef RWLock::WLockGuard WLockGuard;
  typedef RWLock::WLockGuardWithRetryInterval WLockGuardWithRetryInterval;
  const int64_t MAX_HANDLE_TIME_US_PER_ROUND_US = 100 * 1000; //100ms
  const int64_t WRLOCK_RETRY_INTERVAL_US = 20 * 1000;  // 20ms
private:
  bool is_inited_;
  bool is_in_stop_state_; //stop后不能上任, 残留的cb会继续处理
  int64_t ref_cnt_; // guarantee the effectiveness of self memory
  share::ObLSID ls_id_;
  common::ObRole role_;
  int64_t proposal_id_;
  ObLogApplyService *ap_sv_;
  palf::LSN palf_committed_end_lsn_;
  //LSN standy_committed_end_lsn_;
  //palf::LSN min_committed_end_lsn_;
  share::SCN last_check_scn_; //当前待确认的最大连续回调位点
  share::SCN max_applied_cb_scn_; //该位点前的cb保证都已经回调完成
  ObApplyServiceSubmitTask submit_task_;
  ObApplyServiceQueueTask cb_queues_[APPLY_TASK_QUEUE_SIZE];
  palf::PalfEnv *palf_env_;
  palf::PalfHandle palf_handle_;
  ObApplyFsCb fs_cb_;
  mutable RWLock lock_; //保护role_, proposal_id_及is_in_stop_state_
  mutable lib::ObMutex mutex_; //互斥获取最大连续位点不会被并发调用
  mutable int64_t get_info_debug_time_;
  mutable int64_t try_wrlock_debug_time_;
  ObMiniStat::ObStatItem cb_append_stat_; //获取lsn和ts的耗时
  ObMiniStat::ObStatItem cb_wait_thread_stat_; //等待首次线程调度的耗时, 此次处理不一定会回调
  ObMiniStat::ObStatItem cb_wait_commit_stat_; //从第一次被处理到真正回调之间的耗时
  ObMiniStat::ObStatItem cb_execute_stat_; //cb执行on_success/on_failure的耗时
  ObMiniStat::ObStatItem cb_stat_; //cb从产生到执行on_success的耗时
};

class ObLogApplyService : public lib::TGTaskHandler
{
public:
  ObLogApplyService();
  virtual ~ObLogApplyService();
public:
  int init(palf::PalfEnv *palf_env,
           ObLSAdapter *ls_adapter);
  void destroy();
  int start();
  void stop();
  void wait();
  int add_ls(const share::ObLSID &id);
  int remove_ls(const share::ObLSID &id);
  int get_apply_status(const share::ObLSID &id, ObApplyStatusGuard &guard);
  void revert_apply_status(ObApplyStatus *apply_status);
  void handle(void *task);
  void handle_drop(void *task);
  int is_apply_done(const share::ObLSID &id,
                    bool &is_done,
                    palf::LSN &end_lsn);
  int switch_to_leader(const share::ObLSID &id, const int64_t proposal_id);
  int switch_to_follower(const share::ObLSID &id);
  int get_max_applied_scn(const share::ObLSID &id, share::SCN &scn);
  int push_task(ObApplyServiceTask *task);
  int wait_append_sync(const share::ObLSID &ls_id);
  int stat_for_each(const common::ObFunction<int (const ObApplyStatus &)> &func);
  int diagnose(const share::ObLSID &id, ApplyDiagnoseInfo &diagnose_info);
public:
  class GetApplyStatusFunctor
  {
  public:
    GetApplyStatusFunctor(ObApplyStatusGuard &guard)
        : ret_code_(common::OB_SUCCESS), guard_(guard){}
    ~GetApplyStatusFunctor(){}
    bool operator()(const share::ObLSID &id, ObApplyStatus *apply_status);
    int get_ret_code() const { return ret_code_; }
    TO_STRING_KV(K(ret_code_));
  private:
    int ret_code_;
    ObApplyStatusGuard &guard_;
  };
  class RemoveApplyStatusFunctor
  {
  public:
    explicit RemoveApplyStatusFunctor()
        : ret_code_(common::OB_SUCCESS) {}
    ~RemoveApplyStatusFunctor(){}
    bool operator()(const share::ObLSID &id, ObApplyStatus *replay_status);
    int get_ret_code() const { return ret_code_; }
    TO_STRING_KV(K(ret_code_));
  private:
    int ret_code_;
  };
  //删除并清理所有cb
  class ResetApplyStatusFunctor
  {
  public:
    explicit ResetApplyStatusFunctor()
        : ret_code_(common::OB_SUCCESS) {}
    ~ResetApplyStatusFunctor(){}
    bool operator()(const share::ObLSID &id, ObApplyStatus *replay_status);
    int get_ret_code() const { return ret_code_; }
    TO_STRING_KV(K(ret_code_));
  private:
    int ret_code_;
  };
private:
  int handle_cb_queue_(ObApplyStatus *apply_status,
                       ObApplyServiceQueueTask *cb_queue,
                       bool &is_timeslice_run_out);
  int handle_submit_task_(ObApplyStatus *apply_status);
  int remove_all_ls_();
private:
  bool is_inited_;
  bool is_running_;
  int tg_id_;
  palf::PalfEnv *palf_env_;
  ObLSAdapter *ls_adapter_;
  common::ObLinearHashMap<share::ObLSID, ObApplyStatus*> apply_status_map_;
  DISALLOW_COPY_AND_ASSIGN(ObLogApplyService);
};

// for apply_status_map in apply service
class ObApplyStatusGuard
{
public:
  ObApplyStatusGuard();
  ~ObApplyStatusGuard();
  ObApplyStatus *get_apply_status();
private:
  void set_apply_status_(ObApplyStatus *apply_status);
private:
  friend class ObLogApplyService::GetApplyStatusFunctor;
  ObApplyStatus *apply_status_;
  DISALLOW_COPY_AND_ASSIGN(ObApplyStatusGuard);
};

} // namespace logservice
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_LOG_APPLY_SERVICE_
