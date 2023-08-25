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

#ifndef OCEABASE_STORAGE_LS_RESTORE_HANDLER_H
#define OCEABASE_STORAGE_LS_RESTORE_HANDLER_H

#include "ob_ls_restore_args.h"
#include "ob_ls_restore_task_mgr.h"
#include "share/restore/ob_ls_restore_status.h"
#include "storage/high_availability/ob_storage_restore_struct.h" 
#include "storage/high_availability/ob_storage_ha_struct.h"
#include "common/ob_role.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashset.h"
#include "lib/lock/ob_mutex.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "storage/ob_storage_rpc.h"

namespace oceanbase
{
namespace storage
{

class ObLSRestoreResultMgr final
{
public:
  enum RestoreFailedType {
    DATA_RESTORE_FAILED_TYPE = 0,
    CLOG_RESTORE_FAILED_TYPE = 1,
    MAX_FAILED_TYPE 
  };
  const static int64_t OB_MAX_LS_RESTORE_RETRY_TIME_INTERVAL = 10 * 1000 * 1000; // 10s
  const static int64_t OB_MAX_RESTORE_RETRY_TIMES = 64;
public:
  ObLSRestoreResultMgr();
  ~ObLSRestoreResultMgr() {}
  int get_result() const { return result_; }
  const share::ObTaskId &get_trace_id() const { return trace_id_; }
  bool can_retry() const;
  bool is_met_retry_time_interval();
  void set_result(const int result, const share::ObTaskId &trace_id, const RestoreFailedType &failed_type);
  int get_comment_str(const ObLSID &ls_id, const ObAddr &addr, ObHAResultInfo::Comment &comment) const;
  bool can_retrieable_err(const int err) const;

  TO_STRING_KV(K_(result), K_(retry_cnt), K_(trace_id), K_(failed_type));
private:
  lib::ObMutex mtx_;
  int result_;
  int64_t retry_cnt_;
  int64_t last_err_ts_;
  share::ObTaskId trace_id_;
  RestoreFailedType failed_type_;

  DISALLOW_COPY_AND_ASSIGN(ObLSRestoreResultMgr);
};

class ObILSRestoreState;
class ObLSRestoreHandler : public ObIHAHandler
{
public:
  ObLSRestoreHandler();
  virtual ~ObLSRestoreHandler();

  int init(storage::ObLS *ls);
  void destroy();
  virtual int process();
  // used by clog restore to record the failed info.
  int record_clog_failed_info(const share::ObTaskId &trace_id, const share::ObLSID &ls_id, const int &result);

  void try_record_one_tablet_to_restore(const common::ObTabletID &tablet_id);

  int get_consistent_scn(share::SCN &consistent_scn);

  int handle_execute_over(const share::ObTaskId &task_id, const ObIArray<common::ObTabletID> &restore_succeed_tablets, 
      const ObIArray<common::ObTabletID> &restore_failed_tablets, const share::ObLSID &ls_id, const int &result);
  // when follower received rpc, call this
  int handle_pull_tablet(const ObIArray<common::ObTabletID> &tablet_ids, 
      const share::ObLSRestoreStatus &leader_restore_status, const int64_t leader_proposal_id);
  void wakeup();
  void stop() { ATOMIC_STORE(&is_stop_, true); } // when remove ls, set this
  int safe_to_destroy(bool &is_safe);
  int offline();
  int online();
  bool is_stop() { return is_stop_; }
  int update_rebuild_seq();
  int64_t get_rebuild_seq();
  int fill_restore_arg();
private:
  int cancel_task_();
  int check_before_do_restore_(bool &can_do_restore);
  int check_in_member_or_learner_list_(bool &is_in_member_or_learner_list) const;
  int update_state_handle_();
  int check_meta_tenant_normal_(bool &is_normal);
  int check_restore_job_exist_(bool &is_exist);
  int get_restore_state_handler_(const share::ObLSRestoreStatus &new_status, ObILSRestoreState *&new_state_handler);
  template <typename T>
  int construct_state_handler_(T *&new_handler);
  int deal_failed_restore_();
private:
  bool is_inited_;
  bool is_stop_; // used by ls destory
  bool is_online_; // used by ls online/offline
  int64_t rebuild_seq_; // update by rebuild
  lib::ObMutex mtx_;
  ObLSRestoreResultMgr result_mgr_;
  storage::ObLS *ls_;
  ObTenantRestoreCtx ls_restore_arg_;
  ObILSRestoreState *state_handler_;
  common::ObFIFOAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObLSRestoreHandler);
};

class ObILSRestoreState
{
public:
  ObILSRestoreState(const share::ObLSRestoreStatus::Status &status);
  virtual ~ObILSRestoreState();
  int init(storage::ObLS &ls, logservice::ObLogService &log_srv, ObTenantRestoreCtx &restore_args);
  void destroy();
  virtual int do_restore() = 0;
  virtual void set_retry_flag() {} // used by restore sys tablets
  int deal_failed_restore(const ObLSRestoreResultMgr &result_mgr);
  int handle_pull_tablet(const ObIArray<common::ObTabletID> &tablet_ids,
      const share::ObLSRestoreStatus &leader_restore_status, const int64_t leader_proposal_id);
  share::ObLSRestoreStatus get_restore_status() const { return ls_restore_status_; }
  common::ObRole get_role() const { return role_; }
  ObLSRestoreTaskMgr &get_tablet_mgr() { return tablet_mgr_; }
  int check_leader_restore_finish(bool &finish);
  storage::ObLS *get_ls() const { return ls_; }

  // Check if log has been recovered to restore_scn.
  virtual int check_recover_finish(bool &is_finish) const
  {
    is_finish = false;
    return OB_SUCCESS;
  }

  int report_start_replay_clog_lsn_();
  int report_finish_replay_clog_lsn_();

  TO_STRING_KV(K_(*ls), K_(ls_restore_status));
protected:
  int leader_fill_tablet_group_restore_arg_(const ObIArray<ObTabletID> &tablet_need_restore,
      const ObTabletRestoreAction::ACTION &action, ObTabletGroupRestoreArg &tablet_group_restore_arg);
  int follower_fill_tablet_group_restore_arg_(const ObIArray<ObTabletID> &tablet_need_restore,
      const ObTabletRestoreAction::ACTION &action, ObTabletGroupRestoreArg &tablet_group_restore_arg);

  int notify_follower_restore_tablet_(const ObIArray<common::ObTabletID> &tablet_ids);
  int get_follower_server_(ObIArray<ObStorageHASrcInfo> &follower);
  int check_all_follower_restore_finish_(bool &finish);
  int check_follower_restore_finish(const share::ObLSRestoreStatus &leader_status, 
      const share::ObLSRestoreStatus &follower_status, bool &is_finish);
  bool check_leader_restore_finish_(
      const share::ObLSRestoreStatus &leader_status,
      const share::ObLSRestoreStatus &follower_status) const;

  int update_role_();
  bool is_switch_to_leader_(const ObRole &new_role);
  bool is_switch_to_follower_(const ObRole &new_role);
  int advance_status_(storage::ObLS &ls, const share::ObLSRestoreStatus &next_status);
  int report_ls_restore_status_(const storage::ObLS &ls, const share::ObLSRestoreStatus &next_status);

  int request_leader_status_(share::ObLSRestoreStatus &leader_restore_status);
  int get_leader_(ObStorageHASrcInfo &leader);
  int schedule_tablet_group_restore_(
      const ObTabletGroupRestoreArg &arg,
      const share::ObTaskId &task_id);
  int schedule_ls_restore_(
      const ObLSRestoreArg &arg,
      const share::ObTaskId &task_id);
  int check_restore_concurrency_limit_(bool &reach_limit);
  int inner_check_can_do_restore_(bool &can_restore);
  int schedule_tablet_group_restore_dag_net_(
      const ObTabletGroupRestoreArg &arg,
      const share::ObTaskId &task_id);
  int schedule_ls_restore_dag_net_(
      const ObLSRestoreArg &arg,
      const share::ObTaskId &task_id);

  int insert_initial_ls_restore_progress_();
  int report_ls_restore_progress_(storage::ObLS &ls, const share::ObLSRestoreStatus &status, 
      const share::ObTaskId &trace_id, const int result = OB_SUCCESS, const char *comment = "");

  int online_();
  void offline_();
  int update_restore_status_(
      storage::ObLS &ls,
      const share::ObLSRestoreStatus &next_status);
  int check_new_election_(bool &is_changed) const;

  int check_replay_to_target_scn_(
      const share::SCN &target_scn,
      bool &replayed) const;

protected:
  bool is_inited_;
  int64_t cluster_id_;
  storage::ObLS *ls_;
  share::ObLSRestoreStatus ls_restore_status_;
  ObTenantRestoreCtx *ls_restore_arg_;
  common::ObRole role_;
  int64_t proposal_id_;
  ObLSRestoreTaskMgr tablet_mgr_;
  share::ObLocationService *location_service_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
  common::ObMySQLProxy *proxy_;
  common::ObAddr self_addr_;
  bool need_report_clog_lsn_;
  
  DISALLOW_COPY_AND_ASSIGN(ObILSRestoreState);
};

class ObLSRestoreStartState final : public ObILSRestoreState 
{
public:
  ObLSRestoreStartState();
  virtual ~ObLSRestoreStartState();
  virtual int do_restore() override;
private:
  int check_ls_meta_exist_(bool &is_exist);
  int do_with_no_ls_meta_();
  int check_ls_created_(bool &is_created);
  int check_sys_ls_restore_finished_(bool &restore_finish);
  int do_with_uncreated_ls_();
  int check_ls_leader_ready_(bool &is_ready);
  int inc_need_restore_ls_cnt_();
  DISALLOW_COPY_AND_ASSIGN(ObLSRestoreStartState);
};

class ObLSRestoreSysTabletState final : public ObILSRestoreState
{
public:
  ObLSRestoreSysTabletState();
  virtual ~ObLSRestoreSysTabletState();
  virtual int do_restore() override;
  virtual void set_retry_flag() { retry_flag_ = true; }
private:
  int leader_restore_sys_tablet_();
  int follower_restore_sys_tablet_();
  int do_restore_sys_tablet();
  bool is_need_retry_();
  int leader_fill_ls_restore_arg_(ObLSRestoreArg &arg);
  int follower_fill_ls_restore_arg_(ObLSRestoreArg &arg);
private:
  bool retry_flag_;
  DISALLOW_COPY_AND_ASSIGN(ObLSRestoreSysTabletState);
};

class ObLSRestoreCreateUserTabletState final : public ObILSRestoreState
{
public:
  ObLSRestoreCreateUserTabletState();
  virtual ~ObLSRestoreCreateUserTabletState();
  virtual int do_restore() override;
private:
  int leader_create_user_tablet_();
  int follower_create_user_tablet_();
  int do_create_user_tablet_(
      const ObLSRestoreTaskMgr::ToRestoreTabletGroup &tablet_need_restore);
  DISALLOW_COPY_AND_ASSIGN(ObLSRestoreCreateUserTabletState);
};


class ObLSRestoreConsistentScnState final : public ObILSRestoreState
{
public:
  ObLSRestoreConsistentScnState(): ObILSRestoreState(ObLSRestoreStatus::Status::RESTORE_TO_CONSISTENT_SCN) {}
  virtual ~ObLSRestoreConsistentScnState() {}
  virtual int do_restore() override;

  // Check if log has recovered to consistent_scn.
  int check_recover_to_consistent_scn_finish(bool &is_finish) const;

private:
  // Set restore status to EMPTY for those committed tablets whose restore status is FULL,
  // but transfer table is not replaced.
  int set_empty_for_transfer_tablets_();

private:
  DISALLOW_COPY_AND_ASSIGN(ObLSRestoreConsistentScnState);
};


class ObLSQuickRestoreState final : public ObILSRestoreState
{
public:
  ObLSQuickRestoreState();
  virtual ~ObLSQuickRestoreState();
  virtual int do_restore() override;

  // Check if log has been recovered to restore_scn.
  virtual int check_recover_finish(bool &is_finish) const override;

private:
  int leader_quick_restore_();
  int follower_quick_restore_();
  int do_quick_restore_(
      const ObLSRestoreTaskMgr::ToRestoreTabletGroup &tablet_need_restore);
  int check_clog_replay_finish_(bool &is_finish);
  int check_tablet_checkpoint_();
  // Force reload all tablets and check is restored.
  bool has_rechecked_after_clog_recovered_;
  DISALLOW_COPY_AND_ASSIGN(ObLSQuickRestoreState);
};

class ObLSQuickRestoreFinishState final : public ObILSRestoreState
{
public:
  ObLSQuickRestoreFinishState();
  virtual ~ObLSQuickRestoreFinishState();
  virtual int do_restore() override;

  // Check if log has been recovered to restore_scn.
  virtual int check_recover_finish(bool &is_finish) const override
  {
    is_finish = true;
    return OB_SUCCESS;
  }
private:
  int leader_quick_restore_finish_();
  int follower_quick_restore_finish_();
  DISALLOW_COPY_AND_ASSIGN(ObLSQuickRestoreFinishState);
};

class ObLSRestoreMajorState final : public ObILSRestoreState
{
public:
  ObLSRestoreMajorState();
  virtual ~ObLSRestoreMajorState();
  virtual int do_restore() override;

  // Check if log has been recovered to restore_scn.
  virtual int check_recover_finish(bool &is_finish) const override
  {
    is_finish = true;
    return OB_SUCCESS;
  }
private:
  int leader_restore_major_data_();
  int follower_restore_major_data_();
  int do_restore_major_(
      const ObLSRestoreTaskMgr::ToRestoreTabletGroup &tablet_need_restore);
  DISALLOW_COPY_AND_ASSIGN(ObLSRestoreMajorState);
};

class ObLSRestoreFinishState final : public ObILSRestoreState
{
  public:
    ObLSRestoreFinishState();
    virtual ~ObLSRestoreFinishState();
    virtual int do_restore() override;

  // Check if log has been recovered to restore_scn.
  virtual int check_recover_finish(bool &is_finish) const override
  {
    is_finish = true;
    return OB_SUCCESS;
  }
  private:
    int restore_finish_();
    DISALLOW_COPY_AND_ASSIGN(ObLSRestoreFinishState);
};

class ObLSRestoreWaitState : public ObILSRestoreState
{
public:
  ObLSRestoreWaitState(const share::ObLSRestoreStatus::Status &status);
  virtual ~ObLSRestoreWaitState();
  virtual int do_restore() override;

protected:
  virtual int check_can_advance_status_(bool &can) const;

private:
  int check_all_tablets_has_finished_(bool &all_finished);
  int leader_wait_follower_();
  int follower_wait_leader_();

private:
  // Indicate whether has checked all tablets has been restored.
  bool has_confirmed_;
  DISALLOW_COPY_AND_ASSIGN(ObLSRestoreWaitState);
};

class ObLSRestoreWaitRestoreSysTabletState final : public ObLSRestoreWaitState
{
public:
  ObLSRestoreWaitRestoreSysTabletState()
    : ObLSRestoreWaitState(share::ObLSRestoreStatus::Status::WAIT_RESTORE_SYS_TABLETS) {}
  virtual ~ObLSRestoreWaitRestoreSysTabletState() {}
private:
  DISALLOW_COPY_AND_ASSIGN(ObLSRestoreWaitRestoreSysTabletState);
};

class ObLSRestoreWaitCreateUserTabletState final : public ObLSRestoreWaitState
{
public:
  ObLSRestoreWaitCreateUserTabletState()
    : ObLSRestoreWaitState(share::ObLSRestoreStatus::Status::WAIT_RESTORE_TABLETS_META) {}
  virtual ~ObLSRestoreWaitCreateUserTabletState() {}
private:
  DISALLOW_COPY_AND_ASSIGN(ObLSRestoreWaitCreateUserTabletState);
};


class ObLSWaitRestoreConsistentScnState final : public ObLSRestoreWaitState
{
public:
  ObLSWaitRestoreConsistentScnState()
    : ObLSRestoreWaitState(ObLSRestoreStatus::Status::WAIT_RESTORE_TO_CONSISTENT_SCN) {}
  virtual ~ObLSWaitRestoreConsistentScnState() {}

  // Check if log has been recovered to restore_scn.
  virtual int check_recover_finish(bool &is_finish) const override
  {
    is_finish = false;
    return OB_SUCCESS;
  }

protected:
  int check_can_advance_status_(bool &can) const override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLSWaitRestoreConsistentScnState);
};


class ObLSRestoreWaitQuickRestoreState final : public ObLSRestoreWaitState
{
public:
  ObLSRestoreWaitQuickRestoreState()
    : ObLSRestoreWaitState(share::ObLSRestoreStatus::Status::WAIT_QUICK_RESTORE) {}
  virtual ~ObLSRestoreWaitQuickRestoreState() {}

  // Check if log has been recovered to restore_scn.
  virtual int check_recover_finish(bool &is_finish) const override
  {
    is_finish = true;
    return OB_SUCCESS;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObLSRestoreWaitQuickRestoreState);
};

class ObLSRestoreWaitRestoreMajorDataState final : public ObLSRestoreWaitState
{
public:
  ObLSRestoreWaitRestoreMajorDataState()
    : ObLSRestoreWaitState(share::ObLSRestoreStatus::Status::WAIT_RESTORE_MAJOR_DATA) {}
  virtual ~ObLSRestoreWaitRestoreMajorDataState() {}

  // Check if log has been recovered to restore_scn.
  virtual int check_recover_finish(bool &is_finish) const override
  {
    is_finish = true;
    return OB_SUCCESS;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObLSRestoreWaitRestoreMajorDataState);
};

}
}

#endif
