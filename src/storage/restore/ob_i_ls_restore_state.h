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

#ifndef OCEABASE_STORAGE_LS_RESTORE_STATE_H
#define OCEABASE_STORAGE_LS_RESTORE_STATE_H

#include "ob_ls_restore_stat.h"
#include "storage/ob_storage_rpc.h"

namespace oceanbase
{
namespace storage
{
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
  const ObTenantRestoreCtx *get_restore_arg() const { return ls_restore_arg_; }

  // Check if log has been recovered to restore_scn.
  virtual int check_recover_finish(bool &is_finish) const
  {
    is_finish = false;
    return OB_SUCCESS;
  }

  int report_start_replay_clog_lsn_();
  int report_finish_replay_clog_lsn_();
  int add_finished_tablet_cnt(const int64_t cnt);
  int report_unfinished_tablet_cnt(const int64_t cnt);

  int add_finished_bytes(const int64_t bytes);
  int report_unfinished_bytes(const int64_t bytes);
  int advance_restore_status(const ObLSRestoreStatus &next_status);

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
  bool need_notify_rs_restore_finish_(const ObLSRestoreStatus &ls_restore_status);

  void notify_rs_restore_finish_();
  int check_restore_pre_finish_(bool &is_finish) const;
  int check_ls_leader_ready_(bool &is_ready);
  int check_clog_replay_finish_(bool &is_finish);
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

}
}
#endif