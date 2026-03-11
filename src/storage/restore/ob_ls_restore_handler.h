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
#include "ob_ls_restore_stat.h"
#include "ob_sn_ls_restore_state.h"

namespace oceanbase
{
namespace storage
{
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
  bool is_stop() { return ATOMIC_LOAD(&is_stop_); }
  int update_rebuild_seq();
  int64_t get_rebuild_seq();
  int fill_restore_arg();
  void set_is_online(const bool is_online) { ATOMIC_STORE(&is_online_, is_online); }
  bool is_online() {return ATOMIC_LOAD(&is_online_); }
  const ObTenantRestoreCtx &get_restore_ctx() const { return ls_restore_arg_; }
  const ObLSRestoreStat &get_restore_stat() const { return restore_stat_; }
  ObLSRestoreStat &restore_stat() { return restore_stat_; }
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
  bool need_update_state_handle_(share::ObLSRestoreStatus &new_status);
  int refresh_restore_info_();
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
  ObLSRestoreStat restore_stat_;
  share::ObTaskId trace_id_;
  bool succeed_set_dest_info_;
  int64_t restore_job_id_;
  DISALLOW_COPY_AND_ASSIGN(ObLSRestoreHandler);
};
}
}
#endif
