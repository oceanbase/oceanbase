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

#ifndef OCEANBASE_SHARE_OB_BACKUP_DATA_SET_TASK_MGR_H_
#define OCEANBASE_SHARE_OB_BACKUP_DATA_SET_TASK_MGR_H_

#include "ob_backup_data_scheduler.h"

namespace oceanbase
{
namespace rootserver
{

class ObBackupSetTaskMgr
{
public:
  ObBackupSetTaskMgr();
  ~ObBackupSetTaskMgr() {};
public:
  int init(const uint64_t tenant_id,
           share::ObBackupJobAttr &job_attr,
           common::ObMySQLProxy &sql_proxy,
           obrpc::ObSrvRpcProxy &rpc_proxy,
           ObBackupTaskScheduler &task_scheduler,
           ObBackupLeaseService  &lease_service,
           ObMultiVersionSchemaService &schema_service,
           ObBackupService &backup_service);
  int process();
  int do_clean_up(ObMySQLTransaction &trans);
  int deal_failed_set_task(ObMySQLTransaction &trans);
  bool is_force_cancel() const;
  bool can_write_extern_infos(const int err) const;
  int write_extern_infos();
  int write_backup_set_placeholder(const bool is_start);
  share::ObBackupStatus::Status get_status() const { return next_status_.status_; }
  TO_STRING_KV(K_(meta_tenant_id), K_(set_task_attr));
private:
  int persist_sys_ls_task_();
  int do_persist_sys_ls_task_();
  int persist_ls_attr_info_(ObIArray<share::ObLSID> &ls_ids);
  int generate_ls_tasks_(const ObIArray<share::ObLSID> &ls_ids, const share::ObBackupDataTaskType &type);

  int backup_sys_meta_();
  int backup_user_meta_();
  int change_meta_turn_();
  // TODO: need the enbale/disable transfer
  int disable_transfer_();
  int enable_transfer_();
  int do_backup_meta_(ObArray<share::ObBackupLSTaskAttr> &ls_task, int64_t &finish_cnt);
  int do_backup_root_key_();
  int merge_tablet_to_ls_info_(const ObIArray<share::ObBackupLSTaskAttr> &ls_tasks);
  int construct_ls_task_map_(const ObIArray<share::ObBackupLSTaskAttr> &ls_tasks, 
      hash::ObHashMap<share::ObLSID, const share::ObBackupLSTaskAttr *> &ls_map);
  int get_extern_tablet_info_(const share::ObLSID &ls_id, const int64_t &retry_cnt, 
      share::ObBackupDataTabletToLSInfo &tablet_to_ls_info, share::SCN &backup_scn);
  int check_tablets_match_(const share::ObLSID &ls_id, const ObIArray<ObTabletID> &cur_tablet_ids, 
      const ObIArray<ObTabletID> &user_tablet_ids, const share::SCN &backup_scn);
  int do_check_inc_tablets_(const share::ObLSID &ls_id, const ObIArray<ObTabletID> &inc_tablets, 
      const share::SCN &backup_scn);
  int get_dst_server_(const share::ObLSID &ls_id, ObAddr &dst);
  int merge_ls_meta_infos_(const ObIArray<share::ObBackupLSTaskAttr> &ls_tasks);

  int backup_data_();
  int do_backup_data_(ObArray<share::ObBackupLSTaskAttr> &ls_task, int64_t &finish_cnt, 
      share::ObBackupLSTaskAttr *& build_index_attr);
  int build_index_(share::ObBackupLSTaskAttr *build_index_attr, const int64_t turn_id, const int64_t task_id, 
      bool &finish_build_index);
  int check_change_task_turn_(const ObIArray<share::ObBackupLSTaskAttr> &ls_tasks, bool &need_change_turn, 
      ObIArray<share::ObBackupDataTabletToLSInfo> &tablets_to_ls, ObIArray<share::ObLSID> &new_ls_array);
  int change_task_turn_(ObIArray<share::ObBackupLSTaskAttr> &ls_task, 
      ObIArray<share::ObBackupDataTabletToLSInfo> &tablets_to_ls, ObIArray<share::ObLSID> &new_ls_array);
  int get_change_turn_tablets_(const ObIArray<share::ObBackupLSTaskAttr> &ls_tasks, 
      ObIArray<share::ObBackupDataTabletToLSInfo> &tablet_to_ls, ObIArray<share::ObLSID> &new_ls_ids);
  int do_get_change_turn_tablets_(const ObIArray<share::ObBackupLSTaskAttr> &ls_tasks, 
      const ObIArray<share::ObBackupSkipTabletAttr> &all_tablets, 
      ObIArray<share::ObBackupDataTabletToLSInfo> &tablet_to_ls, ObIArray<share::ObLSID> &new_ls_ids);
  int construct_cur_ls_set_(const ObIArray<share::ObBackupLSTaskAttr> &ls_tasks, 
      common::hash::ObHashSet<share::ObLSID> &ls_id_set);
  int get_change_turn_ls_(ObIArray<share::ObBackupLSTaskAttr> &ls_task, 
      const ObIArray<share::ObBackupDataTabletToLSInfo> &tablets_to_ls,
      ObIArray<share::ObBackupLSTaskAttr *> &need_change_turn_ls_tasks);
  int persist_deleted_tablets_info_(const common::ObIArray<share::ObBackupSkipTabletAttr> &skip_tablets);
  int update_inner_task_(const ObIArray<share::ObLSID> &new_ls_ids, 
      const ObIArray<share::ObBackupLSTaskAttr *> &need_change_turn_ls_tasks);
  int update_task_type_(const ObIArray<share::ObBackupLSTaskAttr> &ls_task);

  int backup_completing_log_();
  int do_backup_completing_log_(ObArray<share::ObBackupLSTaskAttr> &ls_task, int64_t &finish_cnt);
  int calculate_start_replay_scn_(share::SCN &start_replay_scn);

  int do_cancel_();
  
  int do_failed_ls_task_(ObMySQLTransaction &trans, const ObIArray<share::ObBackupLSTaskAttr> &ls_task);
  int write_tenant_backup_set_infos_();
  int write_extern_locality_info_(share::ObExternTenantLocalityInfoDesc &locality_info);
  int write_backup_set_info_(const share::ObBackupSetTaskAttr &set_task_attr, 
      share::ObExternBackupSetInfoDesc &backup_set_info);
  int write_extern_diagnose_info_(const share::ObExternTenantLocalityInfoDesc &locality_info,
      const share::ObExternBackupSetInfoDesc &backup_set_info);

  int write_extern_ls_info_(const ObArray<share::ObBackupLSTaskAttr> &ls_tasks);
  int write_tablet_to_ls_infos_(const ObIArray<share::ObBackupDataTabletToLSInfo> &tablets_to_ls, const int64_t turn_id);
  int write_deleted_tablet_infos_();
  
  int set_backup_set_files_failed_(ObMySQLTransaction &trans);
  int advance_status_(ObMySQLTransaction &trans, const share::ObBackupStatus &next_status, const int result = OB_SUCCESS,
      const share::SCN &scn = share::SCN::min_scn(), const int64_t end_ts = 0);
  int get_next_status_(const share::ObBackupStatus &cur_status, share::ObBackupStatus &next_status);
private:
  bool is_inited_;
  uint64_t meta_tenant_id_;
  share::ObBackupStatus next_status_;
  share::ObBackupSetTaskAttr set_task_attr_;
  share::ObBackupJobAttr *job_attr_;
  common::ObMySQLProxy *sql_proxy_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  ObBackupLeaseService  *lease_service_;
  ObBackupTaskScheduler *task_scheduler_;
  ObMultiVersionSchemaService *schema_service_;
  ObBackupService *backup_service_;
  share::ObBackupDataStore store_;
  ObMySQLTransaction trans_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupSetTaskMgr);
};

}
}

#endif  // OCEANBASE_SHARE_OB_BACKUP_DATA_SET_TASK_MGR_H_
