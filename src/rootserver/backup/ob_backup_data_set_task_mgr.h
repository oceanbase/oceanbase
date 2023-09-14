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
namespace share
{
class ObAllTenantInfo;
struct ObBackupDataLSAttrDesc;
}
namespace storage
{
class ObTabletMeta;
}
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
           ObMultiVersionSchemaService &schema_service,
           ObBackupDataService &backup_service);
  int process();
  int do_clean_up();
  int deal_failed_set_task(ObMySQLTransaction &trans);
  share::ObBackupStatus::Status get_status() const { return set_task_attr_.status_.status_; }
  TO_STRING_KV(K_(meta_tenant_id), K_(set_task_attr));
private:
  int persist_sys_ls_task_();
  int do_persist_sys_ls_task_();
  int persist_ls_attr_info_(const share::ObBackupLSTaskAttr &sys_ls_task, ObIArray<share::ObLSID> &ls_ids);
  int sync_wait_backup_user_ls_scn_(const share::ObBackupLSTaskAttr &sys_ls_task, share::SCN &scn);
  int generate_ls_tasks_(const ObIArray<share::ObLSID> &ls_ids, const share::ObBackupDataTaskType &type);
  int calc_task_turn_(const ObBackupDataTaskType &type, int64_t &turn_id);
  int backup_sys_meta_();
  int do_backup_meta_(ObIArray<share::ObBackupLSTaskAttr> &ls_task, int64_t &finish_cnt);
  int backup_user_meta_();
  int calc_consistent_scn_(ObIArray<share::ObBackupLSTaskAttr> &ls_tasks, share::SCN &consistent_scn);
  int check_need_change_meta_turn_(ObIArray<share::ObBackupLSTaskAttr> &ls_tasks, bool &need_change_turn);
  int change_meta_turn_(const share::ObBackupLSTaskAttr &sys_ls_task);
  int get_backup_user_meta_task_(ObIArray<share::ObBackupLSTaskAttr> &ls_task);
  int merge_tablet_to_ls_info_(const share::SCN &consistent_scn, const ObIArray<share::ObBackupLSTaskAttr> &ls_tasks);
  int get_tablet_list_by_snapshot(
      const share::SCN &consistent_scn, common::hash::ObHashMap<share::ObLSID, ObArray<ObTabletID>> &latest_ls_tablet_map);
  int fill_map_with_sys_tablets_(common::hash::ObHashMap<share::ObLSID, ObArray<ObTabletID>> &latest_ls_tablet_map);
  int update_tablet_id_backup_scn_(const share::SCN &backup_scn);
  int get_extern_tablet_info_(const share::ObLSID &ls_id,
      ObIArray<ObTabletID> &user_tablet_ids, share::SCN &backup_scn);
  int merge_ls_meta_infos_(const ObIArray<share::ObBackupLSTaskAttr> &ls_tasks);
  int do_backup_root_key_();
  int backup_data_();
  int do_backup_data_(ObArray<share::ObBackupLSTaskAttr> &ls_task, int64_t &finish_cnt, 
      share::ObBackupLSTaskAttr *& build_index_attr);
  int backup_data_finish_(const ObIArray<share::ObBackupLSTaskAttr> &ls_tasks,
                          const ObBackupLSTaskAttr &build_index_attr);
  int build_index_(share::ObBackupLSTaskAttr *build_index_attr, bool &finish_build_index);
  int check_need_change_turn_(const ObIArray<share::ObBackupLSTaskAttr> &ls_tasks, bool &need_change_turn,
      ObIArray<storage::ObBackupDataTabletToLSInfo> &tablets_to_ls);
  int change_turn_(const ObIArray<share::ObBackupLSTaskAttr> &ls_tasks,
                   const ObBackupLSTaskAttr &build_index_attr,
                   ObIArray<storage::ObBackupDataTabletToLSInfo> &tablets_to_ls);
  int change_task_turn_(const ObIArray<share::ObBackupLSTaskAttr> &ls_task,
      const ObIArray<storage::ObBackupDataTabletToLSInfo> &tablets_to_ls);
  int filter_new_ls_from_tablet_info_(const ObIArray<ObBackupLSTaskAttr> &ls_task,
                                      const ObIArray<storage::ObBackupDataTabletToLSInfo> &tablets_to_ls,
                                      ObIArray<ObLSID> &new_ls_array);
  int get_next_turn_id_(int64_t &next_turn_id);
  int get_change_turn_tablets_(const ObIArray<share::ObBackupLSTaskAttr> &ls_tasks, 
                               ObIArray<storage::ObBackupDataTabletToLSInfo> &tablet_to_ls);
  int get_tablets_of_deleted_ls_(
      const ObIArray<ObBackupLSTaskAttr> &ls_tasks, common::hash::ObHashSet<ObBackupSkipTabletAttr> &skip_tablets);
  int do_get_change_turn_tablets_(const ObIArray<share::ObBackupLSTaskAttr> &ls_tasks, 
      const common::hash::ObHashSet<ObBackupSkipTabletAttr> &skip_tablets,
      ObIArray<storage::ObBackupDataTabletToLSInfo> &tablet_to_ls);
  int construct_cur_ls_set_(const ObIArray<share::ObBackupLSTaskAttr> &ls_tasks, 
      common::hash::ObHashSet<share::ObLSID> &ls_id_set);
  int get_change_turn_ls_(const ObIArray<share::ObBackupLSTaskAttr> &ls_task,
      const ObIArray<storage::ObBackupDataTabletToLSInfo> &tablets_to_ls,
      ObIArray<const share::ObBackupLSTaskAttr *> &need_change_turn_ls_tasks);
  int update_inner_task_(const ObIArray<share::ObLSID> &new_ls_ids, 
      const ObIArray<const share::ObBackupLSTaskAttr *> &need_change_turn_ls_tasks);
  int convert_task_type_(const ObIArray<share::ObBackupLSTaskAttr> &ls_task);
  int backup_completing_log_();
  int do_backup_completing_log_(ObArray<share::ObBackupLSTaskAttr> &ls_task, int64_t &finish_cnt);
  int calculate_start_replay_scn_(share::SCN &start_replay_scn);
  int do_cancel_();
  int do_failed_ls_task_(ObMySQLTransaction &trans, const ObIArray<share::ObBackupLSTaskAttr> &ls_task);
  int write_backup_set_placeholder_(const bool is_start);
  int write_extern_infos_();
  int write_tenant_backup_set_infos_();
  int write_extern_locality_info_(storage::ObExternTenantLocalityInfoDesc &locality_info);
  int write_backup_set_info_(const share::ObBackupSetTaskAttr &set_task_attr, 
      storage::ObExternBackupSetInfoDesc &backup_set_info);
  int write_extern_diagnose_info_(const storage::ObExternTenantLocalityInfoDesc &locality_info,
      const storage::ObExternBackupSetInfoDesc &backup_set_info);
  int write_log_format_file_();

  int write_extern_ls_info_(const ObArray<share::ObBackupLSTaskAttr> &ls_tasks);
  int write_or_update_tablet_to_ls_(ObIArray<storage::ObBackupDataTabletToLSInfo> &tablets_to_ls);
  
  int set_backup_set_files_failed_(ObMySQLTransaction &trans);
  int advance_status_(ObMySQLTransaction &trans, const share::ObBackupStatus &next_status, const int result = OB_SUCCESS,
      const share::SCN &scn = share::SCN::min_scn(), const int64_t end_ts = 0);
  int get_next_status_(const share::ObBackupStatus &cur_status, share::ObBackupStatus &next_status);
private:
  bool is_inited_;
  uint64_t meta_tenant_id_;
  share::ObBackupSetTaskAttr set_task_attr_;
  share::ObBackupJobAttr *job_attr_;
  common::ObMySQLProxy *sql_proxy_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  ObBackupTaskScheduler *task_scheduler_;
  ObMultiVersionSchemaService *schema_service_;
  ObBackupDataService *backup_service_;
  storage::ObBackupDataStore store_;
  ObMySQLTransaction trans_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupSetTaskMgr);
};

}
}

#endif  // OCEANBASE_SHARE_OB_BACKUP_DATA_SET_TASK_MGR_H_
