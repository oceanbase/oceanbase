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

#ifndef OCEANBASE_SHARE_OB_BACKUP_CLEAN_SET_TASK_MGR_H_
#define OCEANBASE_SHARE_OB_BACKUP_CLEAN_SET_TASK_MGR_H_
#include "share/backup/ob_backup_clean_struct.h"
#include "share/backup/ob_archive_struct.h"
#include "ob_backup_service.h"
namespace oceanbase
{
namespace rootserver
{

class ObBackupCleanTaskMgr
{
public:
  friend class ObBackupCleanLSTaskMgr;
public:
  ObBackupCleanTaskMgr();
  ~ObBackupCleanTaskMgr() {};
public:
  int init(
      const uint64_t tenant_id,
      const int64_t task_id,
      share::ObBackupCleanJobAttr &job_attr,
      common::ObISQLClient &sql_proxy,
      obrpc::ObSrvRpcProxy &rpc_proxy,
      ObBackupTaskScheduler &task_scheduler,
      ObBackupCleanService &backup_service);
  int persist_ls_task();
  int do_ls_task();
  int do_cleanup();
  int deal_failed_task(const int error);
  int process();
private:
  int do_cancel_();
  int advance_task_status_(
      common::ObISQLClient &sql_proxy,
      const share::ObBackupCleanStatus &next_status, 
      const int result,
      const int64_t end_ts);
  int prepare_backup_clean_task_();
  int do_backup_clean_ls_tasks_(
      ObArray<share::ObBackupCleanLSTaskAttr> &ls_task, 
      int64_t &finish_cnt);
  int backup_clean_ls_tasks_();
  int generate_ls_task_(const share::ObLSID &ls_id, share::ObBackupCleanLSTaskAttr &ls_attr);
  int persist_ls_tasks_();
  int do_failed_ls_task_(
      common::ObISQLClient &sql_proxy,
      ObIArray<share::ObBackupCleanLSTaskAttr> &ls_task,
      int64_t &success_ls_count,
      int &result);
  int mark_backup_set_files_deleting_();
  int mark_backup_piece_files_deleting_();
  int mark_backup_set_files_deleted_();
  int mark_backup_piece_files_deleted_();
  int mark_backup_files_deleted_();
  int mark_backup_files_deleting_();
  int try_delete_extern_set_end_file_();
  int try_delete_extern_piece_end_file_();
  int try_delete_extern_end_file_();
  int delete_tenant_locality_info_();
  int delete_tenant_diagnose_info_();
  int delete_tenant_backup_set_infos_();
  int delete_single_backup_set_info_();
  int delete_backup_set_start_file_();
  int delete_backup_piece_start_file_();
  int delete_backup_set_inner_placeholder_();
  int delete_minor_data_info_dir_();
  int delete_major_data_info_dir_();
  int get_set_ls_ids_(common::ObIArray<share::ObLSID> &ls_ids);
  int get_piece_ls_ids_(common::ObIArray<share::ObLSID> &ls_ids);
  int get_ls_ids_(common::ObIArray<share::ObLSID> &ls_ids);
  int parse_int_(const char *str, int64_t &val);
  int parse_ls_id_(const char *dir_name, int64_t &id_val);
  int get_ls_ids_from_traverse_(const share::ObBackupPath &path, common::ObIArray<share::ObLSID> &ls_ids);
  int get_set_ls_ids_from_traverse_(common::ObIArray<share::ObLSID> &ls_ids);
  int get_piece_ls_ids_from_traverse_(common::ObIArray<share::ObLSID> &ls_ids);
  int move_task_to_history_();
  int delete_backup_meta_info_files_();
  int delete_data_info_turn_files_(const share::ObBackupPath &infos_path);
  int delete_backup_set_meta_info_files_();
  int delete_backup_piece_meta_info_files_();
  int delete_backup_dir_(const share::ObBackupPath &path);
  int delete_backup_set_dir_();
  int delete_meta_info_dir_();
  int delete_single_piece_info_();
  int delete_tenant_archive_piece_infos();
  int delete_piece_info_file_();
  int delete_piece_inner_placeholder_file_();
  int delete_piece_checkpoint_file_();
  bool is_can_retry_(const int error);

private:
  bool is_inited_;
  uint64_t tenant_id_;
  share::ObBackupSetFileDesc backup_set_info_;
  share::ObTenantArchivePieceAttr backup_piece_info_;
  share::ObBackupCleanTaskAttr task_attr_;
  share::ObBackupDest backup_dest_;
  share::ObBackupCleanJobAttr *job_attr_;
  common::ObISQLClient *sql_proxy_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  ObBackupTaskScheduler *task_scheduler_;
  ObBackupCleanService *backup_service_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupCleanTaskMgr); 
};

}
}

#endif  // OCEANBASE_SHARE_OB_BACKUP_CLEAN_SET_TASK_MGR_H_
