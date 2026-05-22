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

#ifndef OCEANBASE_ROOTSERVER_OB_BACKUP_VALIDATE_TASK_MGR_H_
#define OCEANBASE_ROOTSERVER_OB_BACKUP_VALIDATE_TASK_MGR_H_
#include "share/backup/ob_backup_validate_struct.h"
#include "share/backup/ob_archive_struct.h"
#include "storage/backup/ob_backup_data_store.h"
#include "ob_backup_service.h"

namespace oceanbase
{
namespace rootserver
{

class ObBackupValidateTaskMgr final
{
public:
  friend class ObBackupValidateLSTaskMgr;
public:
  ObBackupValidateTaskMgr();
  ~ObBackupValidateTaskMgr() {};
public:
  int init(
      const uint64_t tenant_id,
      const int64_t task_id,
      share::ObBackupValidateJobAttr &job_attr,
      common::ObISQLClient &sql_proxy,
      obrpc::ObSrvRpcProxy &rpc_proxy,
      ObBackupTaskScheduler &task_scheduler,
      ObBackupService &backup_service);
  int deal_failed_task(const int error);
  int process();
  int check_task_running(bool &is_running);
  int do_clean_up();
private:
  int do_cancel_();
  int advance_task_status_(
      common::ObMySQLTransaction &trans,
      const share::ObBackupValidateStatus &next_status,
      const int result,
      const int64_t end_ts);
  int prepare_backup_validate_task_();
  int do_backup_validate_ls_tasks_(ObArray<share::ObBackupValidateLSTaskAttr> &ls_tasks, int64_t &finish_cnt);
  int backup_validate_ls_tasks_();
  int generate_ls_task_(const share::ObLSID &ls_id, share::ObBackupValidateLSTaskAttr &ls_attr);
  int generate_and_insert_ls_tasks_(common::ObMySQLTransaction &trans, const ObArray<share::ObLSID> &ls_ids);
  int read_meta_files_();
  int do_failed_ls_task_(
      common::ObISQLClient &sql_proxy,
      ObIArray<share::ObBackupValidateLSTaskAttr> &ls_task,
      int64_t &success_ls_count,
      int &result);
  bool is_can_retry_(const int error);
  int get_ls_ids_and_total_bytes_(
      const share::ObBackupDest &backup_set_dest,
      ObIArray<ObLSID> &ls_ids,
      int64_t &total_bytes);
  int get_backup_set_ls_ids_and_total_bytes_(
      const share::ObBackupDest &backup_set_dest,
      ObIArray<ObLSID> &ls_ids,
      int64_t &total_bytes);
  int get_archive_piece_ls_ids_and_total_bytes_(
      const share::ObBackupDest &backup_piece_dest,
      ObIArray<ObLSID> &ls_ids,
      int64_t &total_bytes);

  int get_archive_piece_ls_ids_(
      const share::ObBackupDest &backup_piece_dest,
      const share::ObBackupPath &piece_path,
      ObIArray<ObLSID> &ls_ids);

private:
  bool is_inited_;
  uint64_t tenant_id_;
  share::ObBackupValidateTaskAttr task_attr_;
  share::ObBackupValidateJobAttr *job_attr_;
  common::ObISQLClient *sql_proxy_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  ObBackupTaskScheduler *task_scheduler_;
  ObBackupService *validate_service_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupValidateTaskMgr);
};

}//rootserver
}//oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_BACKUP_VALIDATE_TASK_MGR_H_