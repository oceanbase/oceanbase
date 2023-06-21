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

#ifndef OCEANBASE_SHARE_OB_BACKUP_CLEAN_LS_TASK_MGR_H_
#define OCEANBASE_SHARE_OB_BACKUP_CLEAN_LS_TASK_MGR_H_
#include "ob_backup_service.h"
#include "share/backup/ob_backup_clean_struct.h"
#include "share/backup/ob_archive_struct.h"

namespace oceanbase
{
namespace rootserver
{

class ObBackupCleanLSTaskMgr final
{
public:
  ObBackupCleanLSTaskMgr();
  virtual ~ObBackupCleanLSTaskMgr();
  int init(
      share::ObBackupCleanTaskAttr &task_attr,
      share::ObBackupCleanLSTaskAttr &ls_attr,
      ObBackupTaskScheduler &task_scheduler,
      common::ObISQLClient &sql_proxy,
      ObBackupCleanService &backup_service);
  int process(int64_t &finish_cnt);
  int cancel(int64_t &finish_cnt);
  static int advance_ls_task_status(
      ObBackupCleanService &backup_service,
      common::ObISQLClient &sql_proxy, 
      const share::ObBackupCleanLSTaskAttr &ls_attr, 
      const share::ObBackupTaskStatus &next_status, 
      const int result = OB_SUCCESS, 
      const int64_t end_ts = 0);
  static int redo_ls_task(
      ObBackupCleanService &backup_service,
      common::ObISQLClient &sql_proxy, 
      const share::ObBackupCleanLSTaskAttr &ls_attr,
      const int64_t retry_id);
  static int statistic_info(
      ObBackupCleanService &backup_service,
      common::ObISQLClient &sql_proxy, 
      const share::ObBackupCleanLSTaskAttr &ls_attr);
private:
  int add_task_();
  int finish_(int64_t &finish_cnt);
private:
  bool is_inited_;
  share::ObBackupCleanTaskAttr *task_attr_;
  share::ObBackupCleanLSTaskAttr *ls_attr_;
  ObBackupTaskScheduler *task_scheduler_;
  common::ObISQLClient *sql_proxy_;
  ObBackupCleanService *backup_service_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupCleanLSTaskMgr);
};

}
}
#endif  // OCEANBASE_SHARE_OB_BACKUP_CLEAN_LS_TASK_MGR_H_
