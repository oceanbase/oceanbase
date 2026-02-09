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

#ifndef OCEANBASE_SHARE_OB_BACKUP_VALIDATE_LS_TASK_MGR_H_
#define OCEANBASE_SHARE_OB_BACKUP_VALIDATE_LS_TASK_MGR_H_
#include "ob_backup_service.h"
#include "share/backup/ob_backup_validate_struct.h"
#include "share/backup/ob_archive_struct.h"

namespace oceanbase
{
namespace rootserver
{

class ObBackupValidateLSTaskMgr final
{
public:
  ObBackupValidateLSTaskMgr();
  ~ObBackupValidateLSTaskMgr();
  int init(
      share::ObBackupValidateJobAttr &job_attr,
      share::ObBackupValidateTaskAttr &task_attr,
      share::ObBackupValidateLSTaskAttr &ls_attr,
      ObBackupTaskScheduler &task_scheduler,
      common::ObISQLClient &sql_proxy,
      ObBackupService &validate_service);
  void reset();
  int process(int64_t &finish_cnt);
  int cancel(int64_t &finish_cnt);
private:
  int add_task_();
  int finish_(int64_t &finish_cnt);
private:
  bool is_inited_;
  share::ObBackupValidateJobAttr *job_attr_;
  share::ObBackupValidateTaskAttr *task_attr_;
  share::ObBackupValidateLSTaskAttr *ls_attr_;
  ObBackupTaskScheduler *task_scheduler_;
  common::ObISQLClient *sql_proxy_;
  ObBackupService *validate_service_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupValidateLSTaskMgr);
};

}
}
#endif  // OCEANBASE_SHARE_OB_BACKUP_VALIDATE_LS_TASK_MGR_H_
