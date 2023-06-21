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

#ifndef OCEANBASE_SHARE_OB_BACKUP_DATA_LS_TASK_MGR_H_
#define OCEANBASE_SHARE_OB_BACKUP_DATA_LS_TASK_MGR_H_

#include "ob_backup_data_scheduler.h"

namespace oceanbase
{
namespace rootserver
{

class ObBackupDataLSTaskMgr
{
public:
  ObBackupDataLSTaskMgr();
  virtual ~ObBackupDataLSTaskMgr();
  int init(share::ObBackupJobAttr &job_attr,
           share::ObBackupSetTaskAttr &set_task,
           share::ObBackupLSTaskAttr &ls_attr,
           ObBackupTaskScheduler &task_scheduler,
           common::ObISQLClient &sql_proxy,
           ObBackupDataService &backup_service);
  // result : when task can't retry, return the result which is the error code
  int process(int64_t &finish_cnt);
  int cancel(int64_t &finish_cnt);

  static int redo_ls_task(ObBackupDataService &backup_service,
                          common::ObISQLClient &sql_proxy,
                          const share::ObBackupLSTaskAttr &ls_attr,
                          const int64_t start_turn_id,
                          const int64_t turn_id,
                          const int64_t retry_id);
  static int handle_execute_over(ObBackupDataService &backup_service,
                          common::ObISQLClient &sql_proxy,
                          const share::ObBackupLSTaskAttr &ls_attr,
                          const share::ObHAResultInfo &result_info);
  static int check_ls_is_dropped(const share::ObBackupLSTaskAttr &ls_attr,
                                 common::ObISQLClient &sql_proxy,
                                 bool &is_dropped);
private:
  int gen_and_add_task_();
  int gen_and_add_backup_data_task_();
  int gen_and_add_backup_meta_task_();
  int gen_and_add_backup_compl_log_();
  int gen_and_add_build_index_task_();
  int finish_(int64_t &finish_cnt);
  int advance_status_(const share::ObBackupTaskStatus &next_status);
private:
  bool is_inited_;
  share::ObBackupJobAttr *job_attr_;
  share::ObBackupLSTaskAttr *ls_attr_;
  share::ObBackupSetTaskAttr *set_task_attr_;
  ObBackupTaskScheduler *task_scheduler_;
  common::ObISQLClient *sql_proxy_;
  ObBackupDataService *backup_service_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupDataLSTaskMgr);
};

}
}

#endif  // OCEANBASE_SHARE_OB_BACKUP_DATA_LS_TASK_MGR_H_
