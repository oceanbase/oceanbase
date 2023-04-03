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
           ObBackupLeaseService &lease_service,
           ObBackupService &backup_service);
  // result : when task can't retry, return the result which is the error code
  int process(int64_t &finish_cnt);
  int cancel(int64_t &finish_cnt);
  static int advance_status(ObBackupLeaseService &lease_service,
                            common::ObISQLClient &sql_proxy,
                            const share::ObBackupLSTaskAttr &ls_attr,
                            const share::ObBackupTaskStatus &next_status,
                            const int result = OB_SUCCESS,
                            const int64_t end_ts = 0);
  static int redo_ls_task(ObBackupLeaseService &lease_service,
                          common::ObISQLClient &sql_proxy,
                          const share::ObBackupLSTaskAttr &ls_attr,
                          const int64_t start_turn_id,
                          const int64_t turn_id,
                          const int64_t retry_id);
  static int mark_ls_task_info_final(ObBackupLeaseService &lease_service,
                                     common::ObISQLClient &sql_proxy,
                                     const share::ObBackupLSTaskAttr &ls_attr);
  static int update_black_server(ObBackupLeaseService &lease_service,
                                 common::ObISQLClient &sql_proxy,
                                 const share::ObBackupLSTaskAttr &ls_attr,
                                 const ObAddr &block_server);
private:
  int gen_and_add_task_();
  int check_ls_is_dropped_(bool &is_dropped);
  int gen_and_add_backup_data_task_();
  int gen_and_add_backup_meta_task_();
  int gen_and_add_backup_compl_log_();
  int gen_and_add_build_index_task_();
  int finish_(int64_t &finish_cnt);
private:
  bool is_inited_;
  share::ObBackupJobAttr *job_attr_;
  share::ObBackupLSTaskAttr *ls_attr_;
  share::ObBackupSetTaskAttr *set_task_attr_;
  ObBackupTaskScheduler *task_scheduler_;
  common::ObISQLClient *sql_proxy_;
  ObBackupLeaseService *lease_service_;
  ObBackupService *backup_service_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupDataLSTaskMgr);
};

}
}

#endif  // OCEANBASE_SHARE_OB_BACKUP_DATA_LS_TASK_MGR_H_
