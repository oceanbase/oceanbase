// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//         http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef OCEANBASE_ROOTSERVER_OB_ALL_VIRTUAL_BACKUP_TASK_SCHEDULER_STAT_H_
#define OCEANBASE_ROOTSERVER_OB_ALL_VIRTUAL_BACKUP_TASK_SCHEDULER_STAT_H_

#endif // OCEANBASE_ROOTSERVER_OB_ALL_BACKUP_TASK_SCHEDULER_STAT_H_

#include "share/ob_virtual_table_projector.h"
#include "rootserver/backup/ob_backup_task_scheduler.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/ob_define.h"
namespace oceanbase
{
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
class ObTableSchema;
}
}
namespace rootserver
{
class ObBackupScheduleTask;
class ObAllBackupScheduleTaskStat : public common::ObVirtualTableProjector
{
public:
  explicit ObAllBackupScheduleTaskStat(omt::ObMultiTenant *omt) : omt_(omt), allocator_() {}
  virtual ~ObAllBackupScheduleTaskStat() {}
  virtual int inner_get_next_row(common::ObNewRow *&row);
private:
  enum Column : int64_t
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP = common::OB_APP_MIN_COLUMN_ID + 1,
    SVR_PORT = common::OB_APP_MIN_COLUMN_ID + 2,
    JOB_ID = common::OB_APP_MIN_COLUMN_ID + 3,
    TASK_ID = common::OB_APP_MIN_COLUMN_ID + 4,
    LS_ID = common::OB_APP_MIN_COLUMN_ID + 5,
    JOB_TYPE = common::OB_APP_MIN_COLUMN_ID + 6,
    TRACE_ID = common::OB_APP_MIN_COLUMN_ID + 7,
    DST = common::OB_APP_MIN_COLUMN_ID + 8,
    IS_SCHEDULE = common::OB_APP_MIN_COLUMN_ID + 9,
    GENERATE_TS = common::OB_APP_MIN_COLUMN_ID + 10,
    SCHEDULE_TS = common::OB_APP_MIN_COLUMN_ID + 11,
    EXECUTOR_TS = common::OB_APP_MIN_COLUMN_ID + 12
  };
  struct Display
  {
    void reset();
    uint64_t tenant_id_;
    int64_t job_id_;
    int64_t task_id_;
    int64_t key_1_;
    BackupJobType job_type_;
    share::ObTaskId trace_id_;
    common::ObAddr dst_; // choosed server
    bool is_schedule;
    int64_t generate_ts_; // time of generating task
    int64_t schedule_ts_; // time of choosing task dst
    int64_t executor_ts_; // time of sending to dst
    TO_STRING_KV(K_(tenant_id), K_(job_id), K_(task_id), K_(key_1), K_(job_type), K_(trace_id),
        K_(dst), K_(generate_ts), K_(schedule_ts), K_(executor_ts));
  };
  int generate_all_row_(ObIArray<ObBackupScheduleTask *> &task_stats);
  int generate_task_stat_(const ObBackupScheduleTask &task_stat, Display &display);
  int get_full_row_(const ObBackupScheduleTask &task_stat);
private:
  omt::ObMultiTenant *omt_;
  common::ObArenaAllocator allocator_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllBackupScheduleTaskStat);
};
}//end namespace rootserver
}//end namespace oceanbase