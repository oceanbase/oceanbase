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

#ifndef OCEANBASE_ROOTSERVER_OB_RECOVER_RESTORE_TABLE_TASK_H
#define OCEANBASE_ROOTSERVER_OB_RECOVER_RESTORE_TABLE_TASK_H

#include "rootserver/ddl_task/ob_ddl_task.h"
#include "rootserver/ddl_task/ob_table_redefinition_task.h"

namespace oceanbase
{
namespace sql
{
  class ObLoadDataStat;
}
namespace rootserver
{
class ObRootService;

class ObRecoverRestoreTableTask final : public ObTableRedefinitionTask
{
public:
  ObRecoverRestoreTableTask();
  virtual ~ObRecoverRestoreTableTask();
  int init(
      const uint64_t src_tenant_id,
      const uint64_t dst_tenant_id,
      const int64_t task_id,
      const share::ObDDLType &ddl_type,
      const int64_t data_table_id,
      const int64_t dest_table_id,
      const int64_t src_schema_version,
      const int64_t dest_schema_version,
      const int64_t parallelism,
      const int64_t consumer_group_id,
      const obrpc::ObAlterTableArg &alter_table_arg,
      const int64_t task_status = share::ObDDLTaskStatus::PREPARE,
      const int64_t snapshot_version = 0);
  int init(const ObDDLTaskRecord &task_record);
  virtual int process() override;
  virtual int update_complete_sstable_job_status(
    const common::ObTabletID &tablet_id,
    const int64_t snapshot_version,
    const int64_t execution_id,
    const int ret_code,
    const ObDDLTaskInfo &addition_info) override;
protected:
  virtual int obtain_snapshot(const share::ObDDLTaskStatus next_task_status) override;
  virtual int fail() override;
  virtual int success() override;
private:
  static const int64_t OB_RECOVER_RESTORE_TABLE_TASK_VERSION = 1L;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_RECOVER_RESTORE_TABLE_TASK_H
