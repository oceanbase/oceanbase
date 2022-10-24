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

#ifndef OCEANBASE_ROOTSERVER_OB_TABLE_REDEFINITION_TASK_H
#define OCEANBASE_ROOTSERVER_OB_TABLE_REDEFINITION_TASK_H

#include "rootserver/ddl_task/ob_ddl_task.h"
#include "rootserver/ddl_task/ob_ddl_redefinition_task.h"

namespace oceanbase
{
namespace rootserver
{
class ObRootService;

class ObTableRedefinitionTask : public ObDDLRedefinitionTask
{
public:
  ObTableRedefinitionTask();
  virtual ~ObTableRedefinitionTask();
  int init(
      const uint64_t tenant_id,
      const int64_t task_id,
      const share::ObDDLType &ddl_type,
      const int64_t data_table_id,
      const int64_t dest_table_id,
      const int64_t schema_version,
      const int64_t parallelism,
      const obrpc::ObAlterTableArg &alter_table_arg,
      const int64_t task_status = share::ObDDLTaskStatus::PREPARE,
      const int64_t snapshot_version = 0);
  int init(const ObDDLTaskRecord &task_record);
  virtual int process() override;
  virtual int update_complete_sstable_job_status(
      const common::ObTabletID &tablet_id,
      const int64_t snapshot_version,
      const int64_t execution_id,
      const int ret_code) override;
protected:
  int table_redefinition(const share::ObDDLTaskStatus next_task_status);
  int copy_table_dependent_objects(const share::ObDDLTaskStatus next_task_status);
  int take_effect(const share::ObDDLTaskStatus next_task_status);
private:
  int copy_table_indexes();
  int copy_table_constraints();
  int copy_table_foreign_keys();
  int send_build_replica_request();
  int check_build_replica_timeout();
  int check_build_replica_end(bool &is_end);
  int check_modify_autoinc(bool &modify_autoinc);
  int check_use_heap_table_ddl_plan(bool &use_heap_table_ddl_plan);
private:
  static const int64_t OB_TABLE_REDEFINITION_TASK_VERSION = 1L;
  bool has_rebuild_index_;
  bool has_rebuild_constraint_;
  bool has_rebuild_foreign_key_;
  common::ObArenaAllocator allocator_;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_TABLE_REDEFINITION_TASK_H
