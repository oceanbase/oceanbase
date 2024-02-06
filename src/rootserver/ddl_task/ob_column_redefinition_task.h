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

#ifndef OCEANBASE_ROOTSERVER_OB_COLUMN_REDEFINITION_TASK_H
#define OCEANBASE_ROOTSERVER_OB_COLUMN_REDEFINITION_TASK_H

#include "rootserver/ddl_task/ob_ddl_task.h"
#include "rootserver/ddl_task/ob_ddl_redefinition_task.h"

namespace oceanbase
{
namespace rootserver
{
class ObRootService;

// the process of DDL operation, including drop column, ...
class ObColumnRedefinitionTask final : public ObDDLRedefinitionTask
{
public:
  ObColumnRedefinitionTask();
  virtual ~ObColumnRedefinitionTask();
  int init(
      const uint64_t tenant_id,
      const int64_t task_id,
      const share::ObDDLType &ddl_type,
      const int64_t data_table_id,
      const int64_t dest_table_id,
      const int64_t schema_version,
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
  virtual int collect_longops_stat(share::ObLongopsValue &value) override;
  virtual bool support_longops_monitoring() const { return true; }
  virtual void flt_set_task_span_tag() const override;
  virtual void flt_set_status_span_tag() const override;
  INHERIT_TO_STRING_KV("ObDDLRedefinitionTask", ObDDLRedefinitionTask,
      K(has_rebuild_index_), K(has_rebuild_constraint_), K(has_rebuild_foreign_key_), K(is_sstable_complete_task_submitted_));
private:
  int copy_table_dependent_objects(const share::ObDDLTaskStatus next_task_status);
  int take_effect(const share::ObDDLTaskStatus next_task_status);
  int copy_table_indexes();
  int copy_table_constraints();
  int copy_table_foreign_keys();
  virtual int serialize_params_to_message(char *buf, const int64_t buf_len, int64_t &pos) const override;
  virtual int deserlize_params_from_message(const uint64_t tenant_id, const char *buf, const int64_t data_len, int64_t &pos) override;
  virtual int64_t get_serialize_param_size() const override;
private:
  static const int64_t OB_COLUMN_REDEFINITION_TASK_VERSION = 1L; 
  bool has_rebuild_index_;
  bool has_rebuild_constraint_;
  bool has_rebuild_foreign_key_;
  common::ObArenaAllocator allocator_;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_COLUMN_REDEFINITION_TASK_H
