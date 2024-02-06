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
namespace sql
{
  class ObLoadDataStat;
}
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
      const uint64_t dest_tenant_id,
      const int64_t task_id,
      const share::ObDDLType &ddl_type,
      const int64_t data_table_id,
      const int64_t dest_table_id,
      const int64_t schema_version,
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
  inline void set_is_copy_indexes(const bool is_copy_indexes) {is_copy_indexes_ = is_copy_indexes;}
  inline void set_is_copy_triggers(const bool is_copy_triggers) {is_copy_triggers_ = is_copy_triggers;}
  inline void set_is_copy_constraints(const bool is_copy_constraints) {is_copy_constraints_ = is_copy_constraints;}
  inline void set_is_copy_foreign_keys(const bool is_copy_foreign_keys) {is_copy_foreign_keys_ = is_copy_foreign_keys;}
  inline void set_is_ignore_errors(const bool is_ignore_errors) {is_ignore_errors_ = is_ignore_errors;}
  inline void set_is_do_finish(const bool is_do_finish) {is_do_finish_ = is_do_finish;}
  virtual int serialize_params_to_message(char *buf, const int64_t buf_len, int64_t &pos) const override;
  virtual int deserlize_params_from_message(const uint64_t tenant_id, const char *buf, const int64_t data_len, int64_t &pos) override;
  virtual int64_t get_serialize_param_size() const override;
  int assign(const ObTableRedefinitionTask *table_redef_task);
  virtual int collect_longops_stat(share::ObLongopsValue &value) override;
  virtual bool support_longops_monitoring() const override { return true; }
  virtual void flt_set_task_span_tag() const override;
  virtual void flt_set_status_span_tag() const override;
  static bool check_task_status_is_pending(const share::ObDDLTaskStatus task_status);
  INHERIT_TO_STRING_KV("ObDDLRedefinitionTask", ObDDLRedefinitionTask,
      K(has_rebuild_index_), K(has_rebuild_constraint_), K(has_rebuild_foreign_key_),
      K(is_copy_indexes_), K(is_copy_triggers_), K(is_copy_constraints_),
      K(is_copy_foreign_keys_), K(is_ignore_errors_), K(is_do_finish_));
protected:
  int table_redefinition(const share::ObDDLTaskStatus next_task_status);
  int copy_table_dependent_objects(const share::ObDDLTaskStatus next_task_status);
  int take_effect(const share::ObDDLTaskStatus next_task_status);
  int set_partition_task_status(const common::ObTabletID &tablet_id,
                                const int ret_code,
                                const int64_t row_scanned,
                                const int64_t row_inserted);
  int repending(const share::ObDDLTaskStatus next_task_status);
private:
  inline bool get_is_copy_indexes() const {return is_copy_indexes_;}
  inline bool get_is_copy_triggers() const {return is_copy_triggers_;}
  inline bool get_is_copy_constraints() const {return is_copy_constraints_;}
  inline bool get_is_copy_foreign_keys() const {return is_copy_foreign_keys_;}
  inline bool get_is_ignore_errors() const {return is_ignore_errors_;}
  inline bool get_is_do_finish() const {return is_do_finish_;}
  int copy_table_indexes();
  int copy_table_constraints();
  int copy_table_foreign_keys();
  int send_build_replica_request();
  int send_build_replica_request_by_sql();
  int check_build_replica_end(bool &is_end);
  int replica_end_check(const int ret_code);
  int check_modify_autoinc(bool &modify_autoinc);
  int check_use_heap_table_ddl_plan(bool &use_heap_table_ddl_plan);
  int get_direct_load_job_stat(common::ObArenaAllocator &allocator, sql::ObLoadDataStat &job_stat);
private:
  static const int64_t OB_TABLE_REDEFINITION_TASK_VERSION = 1L;
  bool has_rebuild_index_;
  bool has_rebuild_constraint_;
  bool has_rebuild_foreign_key_;
  common::ObArenaAllocator allocator_;
  bool is_copy_indexes_;
  bool is_copy_triggers_;
  bool is_copy_constraints_;
  bool is_copy_foreign_keys_;
  bool is_ignore_errors_;
  bool is_do_finish_;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_TABLE_REDEFINITION_TASK_H
