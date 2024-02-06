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

#ifndef OCEANBASE_ROOTSERVER_OB_CHECK_CONSTRAINT_TASK_H
#define OCEANBASE_ROOTSERVER_OB_CHECK_CONSTRAINT_TASK_H

#include "rootserver/ddl_task/ob_ddl_task.h"

namespace oceanbase
{
namespace rootserver
{

class ObCheckConstraintValidationTask : public share::ObAsyncTask
{
public:
  ObCheckConstraintValidationTask(
      const uint64_t tenant_id,
      const int64_t data_table_id,
      const int64_t constraint_id,
      const int64_t target_object_id,
      const int64_t schema_version,
      const common::ObCurTraceId::TraceId &trace_id,
      const int64_t task_id,
      const bool check_table_empty,
      const obrpc::ObAlterTableArg::AlterConstraintType alter_constraint_type);
  virtual ~ObCheckConstraintValidationTask() = default;
  virtual int process() override;
  virtual int64_t get_deep_copy_size() const override { return sizeof(*this); }
  virtual ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const override;
private:
  uint64_t tenant_id_;
  int64_t data_table_id_;
  int64_t constraint_id_;
  int64_t target_object_id_;
  int64_t schema_version_;
  common::ObCurTraceId::TraceId trace_id_;
  int64_t task_id_;
  const bool check_table_empty_;
  obrpc::ObAlterTableArg::AlterConstraintType alter_constraint_type_;
};

class ObForeignKeyConstraintValidationTask : public share::ObAsyncTask
{
public:
  ObForeignKeyConstraintValidationTask(
      const uint64_t tenant_id,
      const int64_t data_table_id,
      const int64_t foregin_key_id,
      const int64_t schema_version,
      const common::ObCurTraceId::TraceId &trace_id,
      const int64_t task_id);
  virtual ~ObForeignKeyConstraintValidationTask() = default;
  virtual int process() override;
  virtual int64_t get_deep_copy_size() const override { return sizeof(*this); }
  virtual ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const override;
  static int get_foreign_key_info(const share::schema::ObTableSchema *table_schema, const int64_t foreign_key_id, share::schema::ObForeignKeyInfo &fk_info);
private:
  int check_fk_by_send_sql() const;
  int get_column_names(const share::schema::ObTableSchema &table_schema,
      const common::ObIArray<uint64_t> &column_ids,
      common::ObIArray<ObString> &column_name_str) const;
  int check_fk_constraint_data_valid(
      const share::schema::ObTableSchema &child_table_schema,
      const share::schema::ObDatabaseSchema &child_database_schema,
      const share::schema::ObTableSchema &parent_table_schema,
      const share::schema::ObDatabaseSchema &parent_database_schema,
      const share::schema::ObForeignKeyInfo &fk_info,
      const bool is_oracle_mode) const;
private:
  uint64_t tenant_id_;
  int64_t data_table_id_;
  int64_t foregin_key_id_;
  int64_t schema_version_;
  common::ObCurTraceId::TraceId trace_id_;
  int64_t task_id_;
};

class ObConstraintTask : public ObDDLTask
{
public:
  ObConstraintTask();
  virtual ~ObConstraintTask() = default;
  int init(
      const int64_t task_id,
      const share::schema::ObTableSchema *table_schema,
      const int64_t object_id,
      const share::ObDDLType ddl_type,
      const int64_t schema_version,
      const obrpc::ObAlterTableArg &alter_table_arg,
      const int64_t consumer_group_id,
      const int64_t parent_task_id = 0,
      const int64_t status = share::ObDDLTaskStatus::WAIT_TRANS_END,
      const int64_t snapshot_version = 0);
  int init(const ObDDLTaskRecord &task_record);
  virtual int process() override;
  int update_check_constraint_finish(const int ret_code);
  virtual int serialize_params_to_message(char *buf, const int64_t buf_size, int64_t &pos) const override;
  virtual int deserlize_params_from_message(const uint64_t tenant_id, const char *buf, const int64_t buf_size, int64_t &pos) override;
  virtual int64_t get_serialize_param_size() const override;
  virtual void flt_set_task_span_tag() const override;
  virtual void flt_set_status_span_tag() const override;
  virtual int cleanup_impl() override;
private:
  int hold_snapshot(const int64_t snapshot_version);
  int release_snapshot(const int64_t snapshot_version);
  int wait_trans_end();
  int validate_constraint_valid();
  int fail();
  int succ();
  int send_check_constraint_request();
  int send_fk_constraint_request();
  int set_foreign_key_constraint_validated();
  int check_column_is_nullable(const uint64_t column_id, bool &is_nullable) const;
  int set_check_constraint_validated();
  int set_constraint_validated();
  int set_new_not_null_column_validate();
  int remove_task_record();
  int report_error_code();
  int report_check_constraint_error_code();
  int report_foreign_key_constraint_error_code();
  int rollback_failed_schema();
  int rollback_failed_check_constraint();
  int rollback_failed_foregin_key();
  int rollback_failed_add_not_null_columns();
  int set_drop_constraint_ddl_stmt_str(
      obrpc::ObAlterTableArg &alter_table_arg,
      common::ObIAllocator &allocator);
  int set_alter_constraint_ddl_stmt_str_for_check(
      obrpc::ObAlterTableArg &alter_table_arg,
      common::ObIAllocator &allocator);
  int set_alter_constraint_ddl_stmt_str_for_fk(
      obrpc::ObAlterTableArg &alter_table_arg,
      common::ObIAllocator &allocator);
  int check_replica_end(bool &is_end);
  int check_health();
  int release_ddl_locks();
private:
  static const int64_t OB_CONSTRAINT_TASK_VERSION = 1;
  common::TCRWLock lock_;
  ObDDLWaitTransEndCtx wait_trans_ctx_;
  obrpc::ObAlterTableArg alter_table_arg_;
  common::ObArenaAllocator allocator_;
  ObRootService *root_service_;
  int64_t check_job_ret_code_;
  int64_t check_replica_request_time_;
  bool is_table_hidden_;
  bool snapshot_held_;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_CHECK_CONSTRAINT_TASK_H

