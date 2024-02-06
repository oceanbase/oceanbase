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

#ifndef OCEANBASE_ROOTSERVER_OB_MODIFY_AUTOINC_TASK_H
#define OCEANBASE_ROOTSERVER_OB_MODIFY_AUTOINC_TASK_H

#include "rootserver/ddl_task/ob_ddl_task.h"

namespace oceanbase
{
namespace rootserver
{

class ObUpdateAutoincSequenceTask : public share::ObAsyncTask
{
public:
  ObUpdateAutoincSequenceTask(
    const uint64_t tenant_id,
    const int64_t data_table_id,
    const int64_t dest_table_id,
    const int64_t schema_version,
    const uint64_t column_id,
    const ObObjType &orig_column_type,
    const ObSQLMode &sql_mode,
    const common::ObCurTraceId::TraceId &trace_id,
    const int64_t task_id);
  virtual ~ObUpdateAutoincSequenceTask() = default;
  virtual int process() override;
  virtual int64_t get_deep_copy_size() const override { return sizeof(*this); }
  virtual ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const override;
private:
  uint64_t tenant_id_;
  int64_t data_table_id_;
  int64_t dest_table_id_;
  int64_t schema_version_;
  uint64_t column_id_;
  ObObjType orig_column_type_;
  ObSQLMode sql_mode_;
  common::ObCurTraceId::TraceId trace_id_;
  int64_t task_id_;
};

class ObModifyAutoincTask : public ObDDLTask
{
public:
  ObModifyAutoincTask();
  virtual ~ObModifyAutoincTask() = default;
  int init(const uint64_t tenant_id,
           const int64_t task_id,
           const int64_t table_id,
           const int64_t schema_version,
           const int64_t consumer_group_id,
           const obrpc::ObAlterTableArg &alter_table_arg,
           const int64_t task_status = share::ObDDLTaskStatus::MODIFY_AUTOINC,
           const int64_t snapshot_version = 0);
  int init(const ObDDLTaskRecord &task_record);
  virtual int process() override;
  virtual int serialize_params_to_message(char *buf, const int64_t buf_size, int64_t &pos) const override;
  virtual int deserlize_params_from_message(const uint64_t tenant_id, const char *buf, const int64_t buf_size, int64_t &pos) override;
  virtual int64_t get_serialize_param_size() const override;
  int notify_update_autoinc_finish(const uint64_t autoinc_val, const int ret_code);
  virtual void flt_set_task_span_tag() const override;
  virtual void flt_set_status_span_tag() const override;
  virtual int cleanup_impl() override;
private:
  int unlock_table();
  int modify_autoinc();
  int wait_trans_end();
  int fail();
  int success();
  int set_schema_available();
  int rollback_schema();
  int check_update_autoinc_end(bool &is_end);
  int check_health();
private:
  static const int64_t OB_MODIFY_AUTOINC_TASK_VERSION = 1L; 
  common::TCRWLock lock_;
  ObDDLWaitTransEndCtx wait_trans_ctx_;
  obrpc::ObAlterTableArg alter_table_arg_;
  int64_t update_autoinc_job_ret_code_;
  int64_t update_autoinc_job_time_;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_MODIFY_AUTOINC_TASK_H
