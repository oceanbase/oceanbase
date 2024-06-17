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

#ifndef OCEANBASE_ROOTSERVER_OB_DDL_RETRY_TASK_H
#define OCEANBASE_ROOTSERVER_OB_DDL_RETRY_TASK_H

#include "rootserver/ddl_task/ob_ddl_task.h"
#include "rpc/obrpc/ob_rpc_result_code.h"

namespace oceanbase
{
namespace rootserver
{
class ObRootService;

class ObDDLRetryTask : public ObDDLTask
{
public:
  ObDDLRetryTask();
  virtual ~ObDDLRetryTask();
  int init(
      const uint64_t tenant_id,
      const int64_t task_id,
      const uint64_t object_id,
      const int64_t schema_version,
      const int64_t consumer_group_id,
      const int32_t sub_task_trace_id,
      const share::ObDDLType &type,
      const obrpc::ObDDLArg *ddl_arg,
      const int64_t task_status = share::ObDDLTaskStatus::PREPARE);
  int init(const ObDDLTaskRecord &task_record);
  virtual int process() override;
  virtual bool is_valid() const override;
  virtual int serialize_params_to_message(char *buf, const int64_t buf_size, int64_t &pos) const override;
  virtual int deserialize_params_from_message(const uint64_t tenant_id, const char *buf, const int64_t buf_size, int64_t &pos) override;
  virtual int64_t get_serialize_param_size() const override;
  static int update_task_status_wait_child_task_finish(
        common::ObMySQLTransaction &trans,
        const uint64_t tenant_id,
        const int64_t task_id);
  virtual void flt_set_task_span_tag() const override;
  virtual void flt_set_status_span_tag() const override;
  virtual int cleanup_impl() override;
private:
  int check_health();
  int prepare(const share::ObDDLTaskStatus next_task_status);
  int drop_schema(const share::ObDDLTaskStatus next_task_status);
  int wait_alter_table(const share::ObDDLTaskStatus next_task_status);
  int succ();
  int fail();
  int deep_copy_ddl_arg(common::ObIAllocator &allocator, const share::ObDDLType &ddl_type, const obrpc::ObDDLArg *source_arg);
  int init_compat_mode(const share::ObDDLType &ddl_type, const obrpc::ObDDLArg *source_arg);
  int get_forward_user_message(const obrpc::ObRpcResultCode &rcode);
  int check_schema_change_done();
  virtual bool is_error_need_retry(const int ret_code) override
  {
    return common::OB_PARTITION_NOT_EXIST != ret_code && ObDDLTask::is_error_need_retry(ret_code);
  }
private:
  static const int64_t OB_DDL_RETRY_TASK_VERSION = 1L;
  obrpc::ObDDLArg *ddl_arg_;
  ObRootService *root_service_;
  int64_t affected_rows_;
  common::ObString forward_user_message_;
  common::ObArenaAllocator allocator_;
  obrpc::ObAlterTableRes alter_table_res_; // in memory
  bool is_schema_change_done_;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_DDL_RETRY_TASK_H
