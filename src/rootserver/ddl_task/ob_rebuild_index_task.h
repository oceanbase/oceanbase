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

#ifndef OCEANBASE_ROOTSERVER_OB_REBUILD_INDEX_TASK_H
#define OCEANBASE_ROOTSERVER_OB_REBUILD_INDEX_TASK_H

#include "rootserver/ddl_task/ob_ddl_task.h"

namespace oceanbase
{
namespace rootserver
{

class ObRebuildIndexTask : public ObDDLTask
{
public:
  ObRebuildIndexTask();
  virtual ~ObRebuildIndexTask();
  int init(
      const uint64_t tenant_id,
      const int64_t task_id,
      const share::ObDDLType &ddl_type,
      const uint64_t data_table_id,
      const uint64_t index_table_id,
      const int64_t schema_version,
      const int64_t parent_task_id,
      const int64_t consumer_group_id,
      const int32_t sub_task_trace_id,
      const int64_t parallelism,
      const uint64_t tenant_data_version,
      const obrpc::ObRebuildIndexArg &rebuild_index_arg);
  int init(const ObDDLTaskRecord &task_record);
  virtual int process() override;
  virtual bool is_valid() const override;
  virtual int serialize_params_to_message(char *buf, const int64_t buf_size, int64_t &pos) const override;
  virtual int deserialize_params_from_message(const uint64_t tenant_id, const char *buf, const int64_t buf_size, int64_t &pos) override;
  virtual int64_t get_serialize_param_size() const override;
  INHERIT_TO_STRING_KV("ObDDLTask", ObDDLTask, KP_(root_service));
  virtual int on_child_task_finish(const uint64_t child_task_key,
                                   const int ret_code) override
  {
    return OB_SUCCESS;
  }
  virtual int cleanup_impl() override;
  uint64_t get_new_index_id() { return new_index_id_; };
private:
  int check_switch_succ();
  int prepare(const share::ObDDLTaskStatus new_status);
  int rebuild_index_impl();
  int drop_index_impl(const bool is_old_index);
  int switch_index_name(const ObDDLTaskStatus next_task_status);
  int create_and_wait_rebuild_task_finish(const share::ObDDLTaskStatus new_status);
  int create_and_wait_drop_task_finish(const share::ObDDLTaskStatus new_status);
  int succ();
  int fail();
  int update_task_message();
  int check_ddl_task_finish(
      const int64_t tenant_id,
      int64_t &task_id,
      bool &is_finished);
  int get_new_index_table_id(
      ObSchemaGetterGuard &schema_guard,
      const int64_t tenant_id,
      const int64_t database_id,
      const int64_t data_table_id,
      const ObString &index_name,
      int64_t &index_id);
  int deep_copy_index_arg(
      common::ObIAllocator &allocator,
      const obrpc::ObRebuildIndexArg &src_index_arg,
      obrpc::ObRebuildIndexArg &dst_index_arg);
  virtual bool is_error_need_retry(const int ret_code) override
  {
    UNUSED(ret_code);
    // we should always retry on drop index task
    return task_status_ < share::ObDDLTaskStatus::DROP_SCHEMA;
  }
private:
  static const int64_t OB_REBUILD_INDEX_TASK_VERSION = 1;
  ObRootService *root_service_;
  obrpc::ObRebuildIndexArg rebuild_index_arg_;
  int64_t index_build_task_id_;
  int64_t index_drop_task_id_;
  uint64_t new_index_id_;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_REBUILD_INDEX_TASK_H
