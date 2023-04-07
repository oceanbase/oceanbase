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

#ifndef OCEANBASE_ROOTSERVER_OB_DROP_INDEX_TASK_H
#define OCEANBASE_ROOTSERVER_OB_DROP_INDEX_TASK_H

#include "rootserver/ddl_task/ob_ddl_task.h"

namespace oceanbase
{
namespace rootserver
{

class ObDropIndexTask : public ObDDLTask
{
public:
  ObDropIndexTask();
  virtual ~ObDropIndexTask();
  int init(
      const uint64_t tenant_id,
      const int64_t task_id,
      const uint64_t data_table_id,
      const uint64_t index_table_id,
      const int64_t schema_version,
      const int64_t parent_task_id,
      const int64_t consumer_group_id,
      const obrpc::ObDropIndexArg &drop_index_arg);
  int init(const ObDDLTaskRecord &task_record);
  virtual int process() override;
  virtual bool is_valid() const override;
  virtual int serialize_params_to_message(char *buf, const int64_t buf_size, int64_t &pos) const override;
  virtual int deserlize_params_from_message(const uint64_t tenant_id, const char *buf, const int64_t buf_size, int64_t &pos) override;
  virtual int64_t get_serialize_param_size() const override;
  INHERIT_TO_STRING_KV("ObDDLTask", ObDDLTask, KP_(root_service));
  virtual void flt_set_task_span_tag() const override;
  virtual void flt_set_status_span_tag() const override;
  virtual int cleanup_impl() override;
private:
  int check_switch_succ();

  int prepare(const share::ObDDLTaskStatus new_status);
  int set_write_only(const share::ObDDLTaskStatus new_status);
  int set_unusable(const share::ObDDLTaskStatus new_status);
  int update_index_status(const share::schema::ObIndexStatus new_status);
  int drop_index_impl();
  int drop_index(const share::ObDDLTaskStatus new_status);
  int succ();
  int fail();
  int deep_copy_index_arg(common::ObIAllocator &allocator,
                          const obrpc::ObDropIndexArg &src_index_arg,
                          obrpc::ObDropIndexArg &dst_index_arg);
  virtual bool is_error_need_retry(const int ret_code) override
  {
    UNUSED(ret_code);
    // we should always retry on drop index task
    return task_status_ < share::ObDDLTaskStatus::DROP_SCHEMA;
  }
private:
  static const int64_t OB_DROP_INDEX_TASK_VERSION = 1;
  ObDDLWaitTransEndCtx wait_trans_ctx_;
  ObRootService *root_service_;
  obrpc::ObDropIndexArg drop_index_arg_;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_DROP_INDEX_TASK_H

