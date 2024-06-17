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

#ifndef OCEANBASE_ROOTSERVER_OB_BUILD_MVIEW_TASK_H
#define OCEANBASE_ROOTSERVER_OB_BUILD_MVIEW_TASK_H

#include "rootserver/ddl_task/ob_ddl_task.h"

namespace oceanbase
{
namespace rootserver
{
class ObBuildMViewTask : public ObDDLTask
{
public:
  ObBuildMViewTask();
  virtual ~ObBuildMViewTask();
  int init(const ObDDLTaskRecord &task_record);
  int init(const uint64_t tenant_id,
           const int64_t task_id,
           const share::schema::ObTableSchema *mview_schema,
           const int64_t schema_version,
           const int64_t parallel,
           const int64_t consumer_group_id,
           const obrpc::ObMViewCompleteRefreshArg &mview_complete_refresh_arg,
           const int64_t parent_task_id,
           const int64_t task_status = share::ObDDLTaskStatus::START_REFRESH_MVIEW_TASK,
           const int64_t snapshot_version = 0);
  virtual int process() override;
  virtual int cleanup_impl() override;
  virtual void flt_set_task_span_tag() const override;
  virtual void flt_set_status_span_tag() const override;
  virtual int serialize_params_to_message(char *buf,
                                          const int64_t buf_size,
                                          int64_t &pos) const override;
  virtual int deserialize_params_from_message(const uint64_t tenant_id,
                                              const char *buf,
                                              const int64_t buf_size,
                                              int64_t &pos) override;
  virtual int64_t get_serialize_param_size() const override;
  virtual bool task_can_retry() const { return false; } //build mview task should not retry
  int on_child_task_prepare(const int64_t task_id);

private:
  int start_refresh_mview_task();
  int wait_child_task_finish();
  int enable_mview();
  int succ();
  int clean_on_fail();
  int check_health();
  int mview_complete_refresh(obrpc::ObMViewCompleteRefreshRes &res);
  int update_task_message();
  int set_mview_complete_refresh_task_id(const int64_t task_id);

private:
  static const int64_t OB_BUILD_MVIEW_TASK_VERSION = 1;
  uint64_t &mview_table_id_;
  ObRootService *root_service_;
  obrpc::ObMViewCompleteRefreshArg arg_;
  int64_t mview_complete_refresh_task_id_;
};
} // namespace rootserver
} // namespace oceanbase

#endif
