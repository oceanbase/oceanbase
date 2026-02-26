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

#ifndef __OB_RS_SYS_DDL_SCHEDULER_UTIL_H__
#define __OB_RS_SYS_DDL_SCHEDULER_UTIL_H__

#include "observer/omt/ob_multi_tenant.h" // for ObMultiTenant
#include "rootserver/ddl_task/ob_ddl_scheduler.h" // for ObDDLScheduler

namespace oceanbase
{
namespace rootserver
{

#define SYS_DDL_SCHEDULER_FUNC(func_name)                                                 \
  template <typename... Args> static int func_name(Args &&...args) {                      \
    int ret = OB_SUCCESS;                                                                 \
    if (OB_ISNULL(GCTX.omt_)) {                                                           \
      ret = OB_INVALID_ARGUMENT;                                                          \
      LOG_WARN("invalid argument", KR(ret), KP(GCTX.omt_));                               \
    } else if (OB_UNLIKELY(!GCTX.omt_->has_tenant(OB_SYS_TENANT_ID))) {                   \
      ret = OB_TENANT_NOT_EXIST;                                                          \
      LOG_WARN("local server does not have SYS tenant resource", KR(ret));                \
    } else if (OB_FAIL(ObDDLUtil::check_local_is_sys_leader())) {                         \
      LOG_WARN("local is not sys tenant leader", KR(ret));                                \
    } else {                                                                              \
      MTL_SWITCH(OB_SYS_TENANT_ID) {                                                      \
        rootserver::ObDDLScheduler* sys_ddl_scheduler = MTL(rootserver::ObDDLScheduler*); \
        if (OB_ISNULL(sys_ddl_scheduler)) {                                               \
          ret = OB_ERR_UNEXPECTED;                                                        \
          LOG_WARN("sys ddl scheduler service is null", KR(ret));                         \
        } else if (OB_FAIL(sys_ddl_scheduler->func_name(std::forward<Args>(args)...))) {  \
          LOG_WARN("fail to execute ddl scheduler function", KR(ret));                    \
        }                                                                                 \
      }                                                                                   \
    }                                                                                     \
    return ret;                                                                           \
  }

class ObSysDDLSchedulerUtil
{
public:
  SYS_DDL_SCHEDULER_FUNC(abort_redef_table);
  SYS_DDL_SCHEDULER_FUNC(cache_auto_split_task);
  SYS_DDL_SCHEDULER_FUNC(copy_table_dependents);
  SYS_DDL_SCHEDULER_FUNC(create_ddl_task);
  SYS_DDL_SCHEDULER_FUNC(destroy_task);
  SYS_DDL_SCHEDULER_FUNC(finish_redef_table);
  SYS_DDL_SCHEDULER_FUNC(get_task_record);
  SYS_DDL_SCHEDULER_FUNC(notify_update_autoinc_end);
  SYS_DDL_SCHEDULER_FUNC(modify_redef_task);
  SYS_DDL_SCHEDULER_FUNC(on_column_checksum_calc_reply);
  SYS_DDL_SCHEDULER_FUNC(on_ddl_task_finish);
  SYS_DDL_SCHEDULER_FUNC(on_ddl_task_prepare);
  SYS_DDL_SCHEDULER_FUNC(on_sstable_complement_job_reply);
  SYS_DDL_SCHEDULER_FUNC(prepare_alter_table_arg);
  SYS_DDL_SCHEDULER_FUNC(recover_task);
  SYS_DDL_SCHEDULER_FUNC(remove_inactive_ddl_task);
  SYS_DDL_SCHEDULER_FUNC(schedule_auto_split_task);
  SYS_DDL_SCHEDULER_FUNC(schedule_ddl_task);
  SYS_DDL_SCHEDULER_FUNC(start_redef_table);
  SYS_DDL_SCHEDULER_FUNC(update_ddl_task_active_time);
  SYS_DDL_SCHEDULER_FUNC(notify_refresh_related_mviews_task_end);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSysDDLSchedulerUtil);
};// end ObSysDDLSchedulerUtil

class ObSysDDLReplicaBuilderUtil
{
public:
  static int push_task(ObAsyncTask &task);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSysDDLReplicaBuilderUtil);
};// end ObSysDDLReplicaBuilderUtil

}
}
#endif /* __OB_RS_SYS_DDL_SCHEDULER_UTIL_H__ */
