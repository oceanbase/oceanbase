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

#define USING_LOG_PREFIX SQL_EXE
#include "sql/executor/ob_maintain_dependency_info_task.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
using namespace common;
using namespace observer;
namespace sql
{
ObMaintainObjDepInfoTask::ObMaintainObjDepInfoTask (const uint64_t tenant_id)
  : tenant_id_(tenant_id),
    gctx_(GCTX),
    rs_rpc_proxy_(*GCTX.rs_rpc_proxy_)
{
  set_retry_times(0);
}

ObMaintainObjDepInfoTask::ObMaintainObjDepInfoTask (
  uint64_t tenant_id,
  obrpc::ObCommonRpcProxy &rs_rpc_proxy,
  const DepObjKeyItemList &insert_dep_objs,
  const DepObjKeyItemList &update_dep_objs,
  const DepObjKeyItemList &delete_dep_objs)
  : tenant_id_(tenant_id),
    gctx_(GCTX),
    rs_rpc_proxy_(rs_rpc_proxy)
{
  set_retry_times(0);
  insert_dep_objs_.assign(insert_dep_objs);
  update_dep_objs_.assign(update_dep_objs);
  delete_dep_objs_.assign(delete_dep_objs);
}

int ObMaintainObjDepInfoTask::check_cur_maintain_task_is_valid(
    const share::schema::ObReferenceObjTable::ObDependencyObjKey &dep_obj_key,
    int64_t dep_obj_schema_version,
    share::schema::ObSchemaGetterGuard &schema_guard,
    bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  switch (dep_obj_key.dep_obj_type_) {
  case share::schema::ObObjectType::TABLE:
  case share::schema::ObObjectType::VIEW: {
    const share::schema::ObSimpleTableSchemaV2 *table_schema = nullptr;
    if (OB_FAIL(schema_guard.get_simple_table_schema(
                tenant_id_,
                dep_obj_key.dep_obj_id_, table_schema))) {
      LOG_WARN("failed to get table schema", K(ret), K(dep_obj_key));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema is null", K(ret));
    } else if (table_schema->get_schema_version() <= dep_obj_schema_version) {
      is_valid = true;
    }
    break;
  }
  default:
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected type", K(ret));
    break;
  }
  return ret;
}

int ObMaintainObjDepInfoTask::check_and_build_dep_info_arg(
    share::schema::ObSchemaGetterGuard &schema_guard,
    obrpc::ObDependencyObjDDLArg &dep_obj_info_arg,
    const common::ObIArray<DepObjKeyItem> &dep_objs,
    share::schema::ObReferenceObjTable::ObSchemaRefObjOp op)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  for (int64_t i = 0 ; OB_SUCC(ret) && i < dep_objs.count(); ++i) {
    if (OB_FAIL(check_cur_maintain_task_is_valid(
                dep_objs.at(i).dep_obj_key_,
                dep_objs.at(i).dep_obj_item_.dep_obj_schema_version_,
                schema_guard,
                is_valid))) {
      LOG_WARN("failed to check current maintain task is valid");
    } else if (is_valid) {
      switch (op) {
      case share::schema::ObReferenceObjTable::INSERT_OP:
        OZ (dep_obj_info_arg.insert_dep_objs_.push_back(dep_objs.at(i)));
        break;
      case share::schema::ObReferenceObjTable::DELETE_OP:
        OZ (dep_obj_info_arg.delete_dep_objs_.push_back(dep_objs.at(i)));
        break;
      case share::schema::ObReferenceObjTable::UPDATE_OP:
        OZ (dep_obj_info_arg.update_dep_objs_.push_back(dep_objs.at(i)));
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected type", K(ret));
        break;
      }
    }
  }
  return ret;
}

share::ObAsyncTask *ObMaintainObjDepInfoTask::deep_copy(char *buf, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  ObAsyncTask *task = nullptr;
  const int64_t need_size = get_deep_copy_size();
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buffer is null", K(ret));
  } else if (OB_UNLIKELY(buf_size < need_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buffer size is not enough", K(ret), K(buf_size), K(need_size));
  } else {
    task = new (buf) ObMaintainObjDepInfoTask(tenant_id_,
                                              rs_rpc_proxy_,
                                              insert_dep_objs_,
                                              update_dep_objs_,
                                              delete_dep_objs_);
  }
  return task;
}

int ObMaintainObjDepInfoTask::process()
{
  int ret = OB_SUCCESS;
  int64_t last_version = 0;
  share::schema::ObSchemaGetterGuard schema_guard;
  SMART_VAR(obrpc::ObDependencyObjDDLArg, dep_obj_info_arg) {
    dep_obj_info_arg.tenant_id_ = tenant_id_;
    dep_obj_info_arg.exec_tenant_id_ = tenant_id_;
    OZ (gctx_.schema_service_->async_refresh_schema(tenant_id_, last_version));
    OZ (gctx_.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard));
    OZ (check_and_build_dep_info_arg(schema_guard, dep_obj_info_arg,
    insert_dep_objs_, share::schema::ObReferenceObjTable::INSERT_OP));
    OZ (check_and_build_dep_info_arg(schema_guard, dep_obj_info_arg,
    update_dep_objs_, share::schema::ObReferenceObjTable::UPDATE_OP));
    OZ (check_and_build_dep_info_arg(schema_guard, dep_obj_info_arg,
    delete_dep_objs_, share::schema::ObReferenceObjTable::DELETE_OP));
    if (OB_FAIL(ret)) {
    } else if (dep_obj_info_arg.insert_dep_objs_.empty()
              && dep_obj_info_arg.update_dep_objs_.empty()
              && dep_obj_info_arg.delete_dep_objs_.empty()) {
      // do nothing
    } else if (OB_FAIL(rs_rpc_proxy_.maintain_obj_dependency_info(dep_obj_info_arg))) {
      LOG_WARN("failed to maintain_obj_dependency_info", K(ret), K(dep_obj_info_arg));
    }
  }
  return ret;
}

void ObMaintainDepInfoTaskQueue::run2()
{
  int ret = OB_SUCCESS;
  LOG_INFO("async task queue start");
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObAddr zero_addr;
    while (!stop_) {
      if (REACH_TIME_INTERVAL(600 * 1000 * 1000)) {
        LOG_INFO("[ASYNC TASK QUEUE]", "queue_size", queue_.size());
      }
      if (last_execute_time_ > 0
         && static_cast<int64_t>(GCONF._ob_obj_dep_maint_task_interval) > 0) {
        while (!stop_ && OB_SUCC(ret)) {
          int64_t now = ObTimeUtility::current_time();
          int64_t sleep_time =
          last_execute_time_ + GCONF._ob_obj_dep_maint_task_interval - now;
          if (sleep_time > 0) {
            ob_usleep(static_cast<int32_t>(MIN(sleep_time, SLEEP_INTERVAL)));
          } else {
            break;
          }
        }
      }
      share::ObAsyncTask *task = NULL;
      ret = pop(task);
      if (OB_FAIL(ret))  {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("pop task from queue failed", K(ret));
        }
      } else if (NULL == task) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pop return a null task", K(ret));
      } else {
        bool rescheduled = false;
        if (task->get_last_execute_time() > 0) {
          while (!stop_ && OB_SUCC(ret)) {
            int64_t now = ObTimeUtility::current_time();
            int64_t sleep_time = task->get_last_execute_time() + task->get_retry_interval() - now;
            if (sleep_time > 0) {
              ob_usleep(static_cast<int32_t>(MIN(sleep_time, SLEEP_INTERVAL)));
            } else {
              break;
            }
          }
        }
        // generate trace id
        ObCurTraceId::init(zero_addr);
        // just do it
        ret = task->process();
        if (OB_FAIL(ret)) {
          LOG_WARN("task process failed, start retry", "max retry time",
              task->get_retry_times(), "retry interval", task->get_retry_interval(),
              K(ret));
          if (task->get_retry_times() > 0) {
            task->set_retry_times(task->get_retry_times() - 1);
            task->set_last_execute_time(ObTimeUtility::current_time());
            if (OB_FAIL(queue_.push(task))) {
              LOG_ERROR("push task to queue failed", K(ret));
            } else {
              rescheduled = true;
            }
          }
        }
        last_execute_time_ = ObTimeUtility::current_time();
        if (!rescheduled) {
          task->~ObAsyncTask();
          allocator_.free(task);
          task = NULL;
        }
      }
    }
  }
  LOG_INFO("async task queue stop");
}

}  // namespace sql
}  // namespace oceanbase
