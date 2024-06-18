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

namespace oceanbase
{
using namespace common;
using namespace observer;
namespace sql
{
ObMaintainObjDepInfoTask::ObMaintainObjDepInfoTask (const uint64_t tenant_id)
  : tenant_id_(tenant_id),
    gctx_(GCTX),
    rs_rpc_proxy_(*GCTX.rs_rpc_proxy_),
    view_schema_(&alloc_),
    reset_view_column_infos_(false)
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
    rs_rpc_proxy_(rs_rpc_proxy),
    view_schema_(&alloc_),
    reset_view_column_infos_(false)
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
    // multi views in one stmt share ref_obj_array, just update own dep
    if (view_schema_.get_table_id() != dep_objs.at(i).dep_obj_key_.dep_obj_id_) {
      /*
      * create view will maintain -1 as dep obj id, but will not call this function
      * if current view level is less than 1 will maintain -1 as dep obj id, try to remove task like this earlier
      */
      continue;
    }
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
  DISABLE_SQL_MEMLEAK_GUARD;
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
    OZ ((static_cast<ObMaintainObjDepInfoTask *> (task))->assign_view_schema(view_schema_));
    OX ((static_cast<ObMaintainObjDepInfoTask *> (task))->reset_view_column_infos_ = reset_view_column_infos_);
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
    dep_obj_info_arg.reset_view_column_infos_ = reset_view_column_infos_;
    OZ (gctx_.schema_service_->async_refresh_schema(tenant_id_, last_version));
    OZ (gctx_.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard));
    OZ (check_and_build_dep_info_arg(schema_guard, dep_obj_info_arg,
    insert_dep_objs_, share::schema::ObReferenceObjTable::INSERT_OP));
    OZ (check_and_build_dep_info_arg(schema_guard, dep_obj_info_arg,
    update_dep_objs_, share::schema::ObReferenceObjTable::UPDATE_OP));
    OZ (check_and_build_dep_info_arg(schema_guard, dep_obj_info_arg,
    delete_dep_objs_, share::schema::ObReferenceObjTable::DELETE_OP));
    if (OB_FAIL(ret)) {
    } else if (view_schema_.is_valid() && OB_FAIL(dep_obj_info_arg.schema_.assign(view_schema_))) {
      LOG_WARN("failed to assign view schema", K(ret));
    } else if (dep_obj_info_arg.insert_dep_objs_.empty()
              && dep_obj_info_arg.update_dep_objs_.empty()
              && dep_obj_info_arg.delete_dep_objs_.empty()
              && !dep_obj_info_arg.schema_.is_valid()) {
      // do nothing
    } else if (OB_FAIL(rs_rpc_proxy_.maintain_obj_dependency_info(dep_obj_info_arg))) {
      LOG_WARN("failed to maintain_obj_dependency_info", K(ret), K(dep_obj_info_arg));
    }
  }
  return ret;
}

int ObMaintainObjDepInfoTask::assign_view_schema(const ObTableSchema &view_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(view_schema_.assign(view_schema))) {
    LOG_WARN("failed to copy view schema", K(ret));
  }
  return ret;
}

int ObMaintainDepInfoTaskQueue::init(const int64_t thread_cnt, const int64_t queue_size)
{
  int ret = OB_SUCCESS;
  auto attr = SET_USE_500("DepInfoTaskQ");
  if (OB_FAIL(ObAsyncTaskQueue::init(thread_cnt, queue_size, "MaintainDepInfoTaskQueue", OB_MALLOC_MIDDLE_BLOCK_SIZE))) {
    LOG_WARN("failed to init base queue", K(ret));
  } else if (OB_FAIL(view_info_set_.create(INIT_BKT_SIZE, attr, attr))) {
    LOG_WARN("failed to init view set", K(ret));
  } else if (OB_FAIL(sys_view_consistent_.create(INIT_BKT_SIZE, attr, attr))) {
    LOG_WARN("failed to init sys view set", K(ret));
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
      IGNORE_RETURN lib::Thread::update_loop_ts();
      if (REACH_TIME_INTERVAL(6 * 1000 * 1000)) {
        if (0 == queue_.size() && 0 != view_info_set_.size()) {
          LOG_WARN("queue size not match", K(queue_.size()), K(view_info_set_.size()));
          view_info_set_.clear();
        }
        if (sys_view_consistent_.size() >= MAX_SYS_VIEW_SIZE) {
          // ignore ret
          LOG_WARN("sys_view_consistent size too much", K(sys_view_consistent_.size()));
          sys_view_consistent_.clear();
        }
        LOG_INFO("[ASYNC TASK QUEUE]", K(queue_.size()), K(sys_view_consistent_.size()));
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
            if (is_queue_almost_full()) {
              ret = OB_SIZE_OVERFLOW;
              LOG_WARN("push task to queue failed", K(ret));
            } else if (OB_FAIL(queue_.push(task))) {
              LOG_WARN("push task to queue failed", K(ret));
            } else {
              rescheduled = true;
            }
          }
        } else {
          const ObTableSchema &view_schema = (static_cast<ObMaintainObjDepInfoTask *> (task))->get_view_schema();
          if (view_schema.is_valid()) {
            // ignore ret
            int tmp_ret = OB_SUCCESS;
            if (OB_SUCCESS != (tmp_ret = view_info_set_.erase_refactored(view_schema.get_table_id()))) {
              LOG_WARN("failed to erase obj id", K(tmp_ret), K(view_schema.get_table_id()));
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
