/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX SERVER
#include "ob_vector_index_async_task.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "src/storage/ls/ob_ls.h"

namespace oceanbase
{
namespace share
{

bool ObVecAsyncTaskExector::check_operation_allow()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  bool bret = true;
  bool is_active_time = true;
  const bool is_not_support = false;
  if (is_not_support) {
    bret = false;
    LOG_DEBUG("skip this round, not support async task.");
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, tenant_data_version))) {
    bret = false;
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (tenant_data_version < DATA_VERSION_4_3_5_2) {
    bret = false;
    LOG_DEBUG("vector index async task can not work with data version less than 4_3_5_2", K(tenant_data_version));
  } else if (ObVecIndexAsyncTaskUtil::in_active_time(tenant_id_, is_active_time)) {
    bret = false;
    LOG_WARN("fail to get active time");
  } else if (!is_active_time) {
    bret = false;
    LOG_INFO("skip this round, not in active time.");
  }
  return bret;
}

int ObVecAsyncTaskExector::check_and_set_thread_pool()
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
  const bool is_not_support = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector index load task not inited", K(ret));
  } else if (is_not_support) {
    // skip
  } else if (OB_ISNULL(vector_index_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(tenant_id_));
  } else if (OB_FAIL(get_index_ls_mgr(index_ls_mgr))) {
    LOG_WARN("fail to get index ls mgr", K(ret), K(tenant_id_));
  } else {
    ObIAllocator *allocator = index_ls_mgr->get_async_task_opt().get_allocator();
    ObVecIndexAsyncTaskHandler &thread_pool_handle = vector_index_service_->get_vec_async_task_handle();
    if (0 == index_ls_mgr->get_complete_adapter_map().size()) { // no vector index exist, skip
    } else {
      common::ObSpinLockGuard init_guard(thread_pool_handle.lock_); // lock thread pool init to avoid init twice
      if (thread_pool_handle.get_tg_id() != INVALID_TG_ID) { // no need to init twice, skip
      } else if (OB_FAIL(thread_pool_handle.init())) {
        LOG_WARN("fail to init vec async task handle", K(ret), K(tenant_id_));
      } else if (OB_FAIL(thread_pool_handle.start())) {
        LOG_WARN("fail to start thread pool", K(ret), K(tenant_id_));
      }
    }
  }
  return ret;
}

int ObVecAsyncTaskExector::load_task(uint64_t &task_trace_base_num)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
  ObArray<ObVecIndexAsyncTaskCtx*> task_ctx_array;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector async task not init", KR(ret));
  } else if (!check_operation_allow()) { // skip
  } else if (OB_FAIL(get_index_ls_mgr(index_ls_mgr))) { // skip
    LOG_WARN("fail to get index ls mgr", K(ret), K(tenant_id_), K(ls_->get_ls_id()));
  } else {
    ObVecIndexAsyncTaskOption &task_opt = index_ls_mgr->get_async_task_opt();
    ObIAllocator *allocator = task_opt.get_allocator();
    const int64_t current_task_cnt = ObVecIndexAsyncTaskUtil::get_processing_task_cnt(task_opt);

    RWLock::RLockGuard lock_guard(index_ls_mgr->get_adapter_map_lock());
    FOREACH_X(iter, index_ls_mgr->get_complete_adapter_map(),
        OB_SUCC(ret) && (task_ctx_array.count() + current_task_cnt <= MAX_ASYNC_TASK_PROCESSING_COUNT)) {
      ObTabletID tablet_id = iter->first;
      ObPluginVectorIndexAdaptor *adapter = iter->second;
      if (OB_ISNULL(adapter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      } else if (adapter->is_need_async_optimal()) {
        int64_t new_task_id = OB_INVALID_ID;
        int64_t index_table_id = OB_INVALID_ID;
        bool inc_new_task = false;
        common::ObCurTraceId::TraceId new_trace_id;

        char *task_ctx_buf = static_cast<char *>(allocator->alloc(sizeof(ObVecIndexAsyncTaskCtx)));
        ObVecIndexAsyncTaskCtx* task_ctx = nullptr;
        if (OB_ISNULL(task_ctx_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("async task ctx is null", K(ret));
        } else if (FALSE_IT(task_ctx = new(task_ctx_buf) ObVecIndexAsyncTaskCtx())) {
        } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::fetch_new_task_id(tenant_id_, new_task_id))) {
          LOG_WARN("fail to fetch new task id", K(ret), K(tenant_id_));
        } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::get_table_id_from_adapter(adapter, tablet_id, index_table_id))) { // only get table 3 table_id to generate new task
          LOG_WARN("fail to get table id from adapter", K(ret), K(tablet_id));
        } else if (OB_INVALID_ID == index_table_id) {
          LOG_DEBUG("index table id is invalid, skip", K(ret)); // skip to next
        } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::fetch_new_trace_id(++task_trace_base_num, allocator, new_trace_id))) {
          LOG_WARN("fail to fetch new trace id", K(ret), K(tablet_id));
        } else {
          LOG_DEBUG("start load task", K(ret), K(tablet_id), K(tenant_id_), K(task_trace_base_num), K(ls_->get_ls_id()));
          // 1. update task_ctx to async task map
          task_ctx->tenant_id_ = tenant_id_;
          task_ctx->ls_ = ls_;
          task_ctx->task_status_.tablet_id_ = tablet_id.id();
          task_ctx->task_status_.tenant_id_ = tenant_id_;
          task_ctx->task_status_.table_id_ = index_table_id;
          task_ctx->task_status_.task_id_ = new_task_id;
          task_ctx->task_status_.task_type_ = ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_INDEX_OPTINAL;
          task_ctx->task_status_.trigger_type_ = ObVecIndexAsyncTaskTriggerType::OB_VEC_TRIGGER_AUTO;
          task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE;
          task_ctx->task_status_.trace_id_ = new_trace_id;
          task_ctx->allocator_.set_tenant_id(tenant_id_);
          if (OB_FAIL(index_ls_mgr->get_async_task_opt().add_task_ctx(tablet_id, task_ctx, inc_new_task))) { // not overwrite
            LOG_WARN("fail to add task ctx", K(ret));
          } else if (inc_new_task && OB_FAIL(task_ctx_array.push_back(task_ctx))) {
            LOG_WARN("fail to push back task status", K(ret), K(task_ctx));
          }
        }
        if (OB_FAIL(ret) || !inc_new_task) { // release memory when fail
          if (OB_NOT_NULL(task_ctx)) {
            task_ctx->~ObVecIndexAsyncTaskCtx();
            allocator->free(task_ctx); // arena need free
            task_ctx = nullptr;
          }
        }
      }
    }
    LOG_INFO("finish load async task", K(ret), K(ls_->get_ls_id()), K(task_ctx_array.count()), K(current_task_cnt));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(insert_new_task(task_ctx_array))) {
    LOG_WARN("fail to insert new tasks", K(ret), K(tenant_id_), K(ls_->get_ls_id()));
  }
  // clear on fail
  if (OB_FAIL(ret) && !task_ctx_array.empty()) {
    if (OB_FAIL(clear_task_ctxs(index_ls_mgr->get_async_task_opt(), task_ctx_array))) {
      LOG_WARN("fail to clear task ctx", K(ret));
    }
  }
  return ret;
}

int ObVecTaskManager::process_task()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_task())) {
    LOG_WARN("fail to create task", K(ret));
  }
  while (OB_SUCC(ret) && !task_ids_.empty()) {
    if (OB_FAIL(check_task_status())) {
      LOG_WARN("fail to check task status", K(ret));
    } else {
      ob_usleep(1LL * 1000 * 1000);
    }
  }
  return ret;
}

int ObVecTaskManager::create_task()
{
  int ret = OB_SUCCESS;
  uint64_t trace_base_num = 0;
  ObSEArray<ObTabletID, 1> tablet_ids;
  ObArray<ObVecIndexAsyncTaskCtx*> task_ctx_array;
  ObArenaAllocator allocator("VecTaskCtx", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id_, index_table_id_, tablet_ids))) {
    LOG_WARN("failed to get tablet ids", K(ret));
  } else {
    for (int i = 0; i < tablet_ids.count() && OB_SUCC(ret); i++) {
      int64_t new_task_id = OB_INVALID_ID;
      ObTabletID tablet_id = tablet_ids.at(i);
      ObVecIndexAsyncTaskCtx* task_ctx = nullptr;
      common::ObCurTraceId::TraceId new_trace_id;
      char *task_ctx_buf = static_cast<char *>(allocator.alloc(sizeof(ObVecIndexAsyncTaskCtx)));
      if (OB_ISNULL(task_ctx_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("async task ctx is null", K(ret));
      } else if (FALSE_IT(task_ctx = new(task_ctx_buf) ObVecIndexAsyncTaskCtx())) {
      } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::fetch_new_task_id(tenant_id_, new_task_id))) {
        LOG_WARN("fail to fetch new task id", K(ret), K(tenant_id_));
      } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::fetch_new_trace_id(++trace_base_num, &allocator, new_trace_id))) {
        LOG_WARN("fail to fetch new trace id", K(ret), K(tablet_id));
      } else {
        task_ctx->tenant_id_ = tenant_id_;
        task_ctx->task_status_.tablet_id_ = tablet_id.id();
        task_ctx->task_status_.tenant_id_ = tenant_id_;
        task_ctx->task_status_.table_id_ = index_table_id_;
        task_ctx->task_status_.task_id_ = new_task_id;
        task_ctx->task_status_.task_type_ = task_type_;
        task_ctx->task_status_.trigger_type_ = ObVecIndexAsyncTaskTriggerType::OB_VEC_TRIGGER_MANUAL;
        task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE;
        task_ctx->task_status_.trace_id_ = new_trace_id;
        task_ctx->task_status_.target_scn_.convert_from_ts(ObTimeUtility::current_time());
        if (OB_FAIL(task_ctx_array.push_back(task_ctx))) {
          LOG_WARN("fail to push back task status", K(ret), K(task_ctx));
        } else if (OB_FAIL(task_ids_.push_back(new_task_id))) {
          LOG_WARN("fail to push back task -id", K(ret), K(new_task_id));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::insert_new_task(tenant_id_, task_ctx_array))) {
    LOG_WARN("fail to insert new tasks", K(ret), K(tenant_id_));
  }
  return ret;
}

int ObVecTaskManager::check_task_status()
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObSEArray<int64_t, 4> finished_task;
  ObSEArray<int64_t, 4> tmp_task;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(sql_proxy));
  } else {
    for (int i = 0; i < task_ids_.count() && OB_SUCC(ret); i++) {
      ObSqlString sql;
      ObVecIndexFieldArray filters;
      ObVecIndexTaskStatusField field;
      field.field_name_ = "task_id";
      field.data_.uint_ = task_ids_.at(i);
      if (OB_FAIL(filters.push_back(field))) {
        LOG_WARN("fail to push back field", K(ret));
      } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::construct_read_task_sql(tenant_id_, OB_ALL_VECTOR_INDEX_TASK_HISTORY_TNAME,
          false, false, filters, *sql_proxy, sql))) {
        LOG_WARN("fail to construct read task sql", K(ret));
      } else {
        SMART_VAR(ObMySQLProxy::MySQLResult, res) {
          ObVecIndexTaskStatus task_result;
          sqlclient::ObMySQLResult* result = nullptr;
          if (OB_FAIL(sql_proxy->read(res, tenant_id_, sql.ptr()))) {
            LOG_WARN("fail to execute sql", KR(ret), K(sql));
          } else if (OB_ISNULL(result = res.get_result())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("error unexpected, query result must not be NULL", K(ret));
          } else if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("fail to get next row", K(ret));
            }
          } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::extract_one_task_sql_result(result, task_result))) {
            LOG_WARN("fail to extrace one result", K(ret));
          } else if (OB_FAIL(task_result.ret_code_)) {
            LOG_WARN("task exec failed", K(ret), K(task_result));
          } else if (OB_FAIL(finished_task.push_back(task_result.task_id_))) {
            LOG_WARN("fail to push back task id", K(ret));
          }
        } // end smart var.
      }
    } // end loop: for i in task_ids_.
  }

  if (OB_FAIL(ret)) {
    if (OB_EAGAIN == ret) {
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "call dbms_vector.refresh_index/rebuild_index before vector index adapter ready is");
      LOG_INFO("call dbms_vector.refresh_index/rebuild_index before vector index adapter ready is not supported, please try again", K(ret));
    }
  } else if (finished_task.empty()) {
  } else if (OB_FAIL(get_difference(finished_task, task_ids_, tmp_task))) {
    LOG_WARN("failed to get difference", K(ret), K(finished_task), K(task_ids_));
  } else if (FALSE_IT(task_ids_.reuse())) {
  } else if (OB_FAIL(task_ids_.assign(tmp_task))) {
    LOG_WARN("failed to assign task id", K(ret), K(tmp_task));
  }
  return ret;
}

}
}
