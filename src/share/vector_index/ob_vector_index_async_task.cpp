/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SERVER
#include "ob_vector_index_async_task.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "src/storage/ls/ob_ls.h"

namespace oceanbase
{
namespace share
{

int ObVecAsyncTaskExecutor::load_triggered_task(const ObVecIndexTaskStatus &task_row)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector index load task not inited", KR(ret));
  } else if (OB_FAIL(get_index_ls_mgr(index_ls_mgr))) {
    LOG_WARN("fail to get index ls mgr", K(ret), K(tenant_id_), K(ls_handle_));
  } else if (OB_ISNULL(index_ls_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null index_ls_mgr", K(ret));
  } else {
    storage::ObLS *ls = ls_handle_.get_ls();
    ObVecIndexAsyncTaskOption &task_opt = index_ls_mgr->get_async_task_opt();
    // Transition STANDBY → PREPARE so that the task is picked up by task scheduling.
    ObVecIndexTaskStatus task_row_copy = task_row;
    task_row_copy.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE;
    if (OB_FAIL(ObVecIndexAsyncTaskUtil::try_add_task_ctx_from_inner_row(
            tenant_id_, task_row_copy, ls, ls_handle_, task_opt, true /*verify_tablet_on_ls*/))) {
      LOG_WARN("fail to add triggered task ctx", K(ret), K(task_row));
    } else {
      LOG_INFO("[VEC_ASYNC_TASK] triggered task loaded, STANDBY->PREPARE", K(ret), K(task_row));
    }
  }
  return ret;
}

bool ObVecAsyncTaskExecutor::check_operation_allow()
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
  } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::in_active_time(tenant_id_, is_active_time))) {
    bret = false;
    LOG_WARN("fail to get active time", KR(ret));
  } else if (!is_active_time) {
    bret = false;
    LOG_INFO("skip this round, not in active time.");
  }
  return bret;
}

int ObVecAsyncTaskExecutor::check_and_set_thread_pool()
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

int ObVecAsyncTaskExecutor::load_task(uint64_t &task_trace_base_num)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
  ObArray<ObVecIndexAsyncTaskCtx*> task_ctx_array;
  DEBUG_SYNC(LOAD_VECTOR_INDEX_ASYNC_TASK);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector async task not init", KR(ret));
  } else if (!check_operation_allow()) { // skip
  } else if (OB_FAIL(get_index_ls_mgr(index_ls_mgr))) { // skip
    LOG_WARN("fail to get index ls mgr", K(ret), K(tenant_id_), K(ls_handle_));
  } else {
    storage::ObLS *ls = ls_handle_.get_ls();
    ObVecIndexAsyncTaskOption &task_opt = index_ls_mgr->get_async_task_opt();
    ObIAllocator *allocator = task_opt.get_allocator();
    const int64_t current_task_cnt = ObVecIndexAsyncTaskUtil::get_processing_task_cnt(task_opt);

    common::hash::ObHashSet<uint64_t> conflict_table_id_set;
    common::hash::ObHashSet<uint64_t> conflict_index_task_set;
    bool ddl_conflict_checked = false;
    {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(conflict_table_id_set.create(16))) {
        LOG_INFO("fail to create conflict_table_id_set, skip DDL conflict check", K(tmp_ret));
      } else if (OB_TMP_FAIL(conflict_index_task_set.create(16))) {
        LOG_INFO("fail to create conflict_index_task_set, skip DDL conflict check", K(tmp_ret));
      } else if (OB_TMP_FAIL(check_has_hnsw_ddl(tenant_id_, conflict_table_id_set, conflict_index_task_set))) {
        LOG_INFO("fail to check hnsw ddl, skip DDL conflict check", K(tmp_ret));
      } else {
        ddl_conflict_checked = true;
      }
    }

    RWLock::RLockGuard lock_guard(index_ls_mgr->get_adapter_map_lock());
    FOREACH_X(iter, index_ls_mgr->get_complete_adapter_map(),
        OB_SUCC(ret) && (task_ctx_array.count() + current_task_cnt <= MAX_ASYNC_TASK_PROCESSING_COUNT)) {
      ObTabletID tablet_id = iter->first;
      ObPluginVectorIndexAdaptor *adapter = iter->second;
      if (OB_ISNULL(adapter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      } else if (OB_FALSE_IT(adapter->check_if_need_optimize())) {
      } else if (OB_SUCC(ret) && adapter->is_need_async_optimal()) {
        if (OB_NOT_NULL(scheduler_) && scheduler_->is_task_disabled(
                ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_INDEX_OPTINAL, tablet_id.id())) {
          LOG_TRACE("[VEC_IDX_TASK_DISABLED] skip generating disabled task",
                    K_(tenant_id), K(tablet_id), "task_type", "OPTIONAL");
          continue;
        }
        if (ddl_conflict_checked) {
          const uint64_t data_table_id = adapter->get_data_table_id();
          const uint64_t inc_table_id = adapter->get_inc_table_id();
          if (OB_HASH_EXIST == conflict_table_id_set.exist_refactored(data_table_id) ||
              OB_HASH_EXIST == conflict_index_task_set.exist_refactored(inc_table_id)) {
            LOG_TRACE("skip creating hnsw async task due to DDL conflict",
                      K_(tenant_id), K(tablet_id), K(data_table_id), K(inc_table_id));
            continue;
          }
        }
        int64_t index_table_id = OB_INVALID_ID;
        if (OB_FAIL(ObVecIndexAsyncTaskUtil::get_table_id_from_adapter(adapter, tablet_id, index_table_id))) { // only get table 3 table_id to generate new task
          LOG_WARN("fail to get table id from adapter", K(ret), K(tablet_id));
        } else if (OB_INVALID_ID == index_table_id) {
          LOG_DEBUG("index table id is invalid, skip", K(ret), K(tablet_id)); // skip to next
          continue;
        }
        int64_t new_task_id = OB_INVALID_ID;
        bool inc_new_task = false;
        bool task_ctx_in_map = false;
        common::ObCurTraceId::TraceId new_trace_id;

        char *task_ctx_buf = static_cast<char *>(allocator->alloc(sizeof(ObVecIndexAsyncTaskCtx)));
        ObVecIndexAsyncTaskCtx* task_ctx = nullptr;
        if (OB_ISNULL(task_ctx_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("async task ctx is null", K(ret));
        } else if (FALSE_IT(task_ctx = new(task_ctx_buf) ObVecIndexAsyncTaskCtx())) {
        } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::fetch_new_task_id(tenant_id_, new_task_id))) {
          LOG_WARN("fail to fetch new task id", K(ret), K(tenant_id_));
        } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::get_truncate_version(tenant_id_, index_table_id, task_ctx->truncate_version_))) {
          LOG_WARN("fail to get table truncate version", K(ret), K(index_table_id));
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::fetch_new_trace_id(++task_trace_base_num, allocator, new_trace_id))) {
          LOG_WARN("fail to fetch new trace id", K(ret), K(tablet_id));
        } else {
          LOG_INFO("[VEC_ASYNC_TASK] task loaded with PREPARE status",
                   K(ret), K(tablet_id), K(tenant_id_), K(new_trace_id), K(new_task_id),
                   K(task_trace_base_num), K(ls->get_ls_id()));
          // 1. update task_ctx to async task map
          task_ctx->tenant_id_ = tenant_id_;
          task_ctx->ls_handle_ = ls_handle_;
          task_ctx->task_status_.tablet_id_ = tablet_id.id();
          task_ctx->task_status_.tenant_id_ = tenant_id_;
          task_ctx->task_status_.table_id_ = index_table_id;
          task_ctx->task_status_.task_id_ = new_task_id;
          task_ctx->task_status_.task_type_ = ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_INDEX_OPTINAL;
          task_ctx->task_status_.trigger_type_ = ObVecIndexAsyncTaskTriggerType::OB_VEC_TRIGGER_AUTO;
          task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE;
          task_ctx->task_status_.exec_addr_ = GCTX.self_addr();
          task_ctx->task_status_.trace_id_ = new_trace_id;
          task_ctx->allocator_.set_tenant_id(tenant_id_);
          int64_t est_mem = 0;
          if (OB_FAIL(ObVecIndexAsyncTaskUtil::estimate_task_memory(task_ctx, est_mem, adapter))) {
            LOG_WARN("fail to estimate optinal task memory", K(ret), K(tablet_id));
          } else if (OB_FALSE_IT(task_ctx->task_status_.task_info_.task_estimate_memory_ = est_mem)) {
          } else if (OB_FAIL(index_ls_mgr->get_async_task_opt().add_task_ctx(tablet_id, task_ctx, inc_new_task))) { // not overwrite
            LOG_WARN("fail to add task ctx", K(ret));
          } else if (FALSE_IT(task_ctx_in_map = inc_new_task)) {
          } else if (inc_new_task && OB_FAIL(task_ctx_array.push_back(task_ctx))) {
            LOG_WARN("fail to push back task status", KR(ret), K(task_ctx));
          }
        }
        if (OB_NOT_NULL(task_ctx) && (!OB_SUCC(ret) || !inc_new_task)) {
          if (task_ctx_in_map) {
            int tmp_ret = ObVecIndexAsyncTaskUtil::clear_task_ctx(index_ls_mgr->get_async_task_opt(), task_ctx);
            if (OB_SUCCESS != tmp_ret) {
              LOG_WARN("fail to clear task ctx after load task failed", K(tmp_ret), K(ret), K(task_ctx));
            }
          } else {
            task_ctx->~ObVecIndexAsyncTaskCtx();
            allocator->free(task_ctx);
          }
          task_ctx = nullptr;
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(insert_new_task(task_ctx_array))) {
    LOG_WARN("fail to insert new tasks", K(ret), K(tenant_id_), K(ls_handle_));
  }
  // clear on fail
  if (OB_FAIL(ret) && !task_ctx_array.empty()) {
    if (OB_FAIL(ObVecIndexAsyncTaskUtil::clear_task_ctxs(index_ls_mgr->get_async_task_opt(), task_ctx_array))) {
      LOG_WARN("fail to clear task ctx", K(ret));
    }
  }
  return ret;
}

int ObVecTaskManager::process_task()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_task(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_STANDBY))) {
    LOG_WARN("fail to create task", K(ret));
  }
  while (OB_SUCC(ret) && !task_ids_.empty()) {
    if (OB_FAIL(check_task_status())) {
      LOG_WARN("fail to check task status", K(ret));
    } else {
      ob_usleep(1LL * 1000 * 1000);
    }

    if (REACH_TIME_INTERVAL(10 * 60L * 1000000)) {  // 10min
      LOG_INFO("vector index task not finished", K(ret), K(task_ids_));
    }
  }
  return ret;
}

int ObVecTaskManager::create_task(ObVecIndexAsyncTaskStatus initial_status)
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
        task_ctx->task_status_.status_ = initial_status;
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
