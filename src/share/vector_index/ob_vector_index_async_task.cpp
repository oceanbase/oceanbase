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

/*
  由于ObVecAsyncTaskScheduler从LOG_SERVICE中注册，以下所有逻辑将会被每个LS执行，若指定执行在LS_LEADER上，则需要通过is_leader_判断。
*/

int ObVecAsyncTaskExector::init(const uint64_t tenant_id, ObLS *ls)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  if (OB_ISNULL(vector_index_service) || OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant vector index load task fail", K(ret), KP(vector_index_service), KP(ls));
  } else {
    vector_index_service_ = vector_index_service;
    ls_ = ls;
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

// alway return success
int ObVecAsyncTaskExector::clear_old_task_ctx_if_need()
{
  int ret = OB_SUCCESS;
  bool all_task_is_finish = true;
  ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector async task not init", KR(ret));
  } else if (OB_FAIL(get_index_ls_mgr(index_ls_mgr))) { // skip
    LOG_WARN("fail to get index ls mgr", K(ret), K(tenant_id_), K(ls_->get_ls_id()));
  } else {
    ObVecIndexAsyncTaskOption &task_opt = index_ls_mgr->get_async_task_opt();
    FOREACH_X(iter, task_opt.get_async_task_map(), OB_SUCC(ret) && all_task_is_finish) {
      ObTabletID tablet_id = iter->first;
      ObVecIndexAsyncTaskCtx *task_ctx = iter->second;
      if (OB_ISNULL(task_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      } else if (!task_ctx->in_thread_pool_) {
        // current task is finished
      } else {
        all_task_is_finish = false; // break if has unfinish task
      }
    }
    if (OB_SUCC(ret) && all_task_is_finish) {
      // all tasks is finish and task record in map should be removed expectedly.
      // when map size > 0, is not expected.
      if (task_opt.get_async_task_map().size() > 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_INFO("unexpected vector async task map", K(ret),
          K(task_opt.get_async_task_map().size()));
        // for debug
        FOREACH_X(iter, task_opt.get_async_task_map(), OB_SUCC(ret)) {
          ObVecIndexAsyncTaskCtx *task_ctx = iter->second;
          if (OB_ISNULL(task_ctx)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected nullptr", K(ret));
          } else {
            LOG_WARN("print finished but is not been removed from map tasks", K(*task_ctx));
          }
        }
      } else {
        index_ls_mgr->get_async_task_opt().get_allocator()->reset();
        LOG_DEBUG("reset vector async task ctx memory", K(ret), K(all_task_is_finish));
      }
    }
  }
  return OB_SUCCESS;
}

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

int ObVecAsyncTaskExector::get_index_ls_mgr(ObPluginVectorIndexMgr *&index_ls_mgr)
{
  int ret = OB_SUCCESS;
  index_ls_mgr = nullptr;
  if (OB_ISNULL(ls_) || OB_ISNULL(vector_index_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(ls_), KP(vector_index_service_));
  } else if (OB_FAIL(vector_index_service_->acquire_vector_index_mgr(ls_->get_ls_id(), index_ls_mgr))) {
    LOG_WARN("fail to acquire vector index mgr", K(ret), K(ls_->get_ls_id()));
  } else if (OB_ISNULL(index_ls_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(index_ls_mgr));
  }
  return ret;
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
      } else if (OB_FAIL(thread_pool_handle.init(allocator))) {
        LOG_WARN("fail to init vec async task handle", K(ret), K(tenant_id_));
      } else if (OB_FAIL(thread_pool_handle.start())) {
        LOG_WARN("fail to start thread pool", K(ret), K(tenant_id_));
      }
    }
  }
  return ret;
}

int ObVecAsyncTaskExector::clear_task_ctxs(
    ObVecIndexAsyncTaskOption &task_opt, const ObVecIndexTaskCtxArray &task_ctx_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < task_ctx_array.count(); ++i) {
    ObVecIndexAsyncTaskCtx *task_ctx = task_ctx_array.at(i);
    int tmp_ret = OB_SUCCESS;
    if (OB_ISNULL(task_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", K(ret), KP(task_ctx));
    } else if (OB_FAIL(clear_task_ctx(task_opt, task_ctx))) {
      LOG_WARN("fail to clear task map", K(ret), K(task_ctx));
    } else if (OB_TMP_FAIL(ObVecIndexAsyncTaskUtil::remove_sys_task(task_ctx))) { // ignore sys task ret code
      LOG_WARN("remove sys task failed", K(tmp_ret));
    }
  }
  return ret;
}

int ObVecAsyncTaskExector::clear_task_ctx(
    ObVecIndexAsyncTaskOption &task_opt, ObVecIndexAsyncTaskCtx *task_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector index load task not inited", K(ret));
  } else if (OB_ISNULL(task_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task ctx", K(ret), KP(task_ctx));
  } else {
    ObTabletID tablet_id(task_ctx->task_status_.tablet_id_);
    if (OB_FAIL(task_opt.del_task_ctx(tablet_id))) {
      if (ret != OB_ENTRY_NOT_EXIST) {
        LOG_WARN("fail to delete task from task map", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  if (OB_NOT_NULL(task_ctx)) {
    task_ctx->~ObVecIndexAsyncTaskCtx();
    task_opt.get_allocator()->free(task_ctx); // arena need free ??
    task_ctx = nullptr;
  }
  return ret;
}

int ObVecAsyncTaskExector::insert_new_task(ObVecIndexTaskCtxArray &task_ctx_array)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector index load task not inited", K(ret));
  } else if (task_ctx_array.count() <= 0) {  // skip empty array
  } else {
    ObMySQLTransaction trans;
    if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
      LOG_WARN("fail start transaction", K(ret), K(tenant_id_));
    } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::batch_insert_vec_task(
        tenant_id_, OB_ALL_VECTOR_INDEX_TASK_TNAME, trans, task_ctx_array))) {
      LOG_WARN("fail to insert vec tasks", K(ret), K(tenant_id_), K(ls_->get_ls_id()));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("fail to commit trans", KR(ret), K(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObVecAsyncTaskExector::update_status_and_ret_code(ObVecIndexAsyncTaskCtx *task_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector index load task not inited", K(ret));
  } else if (OB_ISNULL(task_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task ctx", K(ret), KP(task_ctx));
  } else {
    ObVecIndexTaskKey key(task_ctx->task_status_.tenant_id_,
                          task_ctx->task_status_.table_id_,
                          task_ctx->task_status_.tablet_id_.id(),
                          task_ctx->task_status_.task_id_);

    ObVecIndexFieldArray update_fields;
    ObVecIndexTaskStatusField task_status;
    ObVecIndexTaskStatusField ret_code;
    ObVecIndexTaskStatusField target_scn;

    task_status.field_name_ = "status";
    task_status.data_.uint_ = task_ctx->task_status_.status_;
    ret_code.field_name_ = "ret_code";
    ret_code.data_.uint_ = task_ctx->task_status_.ret_code_;
    target_scn.field_name_ = "target_scn";
    target_scn.data_.int_ = task_ctx->task_status_.target_scn_.convert_to_ts();

    if (OB_FAIL(update_fields.push_back(task_status))) {
      LOG_WARN("fail to push back update field", K(ret), K(task_status));
    } else if (OB_FAIL(update_fields.push_back(ret_code))) {
      LOG_WARN("fail to push back update field", K(ret), K(ret_code));
    } else if (OB_FAIL(update_fields.push_back(target_scn))) {
      LOG_WARN("fail to push back update field", K(ret), K(target_scn));
    } else {
      ObMySQLTransaction trans;
      if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
        LOG_WARN("fail start transaction", K(ret), K(tenant_id_));
      } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::update_vec_task(
          tenant_id_, OB_ALL_VECTOR_INDEX_TASK_TNAME, trans, key, update_fields))) {
        LOG_WARN("fail to update task status", K(ret));
      } else {
        LOG_DEBUG("success to update_status_and_ret_code", KPC(task_ctx));
      }
      if (trans.is_started()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("fail to commit trans", KR(ret), K(tmp_ret));
          ret = OB_SUCC(ret) ? tmp_ret : ret;
        }
      }
    }
  }
  return ret;
}

int ObVecAsyncTaskExector::check_task_result(ObVecIndexAsyncTaskCtx *task_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector index load task not inited", K(ret));
  } else if (OB_ISNULL(task_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task ctx", K(ret), KP(task_ctx));
  } else {
    LOG_DEBUG("ObVecAsyncTaskExector::check_task_result", K(task_ctx->task_status_));
    common::ObSpinLockGuard ctx_guard(task_ctx->lock_);
    if (task_ctx->task_status_.status_ == ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_RUNNING) {
      LOG_WARN("ObVecAsyncTaskExector::check_task_result status=running", KPC(task_ctx));
      if (task_ctx->task_status_.ret_code_ == OB_SUCCESS) {
        task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH;
        LOG_WARN("vector index async task is finish", K(ret), KPC(task_ctx));
      } else if (task_ctx->task_status_.ret_code_ == VEC_ASYNC_TASK_DEFAULT_ERR_CODE) { // skip default code
        LOG_WARN("vector index async task not finish", K(ret), KPC(task_ctx));
      } else if (!ObIDDLTask::in_ddl_retry_white_list(task_ctx->task_status_.ret_code_)) {
        task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH;
        LOG_WARN("vector index async task is finish with failed", KR(ret), KPC(task_ctx));
      } else if (++task_ctx->retry_time_ > VEC_INDEX_TASK_MAX_RETRY_TIME) { // retry
        task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH;
        LOG_WARN("vector index async task is finish and not retry anymore", KR(ret), KPC(task_ctx));
      } else {
        task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE;
        LOG_WARN("vector index async task is finish and will do retry", KR(ret), KPC(task_ctx));
        // check task is canceled
        bool is_cancel = false;
        if (OB_FAIL(ObVecIndexAsyncTaskUtil::check_task_is_cancel(task_ctx, is_cancel))) {
          LOG_WARN("failed to check task is cancel", K(ret), K(task_ctx->sys_task_id_));
        } else if (is_cancel) {
          task_ctx->task_status_.ret_code_ = OB_CANCELED;
          task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH;
        }
      }
      // task need retry or go to end
      if (OB_SUCC(ret) &&
         (task_ctx->task_status_.status_ == ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE ||
          task_ctx->task_status_.status_ == ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH)) {
        task_ctx->in_thread_pool_ = false; // clear old task flag
      }
    } else {
      // do nothing
    }

  }
  return ret;
}

// resume from __all_vector_index_task of filter tenant_id
int ObVecAsyncTaskExector::resume_task()
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector index load task not inited", KR(ret));
  } else if (!check_operation_allow()) { // skip
  } else if (OB_FAIL(get_index_ls_mgr(index_ls_mgr))) { // skip
    LOG_WARN("fail to get index ls mgr", K(ret), K(tenant_id_), K(ls_->get_ls_id()));
  } else {
    const bool for_update = true; // select for update
    ObVecIndexAsyncTaskOption &task_opt = index_ls_mgr->get_async_task_opt();
    ObVecIndexFieldArray filters;
    ObVecIndexTaskStatusField field;
    field.field_name_ = "tenant_id";
    field.data_.uint_ = tenant_id_;

    if (OB_FAIL(filters.push_back(field))) {
      LOG_WARN("fail to push back field", K(ret));
    } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::resume_task_from_inner_table(
        tenant_id_, OB_ALL_VECTOR_INDEX_TASK_TNAME, for_update, filters, ls_,  *GCTX.sql_proxy_, task_opt))) {
      LOG_WARN("fail to resume task from inner table", K(ret), K(tenant_id_));
    }
  }
  return ret;
}

int ObVecAsyncTaskExector::start_task()
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector async task not init", K(ret));
  } else if (!check_operation_allow()) { // skip
  } else if (OB_ISNULL(vector_index_service_) || OB_ISNULL(ls_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(vector_index_service_), KP(ls_));
  } else if (OB_FAIL(get_index_ls_mgr(index_ls_mgr))) {
    LOG_WARN("fail to get index ls mgr", K(ret), K(tenant_id_), K(ls_->get_ls_id()));
  } else {
    ObVecIndexTaskCtxArray task_ctx_array;
    ObVecIndexAsyncTaskOption &task_opt = index_ls_mgr->get_async_task_opt();
    FOREACH_X(iter, task_opt.get_async_task_map(), OB_SUCC(ret)) {
      ObTabletID tablet_id = iter->first;
      ObVecIndexAsyncTaskCtx *task_ctx = iter->second;
      if (OB_ISNULL(task_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      } else if (OB_FAIL(check_task_result(task_ctx))) {
        LOG_WARN("fail to check task result", K(ret), KPC(task_ctx));
      } else {
        switch (task_ctx->task_status_.status_) {
          case ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE:
          {
            // lock ctx to change task status
            common::ObSpinLockGuard ctx_guard(task_ctx->lock_);
            ObVecIndexAsyncTaskHandler &task_handle = vector_index_service_->get_vec_async_task_handle();
            int tmp_ret = OB_SUCCESS;
            if (task_ctx->in_thread_pool_) {                // skip push task
              LOG_DEBUG("task is in thread pool already", KPC(task_ctx));
            } else if (OB_FAIL(task_handle.push_task(tenant_id_, ls_->get_ls_id(), task_ctx))) {
              LOG_WARN("fail to push task to thread pool", K(ret), K(tenant_id_), K(ls_->get_ls_id()), K(*task_ctx));
            } else if (OB_FAIL(update_status_and_ret_code(task_ctx))) {
              LOG_WARN("fail to update task status to inner table",
                K(ret), K(tenant_id_), K(ls_->get_ls_id()), K(*task_ctx));
            } else if (task_ctx->sys_task_id_.is_invalid() && OB_TMP_FAIL(ObVecIndexAsyncTaskUtil::add_sys_task(task_ctx))) {
              LOG_WARN("add sys task failed", K(tmp_ret));
            } else if (FALSE_IT(task_ctx->in_thread_pool_ = true)) {
            }
            break;
          }
          case ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_RUNNING:
          {
            if (OB_FAIL(update_status_and_ret_code(task_ctx))) {
              LOG_WARN("fail to update task status to inner table",
                K(ret), K(tenant_id_), K(ls_->get_ls_id()), K(*task_ctx));
            }
            break;
          }
          case ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH:
          {
            // update task status in inner table
            if (OB_FAIL(update_status_and_ret_code(task_ctx))) {
              LOG_WARN("fail to update task status to inner table",
                K(ret), K(tenant_id_), K(ls_->get_ls_id()), K(*task_ctx));
            } else if (OB_FAIL(task_ctx_array.push_back(task_ctx))) {
              LOG_WARN("fail to push back task_ctx_array", K(ret), K(task_ctx));
            }
            break;
          }
          default :
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected task status", K(ret), K(task_ctx->task_status_));
            break;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(clear_task_ctxs(task_opt, task_ctx_array))) {  // iter map and clean / add map is not allow in same foreach
      LOG_WARN("fail to clean map", K(ret), K(task_ctx_array));
    }
  }
  return ret;
}

int ObVecAsyncTaskExector::load_task()
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
  ObArray<ObVecIndexAsyncTaskCtx*> task_ctx_array;
  uint64_t trace_base_num = 0;
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
        } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::fetch_new_trace_id(++trace_base_num, allocator, new_trace_id))) {
          LOG_WARN("fail to fetch new trace id", K(ret), K(tablet_id));
        } else {
          LOG_DEBUG("start load task", K(ret), K(tablet_id), K(tenant_id_), K(ls_->get_ls_id()));
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
          task_ctx->task_status_.target_scn_.convert_from_ts(ObTimeUtility::current_time());
          if (OB_FAIL(index_ls_mgr->get_async_task_opt().add_task_ctx(tablet_id, task_ctx, inc_new_task))) { // not overwrite
            LOG_WARN("fail to add task ctx", K(ret));
          } else if (inc_new_task && OB_FAIL(task_ctx_array.push_back(task_ctx))) {
            LOG_WARN("fail to push back task status", K(ret), K(task_ctx));
          }
        }
        if (OB_FAIL(ret)) { // release memory when fail
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



}
}
