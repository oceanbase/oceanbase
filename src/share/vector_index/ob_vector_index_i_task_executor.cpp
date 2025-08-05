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
#define USING_LOG_PREFIX SHARE
#include "ob_vector_index_i_task_executor.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "storage/ls/ob_ls.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"

namespace oceanbase
{
using namespace storage;
namespace share
{
int ObVecITaskExecutor::init(const uint64_t tenant_id, storage::ObLS *ls)
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

int ObVecITaskExecutor::get_index_ls_mgr(ObPluginVectorIndexMgr *&index_ls_mgr)
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

// resume from __all_vector_index_task of filter tenant_id
int ObVecITaskExecutor::resume_task()
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

int ObVecITaskExecutor::start_task()
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
            } else if (OB_FAIL(task_handle.push_task(tenant_id_, ls_->get_ls_id(), task_ctx, task_opt.get_allocator()))) {
              LOG_WARN("fail to push task to thread pool", K(ret), K(tenant_id_), K(ls_->get_ls_id()), K(*task_ctx));
            } else if (FALSE_IT(task_ctx->in_thread_pool_ = true)) {
            } else if (OB_FAIL(update_status_and_ret_code(task_ctx))) {
              LOG_WARN("fail to update task status to inner table",
                K(ret), K(tenant_id_), K(ls_->get_ls_id()), K(*task_ctx));
            } else if (task_ctx->sys_task_id_.is_invalid() && OB_TMP_FAIL(ObVecIndexAsyncTaskUtil::add_sys_task(task_ctx))) {
              LOG_WARN("add sys task failed", K(tmp_ret));
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


int ObVecITaskExecutor::update_status_and_ret_code(ObVecIndexAsyncTaskCtx *task_ctx)
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

int ObVecITaskExecutor::clear_task_ctx(
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
  LOG_DEBUG("clear task ctx", K(task_ctx));
  return ret;
}

int ObVecITaskExecutor::clear_task_ctxs(
    ObVecIndexAsyncTaskOption &task_opt, const ObVecIndexTaskCtxArray &task_ctx_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < task_ctx_array.count(); ++i) {
    ObVecIndexAsyncTaskCtx *task_ctx = task_ctx_array.at(i);
    int tmp_ret = OB_SUCCESS;
    if (OB_ISNULL(task_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", K(ret), KP(task_ctx));
    } else if (OB_TMP_FAIL(ObVecIndexAsyncTaskUtil::remove_sys_task(task_ctx))) { // ignore sys task ret code
      LOG_WARN("remove sys task failed", K(tmp_ret));
    } else if (OB_FAIL(clear_task_ctx(task_opt, task_ctx))) {
      LOG_WARN("fail to clear task map", K(ret), K(task_ctx));
    }
  }
  return ret;
}


int ObVecITaskExecutor::check_task_result(ObVecIndexAsyncTaskCtx *task_ctx)
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
        task_ctx->task_status_.ret_code_ = VEC_ASYNC_TASK_DEFAULT_ERR_CODE; // reset ret_code
        LOG_WARN("vector index async task is finish and will do retry", KR(ret), KPC(task_ctx));
        // check task is canceled
        if (task_ctx->sys_task_id_.is_valid()) {
          bool is_cancel = false;
          if (OB_FAIL(SYS_TASK_STATUS_MGR.is_task_cancel(task_ctx->sys_task_id_, is_cancel))) {
            LOG_WARN("failed to check task is cancel", K(ret), K(task_ctx->sys_task_id_));
          } else if (is_cancel) {
            task_ctx->task_status_.ret_code_ = OB_CANCELED;
            task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH;
          }
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

int ObVecITaskExecutor::insert_new_task(ObVecIndexTaskCtxArray &task_ctx_array)
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

// alway return success
int ObVecITaskExecutor::clear_old_task_ctx_if_need()
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
    if (OB_SUCC(ret) && all_task_is_finish && (0 == task_opt.get_ls_processing_task_cnt())) {
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
        LOG_DEBUG("reset vector async task ctx memory", K(ret),
          K(index_ls_mgr->get_async_task_opt().get_allocator()),
          K(ls_->get_ls_id()), K(all_task_is_finish));
      }
    } else {
      LOG_DEBUG("not reset vector async task ctx memory",
        K(ret), K(all_task_is_finish), K(task_opt.get_ls_processing_task_cnt()));
    }
  }
  return OB_SUCCESS;
}

}
}
