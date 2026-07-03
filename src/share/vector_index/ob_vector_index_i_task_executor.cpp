/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SHARE
#include "ob_vector_index_i_task_executor.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "storage/ls/ob_ls.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/ob_ddl_common.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/scheduler/ob_sys_task_stat.h"
#include "share/ob_ddl_task_executor.h"
#include "share/ob_debug_sync.h"
#include "rootserver/ddl_task/ob_ddl_task.h"
#include "share/vector_index/ob_plugin_vector_index_adaptor.h"

namespace oceanbase
{
using namespace storage;
namespace share
{
int ObVecITaskExecutor::init(const uint64_t tenant_id, storage::ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  if (OB_ISNULL(vector_index_service) || OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant vector index load task fail", K(ret), KP(vector_index_service), K(ls_handle));
  } else {
    vector_index_service_ = vector_index_service;
    scheduler_ = &vector_index_service_->get_vec_idx_async_task_sched();
    ls_handle_ = ls_handle;
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObVecITaskExecutor::get_index_ls_mgr(ObPluginVectorIndexMgr *&index_ls_mgr)
{
  int ret = OB_SUCCESS;
  storage::ObLS *ls = ls_handle_.get_ls();
  index_ls_mgr = nullptr;
  if (OB_ISNULL(ls) || OB_ISNULL(vector_index_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(ls_handle_), KP(vector_index_service_));
  } else if (OB_FAIL(vector_index_service_->acquire_vector_index_mgr(ls->get_ls_id(), index_ls_mgr))) {
    LOG_WARN("fail to acquire vector index mgr", K(ret), K(ls->get_ls_id()));
  } else if (OB_ISNULL(index_ls_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(index_ls_mgr));
  }
  return ret;
}

// R1 sweep: push all non-FINISH rows belonging to this LS that were last executed by
// self_addr to FINISH+CANCELED in the inner table. Does not reconstruct ctx in memory —
// any ctx that was alive on this observer has already been cancelled by the role switch
// path before resume_task is called.
int ObVecITaskExecutor::resume_task()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector index load task not inited", KR(ret));
  } else if (!check_operation_allow()) { // skip
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::sweep_self_residual_for_ls(
                 tenant_id_, OB_ALL_VECTOR_INDEX_TASK_TNAME, ls_handle_,
                 GCTX.self_addr(), *GCTX.sql_proxy_))) {
    LOG_WARN("fail to sweep self residual for ls", K(ret), K(tenant_id_), K(ls_handle_));
  }
  return ret;
}

int ObVecITaskExecutor::check_has_hnsw_ddl(
    const uint64_t tenant_id,
    common::hash::ObHashSet<uint64_t> &conflict_table_id_set,
    common::hash::ObHashSet<uint64_t> &conflict_index_task_set)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  share::schema::ObSchemaGetterGuard schema_guard;

  if (OB_FAIL(share::schema::ObMultiVersionSchemaService::get_instance()
                  .get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt(
                 "SELECT object_id, target_object_id, ddl_type FROM %s WHERE ddl_type IN (%d, %d, %d)",
                 OB_ALL_DDL_TASK_STATUS_TNAME,
                 static_cast<int>(DDL_DROP_VEC_INDEX),
                 static_cast<int>(DDL_CREATE_VEC_INDEX),
                 static_cast<int>(DDL_REBUILD_INDEX)))) {
    LOG_WARN("fail to assign sql", K(ret));
  } else {
    LOG_DEBUG("check_has_hnsw_ddl", K(sql), K(tenant_id));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      static const int64_t DDL_CONFLICT_CHECK_TIMEOUT_US = 5L * 1000L * 1000L; // 5s
      common::sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(GCTX.sql_proxy_->read(res, tenant_id, sql.ptr(), nullptr, DDL_CONFLICT_CHECK_TIMEOUT_US))) {
        LOG_WARN("fail to read ddl task status", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret));
      } else {
        LOG_DEBUG("check_has_hnsw_ddl result", K(result->get_column_count()));
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          uint64_t object_id = OB_INVALID_ID;
          uint64_t target_object_id = OB_INVALID_ID;
          int64_t ddl_type = static_cast<int64_t>(DDL_INVALID);
          EXTRACT_INT_FIELD_MYSQL(*result, "object_id", object_id, uint64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "target_object_id", target_object_id, uint64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "ddl_type", ddl_type, int64_t);
          if (OB_SUCC(ret)) {
            if (static_cast<ObDDLType>(ddl_type) == DDL_REBUILD_INDEX) {
              if (OB_FAIL(conflict_index_task_set.set_refactored(target_object_id, 0))) {
                if (OB_HASH_EXIST == ret) {
                  ret = OB_SUCCESS;
                } else {
                  LOG_WARN("fail to insert rebuild target to conflict_index_task_set",
                           K(ret), K(target_object_id), K(ddl_type));
                }
              }
            } else {
              const share::schema::ObSimpleTableSchemaV2 *simple_schema = nullptr;
              int tmp_ret = OB_SUCCESS;
              if (OB_TMP_FAIL(schema_guard.get_simple_table_schema(tenant_id, object_id, simple_schema))) {
                LOG_WARN("fail to get simple table schema, skip this ddl record",
                        K(tmp_ret), K(object_id));
              } else if (OB_ISNULL(simple_schema)) {
              } else if (simple_schema->is_user_hidden_table()) {
                const uint64_t original_table_id = simple_schema->get_association_table_id();
                if (OB_FAIL(conflict_table_id_set.set_refactored(original_table_id, 0))) {
                  if (OB_HASH_EXIST == ret) {
                    ret = OB_SUCCESS;
                  } else {
                    LOG_WARN("fail to insert to conflict_table_id_set", K(ret), K(original_table_id));
                  }
                }
              } else {
                if (OB_FAIL(conflict_index_task_set.set_refactored(target_object_id, 0))) {
                  if (OB_HASH_EXIST == ret) {
                    ret = OB_SUCCESS;
                  } else {
                    LOG_WARN("fail to insert to conflict_index_task_set", K(ret), K(target_object_id));
                  }
                }
              }
            }
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  LOG_DEBUG("check_has_hnsw_ddl done", K(ret),
            "conflict_table_cnt", conflict_table_id_set.size(),
            "conflict_index_cnt", conflict_index_task_set.size());
  return ret;
}

int ObVecITaskExecutor::check_task_ddl_conflict(
    ObVecIndexAsyncTaskCtx *task_ctx,
    ObPluginVectorIndexMgr *index_ls_mgr,
    const common::hash::ObHashSet<uint64_t> &conflict_table_id_set,
    const common::hash::ObHashSet<uint64_t> &conflict_index_task_set,
    bool &is_conflict)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task_ctx) || OB_ISNULL(index_ls_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(task_ctx), KP(index_ls_mgr));
  } else if (task_ctx->task_status_.trigger_type_ != ObVecIndexAsyncTaskTriggerType::OB_VEC_TRIGGER_AUTO ||
            task_ctx->task_status_.task_type_ != ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_INDEX_OPTINAL) {
  } else {
    const uint64_t inc_table_id = task_ctx->task_status_.table_id_;
    LOG_DEBUG("check_task_ddl_conflict", K(inc_table_id), K(conflict_index_task_set.size()));
    if (OB_HASH_EXIST == conflict_index_task_set.exist_refactored(inc_table_id)) {
      is_conflict = true;
    }
    if (OB_SUCC(ret) && !is_conflict) {
      ObPluginVectorIndexAdapterGuard adpt_guard;
      ObTabletID tablet_id(task_ctx->task_status_.tablet_id_);
      if (OB_FAIL(index_ls_mgr->get_adapter_inst_guard(tablet_id, adpt_guard))) {
        if (OB_HASH_NOT_EXIST == ret) {
          is_conflict = true;
          LOG_TRACE("adapter not exist, treat as conflict", K(tablet_id), KPC(task_ctx));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get adapter guard, skip table-level conflict check",
                   K(ret), K(tablet_id));
        }
      } else if (OB_NOT_NULL(adpt_guard.get_adatper())) {
        const uint64_t data_table_id = adpt_guard.get_adatper()->get_data_table_id();
        LOG_DEBUG("check_task_ddl_conflict", K(data_table_id), K(conflict_table_id_set.size()));
        if (OB_HASH_EXIST == conflict_table_id_set.exist_refactored(data_table_id)) {
          is_conflict = true;
        }
      }
    }
  }
  LOG_DEBUG("check_task_ddl_conflict done", K(ret), K(is_conflict));
  return ret;
}

int ObVecITaskExecutor::start_task()
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
  storage::ObLS *ls = ls_handle_.get_ls();
  DEBUG_SYNC(START_VECTOR_INDEX_ASYNC_TASK);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector async task not init", K(ret));
  } else if (!check_operation_allow()) { // skip
  } else if (OB_ISNULL(vector_index_service_) || OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(vector_index_service_), KP(ls));
  } else if (OB_FAIL(get_index_ls_mgr(index_ls_mgr))) {
    LOG_WARN("fail to get index ls mgr", K(ret), K(tenant_id_), K(ls->get_ls_id()));
  } else {
    ObVecIndexTaskCtxArray task_ctx_array;
    ObVecIndexAsyncTaskOption &task_opt = index_ls_mgr->get_async_task_opt();

    common::hash::ObHashSet<uint64_t> conflict_table_id_set;
    common::hash::ObHashSet<uint64_t> conflict_index_task_set;
    bool ddl_conflict_checked = false;
    if (task_opt.get_async_task_map().size() > 0) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(conflict_table_id_set.create(16))) {
        LOG_WARN("fail to create conflict_table_id_set, skip DDL conflict check", K(tmp_ret));
      } else if (OB_TMP_FAIL(conflict_index_task_set.create(16))) {
        LOG_WARN("fail to create conflict_index_task_set, skip DDL conflict check", K(tmp_ret));
      } else if (OB_TMP_FAIL(check_has_hnsw_ddl(tenant_id_, conflict_table_id_set, conflict_index_task_set))) {
        LOG_WARN("fail to check hnsw ddl, skip DDL conflict check", K(tmp_ret));
      } else {
        ddl_conflict_checked = true;
      }
    }

    FOREACH_X(iter, task_opt.get_async_task_map(), OB_SUCC(ret)) {
      ObTabletID tablet_id = iter->first.tablet_id_;
      ObVecIndexAsyncTaskCtx *task_ctx = iter->second;
      if (OB_ISNULL(task_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      } else if (OB_FAIL(check_task_result(task_ctx))) {
        LOG_WARN("fail to check task result", K(ret), KPC(task_ctx));
      } else {
        bool is_conflict = false;
        if (ddl_conflict_checked) {
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(check_task_ddl_conflict(task_ctx, index_ls_mgr,
                                                   conflict_table_id_set,
                                                   conflict_index_task_set,
                                                   is_conflict))) {
            LOG_WARN("fail to check task ddl conflict, treat as no conflict",
                     K(tmp_ret), KPC(task_ctx));
          }
        }
        switch (task_ctx->task_status_.status_) {
          case ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE:
          {
            ObVecIndexAsyncTaskHandler &task_handle = vector_index_service_->get_vec_async_task_handle();
            int tmp_ret = OB_SUCCESS;
            if (is_conflict) {
              {
                common::ObSpinLockGuard ctx_guard(task_ctx->lock_);
                task_ctx->task_status_.status_   = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH;
                task_ctx->task_status_.ret_code_ = OB_CANCELED;
                LOG_INFO("cancel hnsw async PREPARE task due to DDL conflict", KPC(task_ctx));
              }
              if (OB_FAIL(ObVecIndexAsyncTaskUtil::update_status_and_ret_code(task_ctx))) {
                LOG_WARN("fail to update status for conflicted PREPARE task",
                         K(ret), K(tenant_id_), K(ls->get_ls_id()), KPC(task_ctx));
              } else if (OB_FAIL(task_ctx_array.push_back(task_ctx))) {
                LOG_WARN("fail to push back task_ctx_array", K(ret), K(task_ctx));
              }
            } else {
              bool need_update = false;
              {
                common::ObSpinLockGuard ctx_guard(task_ctx->lock_);
                if (task_ctx->in_thread_pool_) {
                  LOG_DEBUG("task is in thread pool already", KPC(task_ctx));
                } else if (OB_FAIL(task_handle.push_task(tenant_id_, ls->get_ls_id(), task_ctx, task_opt.get_allocator()))) {
                  LOG_WARN("fail to push task to thread pool", K(ret), K(tenant_id_), K(ls->get_ls_id()), K(*task_ctx));
                } else {
                  task_ctx->in_thread_pool_ = true;
                  need_update = true;
                }
              }
              if (OB_SUCC(ret) && need_update) {
                if (OB_FAIL(ObVecIndexAsyncTaskUtil::update_status_and_ret_code(task_ctx))) {
                  LOG_WARN("fail to update task status to inner table",
                    K(ret), K(tenant_id_), K(ls->get_ls_id()), K(*task_ctx));
                } else if (task_ctx->sys_task_id_.is_invalid() && OB_TMP_FAIL(ObVecIndexAsyncTaskUtil::add_sys_task(task_ctx))) {
                  LOG_WARN("add sys task failed", K(tmp_ret));
                }
              }
            }
            break;
          }
          case ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_RUNNING:
          {
            int tmp_ret = OB_SUCCESS;
            if (is_conflict) {
              if (OB_FAIL(task_ctx->cancel_task())) {
                LOG_WARN("fail to cancel conflicted vector async task", K(ret), KPC(task_ctx));
              } else if (task_ctx->in_thread_pool_ && task_ctx->sys_task_id_.is_valid()
                         && OB_TMP_FAIL(SYS_TASK_STATUS_MGR.cancel_task(task_ctx->sys_task_id_))) {
                LOG_WARN("fail to cancel conflicted vector async task in sys task mgr", K(tmp_ret), KPC(task_ctx));
              } else {
                LOG_INFO("cancel hnsw async running-stage task due to DDL conflict", KPC(task_ctx));
              }
              if (OB_FAIL(ObVecIndexAsyncTaskUtil::update_status_and_ret_code(task_ctx))) {
                LOG_WARN("fail to update status for conflicted running-stage task",
                         K(ret), K(tenant_id_), K(ls->get_ls_id()), KPC(task_ctx));
              }
            } else {
              ObVecIndexAsyncTaskHandler &task_handle = vector_index_service_->get_vec_async_task_handle();
              bool need_update = false;
              {
                common::ObSpinLockGuard ctx_guard(task_ctx->lock_);
                if (task_ctx->in_thread_pool_) {
                  LOG_DEBUG("task is in thread pool already", KPC(task_ctx));
                } else if (OB_FAIL(task_handle.push_task(tenant_id_, ls->get_ls_id(), task_ctx, task_opt.get_allocator()))) {
                  LOG_WARN("fail to push task to thread pool", K(ret), K(tenant_id_), K(ls->get_ls_id()), K(*task_ctx));
                } else {
                  task_ctx->in_thread_pool_ = true;
                  need_update = true;
                }
              }
              if (OB_SUCC(ret) && need_update) {
                if (OB_FAIL(ObVecIndexAsyncTaskUtil::update_status_and_ret_code(task_ctx))) {
                  LOG_WARN("fail to update task status to inner table",
                           K(ret), K(tenant_id_), K(ls->get_ls_id()), K(*task_ctx));
                } else if (task_ctx->sys_task_id_.is_invalid() && OB_TMP_FAIL(ObVecIndexAsyncTaskUtil::add_sys_task(task_ctx))) {
                  LOG_WARN("add sys task failed", K(tmp_ret));
                }
              }
            }
            break;
          }
          case ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_EXCHANGE:
          case ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CLEAN:
          {
            ObVecIndexAsyncTaskHandler &task_handle = vector_index_service_->get_vec_async_task_handle();
            int tmp_ret = OB_SUCCESS;
            bool need_update = false;
            {
              common::ObSpinLockGuard ctx_guard(task_ctx->lock_);
              if (task_ctx->in_thread_pool_) {
                LOG_DEBUG("task is in thread pool already", KPC(task_ctx));
              } else if (OB_FAIL(task_handle.push_task(tenant_id_, ls->get_ls_id(), task_ctx, task_opt.get_allocator()))) {
                LOG_WARN("fail to push task to thread pool", K(ret), K(tenant_id_), K(ls->get_ls_id()), K(*task_ctx));
              } else {
                task_ctx->in_thread_pool_ = true;
                need_update = true;
              }
            }
            if (OB_SUCC(ret) && need_update) {
              if (OB_FAIL(ObVecIndexAsyncTaskUtil::update_status_and_ret_code(task_ctx))) {
                LOG_WARN("fail to update task status to inner table",
                  K(ret), K(tenant_id_), K(ls->get_ls_id()), K(*task_ctx));
              } else if (task_ctx->sys_task_id_.is_invalid() && OB_TMP_FAIL(ObVecIndexAsyncTaskUtil::add_sys_task(task_ctx))) {
                LOG_WARN("add sys task failed", K(tmp_ret));
              }
            }
            break;
          }
          case ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CANCEL:
          {
            if (OB_FAIL(ObVecIndexAsyncTaskUtil::update_status_and_ret_code(task_ctx))) {
              LOG_WARN("fail to update task status to inner table",
                       K(ret), K(tenant_id_), K(ls->get_ls_id()), K(*task_ctx));
            }
            break;
          }
          case ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH:
          {
            int tmp_ret = OB_SUCCESS;
            if (OB_TMP_FAIL(ObVecIndexAsyncTaskUtil::update_status_and_ret_code(task_ctx))) {
              LOG_WARN("fail to update task status to inner table",
                K(tmp_ret), K(tenant_id_), K(ls->get_ls_id()), K(*task_ctx));
            }
            if (OB_FAIL(task_ctx_array.push_back(task_ctx))) {
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
    common::ObSpinLockGuard guard(index_ls_mgr->task_ctx_lock_);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(clear_task_ctxs(task_opt, task_ctx_array))) {
      LOG_WARN("fail to clean map", K(ret), K(task_ctx_array));
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
    if (OB_FAIL(task_opt.del_task_ctx(tablet_id, task_ctx->task_status_.task_type_))) {
      if (ret != OB_ENTRY_NOT_EXIST) {
        LOG_WARN("fail to delete task from task map", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  if (OB_NOT_NULL(task_ctx)) {
    task_ctx->~ObVecIndexAsyncTaskCtx();
    task_opt.get_allocator()->free(task_ctx);
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
    } else if (OB_TMP_FAIL(ObVecIndexAsyncTaskUtil::remove_sys_task(task_ctx))) {
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
    LOG_DEBUG("ObVecITaskExecutor::check_task_result", K(task_ctx->task_status_));
    common::ObSpinLockGuard ctx_guard(task_ctx->lock_);
    if (task_ctx->task_status_.status_ == ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_RUNNING ||
        task_ctx->task_status_.status_ == ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_EXCHANGE ||
        task_ctx->task_status_.status_ == ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CLEAN) {
      LOG_DEBUG("start check vec async task result", KPC(task_ctx));
      if (task_ctx->task_status_.ret_code_ == OB_SUCCESS) {
        task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH;
        LOG_WARN("vector index async task is finish", K(ret), KPC(task_ctx));
      } else if (task_ctx->task_status_.ret_code_ == OB_CANCELED) {
        task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH;
        LOG_INFO("vector index async task is canceled and finish", K(ret), KPC(task_ctx));
      } else if (task_ctx->task_status_.ret_code_ == VEC_ASYNC_TASK_DEFAULT_ERR_CODE) {
        LOG_WARN("vector index async task not finish", K(ret), KPC(task_ctx));
      } else if (!ObIDDLTask::in_ddl_retry_white_list(task_ctx->task_status_.ret_code_)) {
        task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH;
        LOG_WARN("vector index async task is finish with failed", KR(ret), KPC(task_ctx));
      } else if (++task_ctx->retry_time_ > ObVecIndexAsyncTaskUtil::VEC_INDEX_TASK_MAX_RETRY_TIME) {
        task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH;
        LOG_WARN("vector index async task is finish and not retry anymore", KR(ret), KPC(task_ctx));
      } else {
        task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE;
        task_ctx->task_status_.last_error_code_ = task_ctx->task_status_.ret_code_;
        task_ctx->task_status_.ret_code_ = VEC_ASYNC_TASK_DEFAULT_ERR_CODE;
        LOG_INFO("vector index async task is finish and will do retry", KR(ret), KPC(task_ctx));
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
      if (task_ctx->task_status_.ret_code_ == OB_SUCCESS && task_ctx->task_status_.task_type_ == OB_VECTOR_ASYNC_HYBRID_VECTOR_EMBEDDING && !task_ctx->task_status_.all_finished_) {
        task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE;
        task_ctx->task_status_.last_error_code_ = task_ctx->task_status_.ret_code_;
        task_ctx->task_status_.ret_code_ = VEC_ASYNC_TASK_DEFAULT_ERR_CODE;
      }
      if (OB_SUCC(ret) &&
         (task_ctx->task_status_.status_ == ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE ||
          task_ctx->task_status_.status_ == ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH)) {
        task_ctx->task_status_.all_finished_ = (task_ctx->task_status_.status_ == ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH);
        task_ctx->in_thread_pool_ = false;
      }
    }
  }
  return ret;
}

int ObVecITaskExecutor::insert_new_task(ObVecIndexTaskCtxArray &task_ctx_array)
{
  int ret = OB_SUCCESS;
  common::hash::ObHashSet<ObVecIndexAsyncTaskKey> duplicate_tablet_task;
  ObVecIndexTaskCtxArray new_task_ctx_array;
  ObVecIndexTaskCtxArray skipped_task_ctx_array;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector index load task not inited", K(ret));
  } else if (task_ctx_array.count() <= 0) {  // skip empty array
  } else if (OB_FAIL(duplicate_tablet_task.create(MAX_ASYNC_TASK_PROCESSING_COUNT))) {
    LOG_WARN("fail to create duplicate tablet task set", K(ret));
  } else {
    ObMySQLTransaction trans;
    if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
      LOG_WARN("fail start transaction", K(ret), K(tenant_id_));
    } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::get_duplicate_tablet_vec_task(
        tenant_id_, OB_ALL_VECTOR_INDEX_TASK_TNAME, trans, duplicate_tablet_task))) {
      LOG_WARN("fail to get duplicate tablet vec set", K(ret));
    } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::get_insert_task_ctx_array(
        task_ctx_array, new_task_ctx_array, duplicate_tablet_task))) {
      LOG_WARN("fail to get insert task ctx array", K(ret));
    } else if (duplicate_tablet_task.size() > 0) {
      for (int64_t i = 0; OB_SUCC(ret) && i < task_ctx_array.count(); ++i) {
        ObVecIndexAsyncTaskCtx *task_ctx = task_ctx_array.at(i);
        if (OB_ISNULL(task_ctx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr", K(ret), KP(task_ctx));
        } else {
          ObVecIndexAsyncTaskKey key(task_ctx->task_status_.tablet_id_, task_ctx->task_status_.task_type_);
          if (OB_HASH_EXIST == duplicate_tablet_task.exist_refactored(key)
              && OB_FAIL(skipped_task_ctx_array.push_back(task_ctx))) {
            LOG_WARN("fail to push back skipped task ctx", K(ret), K(i), K(key), KP(task_ctx));
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(ObVecIndexAsyncTaskUtil::batch_insert_vec_task(
        tenant_id_, OB_ALL_VECTOR_INDEX_TASK_TNAME, trans, new_task_ctx_array))) {
      LOG_WARN("fail to insert vec tasks", K(ret), K(tenant_id_), K(ls_handle_));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("fail to commit trans", KR(ret), K(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
    if (OB_SUCC(ret) && skipped_task_ctx_array.count() > 0) {
      int tmp_ret = OB_SUCCESS;
      ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
      if (OB_TMP_FAIL(get_index_ls_mgr(index_ls_mgr))) {
        LOG_WARN("fail to get index ls mgr when clear skipped task ctx",
                 K(tmp_ret), K_(tenant_id), K_(ls_handle));
      } else if (OB_ISNULL(index_ls_mgr)) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null index ls mgr", K(tmp_ret), K_(tenant_id), K_(ls_handle));
      } else if (OB_TMP_FAIL(clear_task_ctxs(index_ls_mgr->get_async_task_opt(), skipped_task_ctx_array))) {
        LOG_WARN("fail to clear skipped duplicate task ctx", K(tmp_ret), K(skipped_task_ctx_array));
      } else {
        LOG_INFO("clear skipped duplicate task ctx after insert new task",
                 K_(tenant_id), K_(ls_handle), K(skipped_task_ctx_array.count()));
      }
    }
  }
  // ensure destroy is called in all paths to avoid memory leak
  if (duplicate_tablet_task.created()) {
    duplicate_tablet_task.destroy();
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
    LOG_WARN("fail to get index ls mgr", K(ret), K(tenant_id_), K(ls_handle_));
  } else {
    ObVecIndexAsyncTaskOption &task_opt = index_ls_mgr->get_async_task_opt();
    FOREACH_X(iter, task_opt.get_async_task_map(), OB_SUCC(ret) && all_task_is_finish) {
      ObTabletID tablet_id = iter->first.tablet_id_;
      ObVecIndexAsyncTaskCtx *task_ctx = iter->second;
      if (OB_ISNULL(task_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      } else if (!task_ctx->in_thread_pool_ &&
          task_ctx->task_status_.status_ == ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH) {
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
        LOG_DEBUG("all vector async tasks cleared, memory freed per-object", K(ret),
          K(ls_handle_), K(all_task_is_finish));
      }
    } else {
      LOG_INFO("not reset vector async task ctx memory",
        K(ret), K(all_task_is_finish), K(task_opt.get_ls_processing_task_cnt()));
    }
  }
  return OB_SUCCESS;
}

}
}
