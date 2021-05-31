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

#define USING_LOG_PREFIX SQL
#include "sql/ob_query_exec_ctx_mgr.h"
#include "share/ob_server_blacklist.h"
#include "observer/ob_server.h"
namespace oceanbase {
using namespace lib;
using namespace common;
using namespace observer;
namespace sql {
ObQueryExecCtx* ObQueryExecCtx::alloc(ObSQLSessionInfo& session)
{
  int ret = OB_SUCCESS;
  ObQueryExecCtx* query_ctx = nullptr;
  MemoryContext* entity = nullptr;
  if (OB_ISNULL(entity = session.get_ctx_mem_context())) {
    ObMemAttr mem_attr;
    mem_attr.tenant_id_ = session.get_effective_tenant_id();
    mem_attr.label_ = "QueryExecCtx";
    mem_attr.ctx_id_ = ObCtxIds::QUERY_EXEC_CTX_ID;

    lib::ContextParam param;
    param.set_mem_attr(mem_attr)
        .set_page_size(OB_MALLOC_NORMAL_BLOCK_SIZE)
        .set_ablock_size(lib::INTACT_MIDDLE_AOBJECT_SIZE);
    // Memory entity used by result set may be accessed across threads during
    // asynchronous execution, so we must use ROOT_CONTEXT
    if (OB_FAIL(ROOT_CONTEXT.CREATE_CONTEXT(entity, param))) {
      LOG_WARN("create entity failed", K(ret), K(mem_attr));
    } else if (NULL == entity) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL memory entity", K(ret));
    }
  } else {
    // memory entity can not be used concurrently.
    session.set_ctx_mem_context(nullptr);
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(query_ctx = op_reclaim_alloc_args(ObQueryExecCtx, session, *entity))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(sizeof(ObQueryExecCtx)));
    } else {
      query_ctx->inc_ref_count();
      query_ctx->get_result_set().get_exec_context().set_query_exec_ctx(query_ctx);
      query_ctx->set_cached_schema_guard_info(session.get_cached_schema_guard_info());
    }
    if (OB_FAIL(ret) && query_ctx != nullptr) {
      ObQueryExecCtx::free(query_ctx);
      query_ctx = nullptr;
    }
  }
  return query_ctx;
}

void ObQueryExecCtx::free(ObQueryExecCtx* query_ctx)
{
  if (query_ctx != nullptr) {
    int64_t ref_cnt = query_ctx->dec_ref_count();
    if (0 == ref_cnt) {
      ObSQLSessionInfo& session = query_ctx->get_result_set().get_session();
      MemoryContext& mem_context = query_ctx->mem_context_;
      query_ctx->cache_schema_info_->try_revert_schema_guard();
      query_ctx->cache_schema_info_ = NULL;
      op_reclaim_free(query_ctx);
      query_ctx = nullptr;
      {
        ObSQLSessionInfo::LockGuard lock_guard(session.get_query_lock());
        // save memory entity to session if none memory entity in session, otherwise
        // destroy it.
        if (nullptr == session.get_ctx_mem_context()) {
          mem_context.reset_remain_one_page();
          session.set_ctx_mem_context(&mem_context);
        } else {
          DESTROY_CONTEXT(&mem_context);
        }
      }
    } else if (ref_cnt < 0) {
      LOG_ERROR("invalid ref cnt, maybe double free", K(ref_cnt));
    }
  }
}

int ObRemoteTaskCtx::init(ObQueryExecCtx& query_ctx)
{
  int ret = OB_SUCCESS;
  query_exec_ctx_ = &query_ctx;
  query_exec_ctx_->inc_ref_count();
  new (&remote_plan_driver_) observer::ObRemotePlanDriver(
      GCTX, sql_ctx_, query_ctx.get_result_set().get_session(), retry_ctrl_, mppacket_sender_);
  if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("session mgr is null", K(ret));
  } else if (OB_FAIL(GCTX.session_mgr_->inc_session_ref(&query_ctx.get_result_set().get_session()))) {
    LOG_WARN("inc session ref failed", K(ret));
  }
  return ret;
}

void ObTaskExecCtxAlloc::free_value(ObRemoteTaskCtx* p)
{
  if (p != nullptr) {
    op_reclaim_free(p);
    p = nullptr;
  }
}

int ObQueryExecCtxMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_exec_ctx_map_.init("QueryExecCtxMap"))) {
    LOG_WARN("init task exec ctx map failed", K(ret));
  } else if (OB_FAIL(TG_CREATE(TGDefIDs::QueryExecCtxGC, tg_id_))) {
    LOG_ERROR("create tg failed", K(ret));
  } else if (OB_FAIL(TG_SET_RUNNABLE_AND_START(tg_id_, *this))) {
    LOG_ERROR("start query exec ctx gc thread failed", K(ret));
  }
  return ret;
}

void ObQueryExecCtxMgr::destroy()
{
  TG_STOP(tg_id_);
  TG_WAIT(tg_id_);
  task_exec_ctx_map_.destroy();
}

int ObQueryExecCtxMgr::create_task_exec_ctx(
    const ObExecutionID& execution_id, ObQueryExecCtx& query_ctx, ObRemoteTaskCtx*& task_ctx)
{
  int ret = OB_SUCCESS;
  task_ctx = nullptr;
  if (OB_ISNULL(task_ctx = op_reclaim_alloc_args(ObRemoteTaskCtx))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(sizeof(ObQueryExecCtx)), K(execution_id));
  } else if (OB_FAIL(task_ctx->init(query_ctx))) {
    LOG_WARN("init task ctx failed", K(ret));
  } else if (OB_FAIL(task_exec_ctx_map_.insert_and_get(execution_id, task_ctx))) {
    LOG_WARN("insert and get query exec ctx failed", K(ret), K(execution_id));
  } else {
    task_ctx->set_execution_id(execution_id);
  }
  if (OB_FAIL(ret) && task_ctx != nullptr) {
    op_reclaim_free(task_ctx);
    task_ctx = nullptr;
  }
  return ret;
}

int ObQueryExecCtxMgr::get_task_exec_ctx(const ObExecutionID& execution_id, ObRemoteTaskCtx*& task_ctx)
{
  int ret = OB_SUCCESS;
  task_ctx = nullptr;
  if (OB_FAIL(task_exec_ctx_map_.get(execution_id, task_ctx))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get query exec ctx map failed", K(ret), K(execution_id));
    } else {
      LOG_INFO("query exec ctx not exists", K(execution_id));
    }
  }
  return ret;
}

void ObQueryExecCtxMgr::revert_task_exec_ctx(ObRemoteTaskCtx* task_ctx)
{
  task_exec_ctx_map_.revert(task_ctx);
  task_ctx = nullptr;
}

int ObQueryExecCtxMgr::drop_task_exec_ctx(const ObExecutionID& execution_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_exec_ctx_map_.del(execution_id))) {
    LOG_WARN("delete query exec ctx map node failed", K(ret), K(execution_id));
  }
  return ret;
}

void ObQueryExecCtxMgr::run1()
{
  int ret = OB_SUCCESS;
  (void)prctl(PR_SET_NAME, "QueryExecCtxGC", 0, 0, 0);
  LOG_INFO("QueryExecCtx GC Thread start");
  while (!has_set_stop()) {
    // GC per 100ms.
    USLEEP(GC_QUERY_CTX_INTERVAL_US);
    GCQueryExecCtx functor;
    if (OB_FAIL(task_exec_ctx_map_.remove_if(functor))) {
      // continue to next.
      LOG_WARN("remove query exec context failed", K(ret));
    }
  }
  LOG_INFO("QueryExecCtx GC Thread end");
}

bool GCQueryExecCtx::operator()(const ObExecutionID& execution_id, ObRemoteTaskCtx* task_ctx)
{
  bool bret = false;
  int ret = OB_SUCCESS;
  if (task_ctx != nullptr && task_ctx->get_query_exec_ctx() != nullptr && task_ctx->is_running()) {
    ObQueryExecCtx* query_ctx = task_ctx->get_query_exec_ctx();
    ObMySQLResultSet& result = query_ctx->get_result_set();
    ObSQLSessionInfo& session = result.get_session();
    ObSQLSessionInfo::LockGuard lock_guard(session.get_query_lock());
    if (!task_ctx->is_exiting()) {
      ObExecContext& exec_ctx = result.get_exec_context();
      if (OB_FAIL(exec_ctx.check_status())) {
        LOG_WARN("check query status failed", K(ret), K(execution_id), K(task_ctx->get_runner_svr()));
      } else if (SVR_BLACK_LIST.is_in_blacklist(
                     share::ObCascadMember(task_ctx->get_runner_svr(), session.get_local_ob_org_cluster_id()))) {
        ret = OB_RPC_POST_ERROR;
        LOG_WARN("runner server in black list, post rpc error", K(ret), K(task_ctx->get_runner_svr()));
      }
      if (OB_SUCCESS != ret) {
        bret = true;
        // set exiting to true to avoid access from cb thread.
        task_ctx->set_is_exiting(true);
        result.set_errcode(ret);
        THIS_WORKER.disable_retry();
        if (OB_FAIL(task_ctx->get_remote_plan_driver().response_result(result))) {
          LOG_WARN("response result failed", K(ret));
        }
        bool need_retry = (RETRY_TYPE_NONE != task_ctx->get_retry_ctrl().get_retry_type());
        if (!need_retry) {
          int fret = OB_SUCCESS;
          if (OB_SUCCESS != (fret = task_ctx->get_mppacket_sender().flush_buffer(true))) {
            LOG_WARN("flush the last buffer to client failed", K(ret), K(fret));
          }
        } else {
          rpc::ObRequest* mysql_request = task_ctx->get_mysql_request();
          if (OB_ISNULL(mysql_request)) {
            LOG_WARN("mysql request is null", K(ret), K(need_retry));
          } else {
            OBSERVER.get_net_frame().get_deliver().deliver(*mysql_request);
          }
        }
      }
    }
  }
  return bret;
}
}  // namespace sql
}  // namespace oceanbase
